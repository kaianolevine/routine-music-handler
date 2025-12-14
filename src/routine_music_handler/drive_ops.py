from __future__ import annotations

import io
import re
from dataclasses import dataclass
from typing import Optional

from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from kaiano_common_utils import logger as log

_DRIVE_ID_RE = re.compile(r"[-\w]{25,}")
_VERSION_RE = re.compile(r"_v(\d+)$")
_PROCESSED_ORIGINALS_FOLDER_NAME = "RoutineMusicHandler_ProcessedOriginals"


@dataclass(frozen=True)
class DownloadedFile:
    file_id: str
    name: str
    mime_type: str
    data: bytes


def extract_drive_file_id(url_or_id: str) -> Optional[str]:
    """Extract a Drive file id from a URL or return the id if already provided."""
    if not url_or_id:
        return None
    m = _DRIVE_ID_RE.search(url_or_id)
    return m.group(0) if m else None


def ensure_subfolder(drive, parent_folder_id: str, folder_name: str) -> str:
    """Return folder id for (parent/folder_name). Create if missing."""
    safe_name = folder_name.replace("'", "\\'")
    q = (
        "mimeType='application/vnd.google-apps.folder' "
        f"and name='{safe_name}' "
        f"and '{parent_folder_id}' in parents "
        "and trashed=false"
    )
    resp = (
        drive.files()
        .list(
            q=q,
            spaces="drive",
            fields="files(id,name)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = resp.get("files", [])
    if files:
        return files[0]["id"]

    created = (
        drive.files()
        .create(
            body={
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_folder_id],
            },
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return created["id"]


def ensure_my_drive_folder(drive, folder_name: str) -> str:
    """Ensure a folder exists in My Drive root and return its id."""
    return ensure_subfolder(drive, parent_folder_id="root", folder_name=folder_name)


def resolve_versioned_filename(
    drive, *, parent_folder_id: str, desired_filename: str
) -> tuple[str, int]:
    """Return (available_filename, version). Requires desired filename end with _v1.ext."""
    if "." in desired_filename:
        base, ext = desired_filename.rsplit(".", 1)
        ext = "." + ext
    else:
        base, ext = desired_filename, ""

    m = _VERSION_RE.search(base)
    if not m:
        raise ValueError(
            "desired_filename must include a _vN suffix before extension (e.g. _v1)"
        )
    base_root = base[: m.start()]
    start_version = int(m.group(1))
    base_root_lc = base_root.lower()
    ext_lc = ext.lower()

    # Fetch existing files with same prefix in the destination folder
    safe_root = (base_root + "_v").replace("'", "\\'")
    q = (
        f"'{parent_folder_id}' in parents and trashed=false "
        f"and name contains '{safe_root}'"
    )

    resp = (
        drive.files()
        .list(
            q=q,
            spaces="drive",
            fields="files(name)",
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )

    used_versions: set[int] = set()
    for f in resp.get("files", []):
        name = f.get("name", "")
        name_lc = name.lower()

        if ext_lc and not name_lc.endswith(ext_lc):
            continue

        stem = name_lc[: -len(ext_lc)] if ext_lc else name_lc
        if not stem.startswith(base_root_lc):
            continue

        m2 = _VERSION_RE.search(stem)
        if m2:
            used_versions.add(int(m2.group(1)))

    v = start_version
    while v in used_versions:
        v += 1

    return f"{base_root}_v{v}{ext}", v


def download_drive_file(drive, file_id: str) -> DownloadedFile:
    meta = (
        drive.files()
        .get(
            fileId=file_id,
            fields="id,name,mimeType",
            supportsAllDrives=True,
        )
        .execute()
    )

    req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    dl = MediaIoBaseDownload(fh, req)

    done = False
    while not done:
        _, done = dl.next_chunk()

    return DownloadedFile(
        file_id=file_id,
        name=meta["name"],
        mime_type=meta.get("mimeType", "application/octet-stream"),
        data=fh.getvalue(),
    )


def upload_new_file(
    drive, *, parent_folder_id: str, filename: str, content: bytes, mime_type: str
) -> str:
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime_type, resumable=False)
    created = (
        drive.files()
        .create(
            body={"name": filename, "parents": [parent_folder_id]},
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return created["id"]


def describe_drive_file_permissions(drive, file_id: str) -> str:
    """Return a compact string describing ownership/permissions/capabilities for debugging 403s."""
    try:
        meta = (
            drive.files()
            .get(
                fileId=file_id,
                fields=(
                    "id,name,mimeType,parents,driveId,"
                    "owners(displayName,emailAddress),"
                    "permissions(type,role,emailAddress,domain),"
                    "capabilities(canDelete,canTrash,canRemoveChildren,canMoveItemWithinDrive)"
                ),
                supportsAllDrives=True,
            )
            .execute()
        )

        owners = meta.get("owners") or []
        owners_str = ",".join(
            [(o.get("emailAddress") or o.get("displayName") or "?") for o in owners]
        )

        perms = meta.get("permissions") or []
        perms_str = ",".join(
            [
                f"{p.get('type', '?')}:{p.get('emailAddress') or p.get('domain') or '?'}={p.get('role', '?')}"
                for p in perms
            ]
        )

        caps = meta.get("capabilities") or {}
        caps_str = ",".join(
            [
                f"canDelete={caps.get('canDelete')}",
                f"canTrash={caps.get('canTrash')}",
                f"canRemoveChildren={caps.get('canRemoveChildren')}",
                f"canMoveItemWithinDrive={caps.get('canMoveItemWithinDrive')}",
            ]
        )

        return (
            f"name={meta.get('name')} mimeType={meta.get('mimeType')} "
            f"driveId={meta.get('driveId')} parents={meta.get('parents')} "
            f"owners=[{owners_str}] perms=[{perms_str}] caps=[{caps_str}]"
        )
    except Exception as e:
        return f"<failed to fetch metadata: {type(e).__name__}: {e}>"


def delete_drive_file(
    drive, file_id: str, *, fallback_remove_parent_id: str | None = None
) -> None:
    """Delete a Drive file (with fallbacks for common permission constraints)."""
    desc = describe_drive_file_permissions(drive, file_id)
    log.info("Delete attempt: file_id=%s %s", file_id, desc)

    # In our typical setup (service account is writer but not owner), delete/trash will always 403.
    # We can detect that up front via capabilities and jump directly to the fallback.
    try:
        caps_meta = (
            drive.files()
            .get(
                fileId=file_id,
                fields="capabilities(canDelete,canTrash)",
                supportsAllDrives=True,
            )
            .execute()
        )
        caps = caps_meta.get("capabilities") or {}
        can_delete = bool(caps.get("canDelete"))
        can_trash = bool(caps.get("canTrash"))
    except Exception as e:
        # If we can't read capabilities for some reason, fall back to the old behavior.
        log.debug(
            "Failed to read capabilities; will attempt delete/trash anyway: file_id=%s err=%s",
            file_id,
            e,
        )
        can_delete = True
        can_trash = True

    skip_delete_trash = (not can_delete) and (not can_trash)
    if skip_delete_trash:
        log.info(
            "Skipping delete/trash due to capabilities: file_id=%s canDelete=%s canTrash=%s",
            file_id,
            can_delete,
            can_trash,
        )

    # 1) Try hard delete
    if not skip_delete_trash:
        try:
            drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()
            return
        except Exception as e:
            log.warning("Hard delete failed: file_id=%s err=%s", file_id, e)

    # 2) Try move to trash
    if not skip_delete_trash:
        try:
            drive.files().update(
                fileId=file_id,
                body={"trashed": True},
                supportsAllDrives=True,
            ).execute()
            log.info("Trashed file: file_id=%s", file_id)
            return
        except Exception as e:
            log.warning("Trash failed: file_id=%s err=%s", file_id, e)

    # 3) We can't delete/trash. Move the original out of the intake folder so it doesn't keep reappearing.
    # If we *remove* the only parent, Drive will show the file in My Drive root ("orphaned").
    # Instead, we MOVE it to a dedicated folder in My Drive.
    if fallback_remove_parent_id:
        # Create/resolve a stable destination in My Drive.
        processed_folder_id = ensure_my_drive_folder(
            drive, _PROCESSED_ORIGINALS_FOLDER_NAME
        )

        # Fetch current parents so we only remove real parents.
        try:
            meta = (
                drive.files()
                .get(fileId=file_id, fields="id,parents", supportsAllDrives=True)
                .execute()
            )
            current_parents = meta.get("parents") or []
        except Exception as e:
            log.warning(
                "Failed to fetch parents before move fallback: file_id=%s err=%s",
                file_id,
                e,
            )
            current_parents = []

        # Prefer removing the configured intake folder if it is a current parent; otherwise remove all current parents.
        remove_parents: list[str] = []
        if current_parents and fallback_remove_parent_id in current_parents:
            remove_parents = [fallback_remove_parent_id]
        elif current_parents:
            remove_parents = list(current_parents)

        remove_str = ",".join(remove_parents) if remove_parents else ""

        try:
            drive.files().update(
                fileId=file_id,
                addParents=processed_folder_id,
                removeParents=remove_str,
                fields="id,parents",
                supportsAllDrives=True,
            ).execute()
            log.info(
                "Moved original to processed folder: file_id=%s processed_folder_id=%s removed_parents=%s",
                file_id,
                processed_folder_id,
                remove_str or "<none>",
            )
            return
        except Exception as e:
            log.warning(
                "Move-to-processed-folder fallback failed: file_id=%s processed_folder_id=%s removed_parents=%s err=%s",
                file_id,
                processed_folder_id,
                remove_str or "<none>",
                e,
            )

    raise PermissionError(
        f"Unable to delete or trash Drive file {file_id}. See logs for permissions/capabilities snapshot."
    )

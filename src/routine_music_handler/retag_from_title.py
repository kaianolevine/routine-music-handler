from __future__ import annotations

import io
import re
import tempfile
from pathlib import Path
from typing import Optional, Tuple

import kaiano.google_drive as google_drive
import kaiano.logger as log
import music_tag
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

AUDIO_EXTS = {".mp3", ".flac", ".m4a", ".mp4", ".ogg", ".opus", ".wav", ".aiff", ".aif"}

INPUT_DRIVE_FOLDER_ID = "1hDFTDOavXDtJN-MR-ruqqapMaXGp4mB6"
OUTPUT_DRIVE_FOLDER_ID = "17LjjgX4bFwxR4NOnnT38Aflp8DSPpjOu"


def split_camel(s: str) -> str:
    """Turn CasesLikeThis / SamCooke / 2AM into spaced words."""
    s = (s or "").strip()
    if not s:
        return ""

    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", s)
    s = re.sub(r"([A-Za-z])(\d)", r"\1 \2", s)
    s = re.sub(r"(\d)([A-Za-z])", r"\1 \2", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_compact_string(value: str) -> Optional[Tuple[str, str]]:
    """
    Parse your custom structure from a single string:
      <baseTitle> <extra...> <artist> <bpm>
    Rules:
      - last part is BPM (ignored)
      - part before BPM is Artist
      - first part is the base Title
      - all middle parts become (Extra) segments on the Title
    Requires at least 3 parts.
    """
    # Normalize common separators so splitting works reliably
    value = (value or "").replace("_", " ").strip()

    parts = [p for p in (value or "").split() if p.strip()]
    if len(parts) < 3:
        return None

    base_title = parts[0]
    artist = parts[-2]
    extras = parts[1:-2]

    bpm = parts[-1]

    # If the BPM digits were appended to the base title token, strip them.
    # Example: base_title="BurnWithMeWhilkMisky101" and bpm="101" -> base_title="BurnWithMeWhilkMisky"
    if bpm.isdigit() and base_title.endswith(bpm) and len(base_title) > len(bpm):
        base_title = base_title[: -len(bpm)]

    new_title = split_camel(base_title)
    for x in extras:
        new_title += f" ({split_camel(x)})"

    new_artist = split_camel(artist)
    return new_title.strip(), new_artist.strip()


def read_source_string(path: Path) -> str:
    """
    source:
      - title: read from Title tag
      - filename: read from filename stem
      - auto: Title if present else filename
    """

    title = ""
    try:
        f = music_tag.load_file(str(path))
        # music_tag returns an object; str() is a safe way to get a human-readable value.
        title = str(f["title"]) if f and ("title" in f) else ""
    except Exception:
        title = ""

    title = (title or "").strip()
    return title


def write_tags(path: Path, new_title: str, new_artist: str) -> None:
    try:
        f = music_tag.load_file(str(path))
    except Exception as e:
        raise ValueError(f"Unable to load audio file for tagging: {e}")

    # Set tags
    f["title"] = new_title
    f["artist"] = new_artist

    # Persist
    f.save()


def _verify_tags_after_write(path: Path) -> tuple[str, str]:
    """Best-effort re-read of Title/Artist after writing, for verification logs."""
    title = ""
    artist = ""

    try:
        f = music_tag.load_file(str(path))
        title = str(f["title"]) if f and ("title" in f) else ""
        artist = str(f["artist"]) if f and ("artist" in f) else ""
    except Exception:
        return "", ""

    return (title or "").strip(), (artist or "").strip()


def _download_drive_file_to_path(drive_service, file_id: str, dest_path: Path) -> None:
    request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(str(dest_path), mode="wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()


def _upload_path_to_output_folder(
    drive_service,
    output_folder_id: str,
    src_path: Path,
    dest_name: str,
) -> str:
    """Upload the modified file as a NEW Drive file into the output folder.

    Returns the new Drive file id.
    """
    media = MediaFileUpload(str(src_path), resumable=True)

    file_metadata = {
        "name": dest_name,
        "parents": [output_folder_id],
    }

    created = (
        drive_service.files()
        .create(
            body=file_metadata,
            media_body=media,
            supportsAllDrives=True,
            fields="id",
        )
        .execute()
    )

    return (created or {}).get("id") or ""


def main() -> int:
    folder_id = (INPUT_DRIVE_FOLDER_ID or "").strip()
    output_folder_id = (OUTPUT_DRIVE_FOLDER_ID or "").strip()

    log.info(
        "🎵 Retag start: "
        f"input_folder_id={folder_id} | output_folder_id={output_folder_id}"
    )

    if not folder_id:
        log.error("❌ INPUT_DRIVE_FOLDER_ID is empty; cannot continue.")
        return 1

    if not output_folder_id:
        log.error("❌ OUTPUT_DRIVE_FOLDER_ID is empty; cannot continue.")
        return 1

    drive_service = google_drive.get_drive_service()
    files = google_drive.list_files_in_folder(drive_service, folder_id)
    log.info(f"Found {len(files)} total file(s) in folder.")

    # Filter to likely-audio files by extension (Drive mime types vary)
    audio_files = []
    for f in files:
        name = (f.get("name") or "").strip()
        file_id = f.get("id")
        if not name or not file_id:
            continue
        if Path(name).suffix.lower() in AUDIO_EXTS:
            audio_files.append(f)
    log.info(f"Found {len(audio_files)} audio candidate file(s) (by extension).")

    rows: list[list[str]] = []
    changed = 0
    skipped = 0

    for f in sorted(audio_files, key=lambda x: (x.get("name") or "").lower()):
        file_id = f.get("id")
        name = f.get("name") or "(unknown)"

        log.info(
            f"➡️ Processing: {name} ({file_id}) → will upload modified copy to output folder"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / name

            try:
                log.debug(f"⬇️ Downloading Drive file to temp path: {local_path}")
                _download_drive_file_to_path(drive_service, file_id, local_path)
                log.debug("✅ Download complete")

                source_str = read_source_string(local_path)
                parsed = parse_compact_string(source_str)

                if not parsed:
                    skipped += 1
                    log.info(
                        "⏭️ Skip: string did not match expected pattern: <title> <extra...> <artist> <bpm>"
                    )
                    rows.append(
                        [
                            name,
                            file_id,
                            source_str,
                            "",
                            "",
                            "SKIP (doesn't match pattern)",
                        ]
                    )
                    continue

                new_title, new_artist = parsed
                log.info(f"✍️ Retag: Title='{new_title}' | Artist='{new_artist}'")

                # Always apply: write tags and upload back to Drive
                write_tags(local_path, new_title, new_artist)
                written_title, written_artist = _verify_tags_after_write(local_path)
                log.info(
                    f"🔁 Verified on-disk tags: Title='{written_title}' | Artist='{written_artist}'"
                )
                log.debug(
                    "⬆️ Uploading modified file to OUTPUT folder as a new Drive file"
                )
                log.debug("(supportsAllDrives=True)")
                new_file_id = _upload_path_to_output_folder(
                    drive_service,
                    output_folder_id=output_folder_id,
                    src_path=local_path,
                    dest_name=name,
                )
                log.debug(f"✅ Upload complete (new_file_id={new_file_id})")

                rows.append(
                    [
                        name,
                        file_id,
                        source_str,
                        new_title,
                        new_artist,
                        f"OK (uploaded copy: {new_file_id})",
                    ]
                )
                changed += 1

            except Exception as e:
                skipped += 1
                log.error(f"❌ ERROR processing {name} ({file_id}): {e}")
                rows.append([name, file_id, "", "", "", f"ERROR: {e}"])

    log.info(f"✅ Retag complete: updated {changed} file(s); skipped {skipped}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

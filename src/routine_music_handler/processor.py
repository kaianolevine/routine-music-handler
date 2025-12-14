from datetime import datetime
from typing import Any, Optional

from kaiano_common_utils import logger as log
from kaiano_common_utils.sheets_formatting import apply_sheet_formatting

from .drive_ops import (
    delete_drive_file,
    download_drive_file,
    ensure_subfolder,
    extract_drive_file_id,
    resolve_versioned_filename,
    upload_new_file,
)
from .file_translations import (
    build_base_filename,
    build_tag_artist,
    build_tag_title,
    sanitize_part,
    tag_audio_bytes_preserve_previous,
)
from .sheet_state import (
    INPUT_COL_COUNT,
    ensure_processed_col_is_last,
    iter_unprocessed_rows,
    mark_row_processed,
    parse_submission_row,
)


def process_submission_sheet(
    *,
    sheet,
    drive,
    gspreads_client,
    submissions_folder_id: str,
    dest_root_folder_id: Optional[str] = None,
) -> None:
    """Process all unprocessed rows in the submission sheet.

    Option A: processed flag is the last column. We mark 'X' only after:
      download -> tag -> upload -> delete original all succeed.
    """
    processed_col = ensure_processed_col_is_last(sheet)

    log.info(
        "Starting submission processing: processed_col=%s submissions_folder_id=%s dest_root_folder_id=%s",
        processed_col,
        submissions_folder_id,
        dest_root_folder_id,
    )

    for row_num, row in iter_unprocessed_rows(sheet, processed_col):
        log.info("Processing row %s", row_num)
        try:
            # Parse positional input
            sub = parse_submission_row(row[:INPUT_COL_COUNT])

            log.debug(
                "Row %s parsed: timestamp=%s division=%s routine=%s descriptor=%s",
                row_num,
                sub.timestamp,
                sub.division,
                sub.routine_name,
                sub.personal_descriptor,
            )

            if not sub.audio_url:
                log.info("Row %s: skipping (missing audio url)", row_num)
                continue

            file_id = extract_drive_file_id(sub.audio_url)
            if not file_id:
                log.warning("Row %s: skipping (could not extract file id)", row_num)
                continue

            base_no_ver_no_ext, season_year = build_base_filename(
                timestamp=sub.timestamp,
                leader=sanitize_part(sub.leader_first) + sanitize_part(sub.leader_last),
                follower=sanitize_part(sub.follower_first)
                + sanitize_part(sub.follower_last),
                division=sanitize_part(sub.division),
                routine=sanitize_part(sub.routine_name),
                descriptor=sanitize_part(sub.personal_descriptor),
            )

            log.info(
                "Row %s filename base: base=%s season_year=%s",
                row_num,
                base_no_ver_no_ext,
                season_year,
            )

            # Destination folder: root / DivisionSubfolder
            root = dest_root_folder_id or submissions_folder_id
            division_folder_name = sanitize_part(sub.division) or "UnknownDivision"
            dest_folder_id = ensure_subfolder(drive, root, division_folder_name)

            log.info(
                "Row %s destination: root=%s division_folder=%s dest_folder_id=%s",
                row_num,
                root,
                division_folder_name,
                dest_folder_id,
            )

            # Download original file
            original = download_drive_file(drive, file_id)
            ext = original.name.rsplit(".", 1)[1] if "." in original.name else ""

            log.info(
                "Row %s downloaded: source_file_id=%s original_name=%s mime_type=%s ext=%s bytes=%s",
                row_num,
                file_id,
                original.name,
                original.mime_type,
                ext,
                len(original.data),
            )

            desired = f"{base_no_ver_no_ext}_v1" + (f".{ext}" if ext else "")
            final_filename, version = resolve_versioned_filename(
                drive, parent_folder_id=dest_folder_id, desired_filename=desired
            )

            log.info(
                "Row %s final filename: desired=%s final=%s version=%s",
                row_num,
                desired,
                final_filename,
                version,
            )

            # Tag bytes (best-effort; returns original bytes on failure/unsupported)
            new_title = build_tag_title(
                leader_first=sanitize_part(sub.leader_first),
                leader_last=sanitize_part(sub.leader_last),
                follower_first=sanitize_part(sub.follower_first),
                follower_last=sanitize_part(sub.follower_last),
            )
            new_artist = build_tag_artist(
                version=version,
                division=sanitize_part(sub.division),
                season_year=season_year,
                routine_name=sanitize_part(sub.routine_name),
                personal_descriptor=sanitize_part(sub.personal_descriptor),
            )

            log.debug(
                "Row %s tags: title=%s artist=%s",
                row_num,
                new_title,
                new_artist,
            )

            tagged_bytes = tag_audio_bytes_preserve_previous(
                filename_for_type=final_filename,
                audio_bytes=original.data,
                new_title=new_title,
                new_artist=new_artist,
            )

            log.info(
                "Row %s tagged bytes: before=%s after=%s",
                row_num,
                len(original.data),
                len(tagged_bytes),
            )

            # Upload to destination
            new_file_id = upload_new_file(
                drive,
                parent_folder_id=dest_folder_id,
                filename=final_filename,
                content=tagged_bytes,
                mime_type=original.mime_type,
            )
            log.info(
                "Row %s uploaded: final_filename=%s new_file_id=%s dest_folder_id=%s",
                row_num,
                final_filename,
                new_file_id,
                dest_folder_id,
            )

            # Log submission to _Submitted_Music spreadsheet (in the destination root folder)
            try:
                log_root_folder_id = dest_root_folder_id or submissions_folder_id
                submitted_music_id = _find_or_create_submitted_music_spreadsheet_id(
                    drive, root_folder_id=log_root_folder_id
                )

                submitted_ss = gspreads_client.open_by_key(submitted_music_id)

                division_tab = (sub.division or "").strip() or "UnknownDivision"
                ws = _ensure_division_tab_and_headers(
                    submitted_ss, division=division_tab
                )

                partnership = _build_partnership_display(
                    sub.leader_first,
                    sub.leader_last,
                    sub.follower_first,
                    sub.follower_last,
                )

                _append_and_sort_submission_log_row(
                    ws=ws,
                    timestamp_value=sub.timestamp,
                    partnership=partnership,
                    division=division_tab,
                    routine_name=sub.routine_name,
                    descriptor=sub.personal_descriptor,
                    version=version,
                )

                log.info(
                    "Row %s logged submission: log_sheet_id=%s division_tab=%s partnership=%s",
                    row_num,
                    submitted_music_id,
                    division_tab,
                    partnership,
                )
            except Exception:
                # Logging to the spreadsheet should not block the main pipeline.
                log.exception(
                    "Row %s failed to log submission to _Submitted_Music", row_num
                )

            # Delete original only after successful upload
            delete_drive_file(
                drive, file_id, fallback_remove_parent_id=submissions_folder_id
            )
            log.info("Row %s deleted original: source_file_id=%s", row_num, file_id)

            # Mark processed in the sheet last
            mark_row_processed(sheet, row_num, processed_col)
            log.info("Row %s marked processed", row_num)

        except Exception:
            # Do not mark processed. This row will retry next run.
            log.exception("Row %s failed to process", row_num)
    log.info("Finished submission processing")


def _get_gspread_client_from_worksheet(sheet: Any) -> Any:
    """Best-effort extraction of a gspread Client from a gspread Worksheet.

    Different gspread versions expose either a Client or an HTTPClient in different places.
    We need the Client because `open_by_key` exists on Client, not HTTPClient.
    """

    # Common case: Worksheet.spreadsheet is a Spreadsheet.
    ss = getattr(sheet, "spreadsheet", None)
    if ss is not None:
        # Some versions expose Client here
        cand = getattr(ss, "client", None)
        if cand is not None and hasattr(cand, "open_by_key"):
            return cand

        # Some versions store Client as _client
        cand = getattr(ss, "_client", None)
        if cand is not None and hasattr(cand, "open_by_key"):
            return cand

        # Some versions expose the Client under ss.client.client (ss.client is HTTPClient)
        cand2 = getattr(cand, "client", None) if cand is not None else None
        if cand2 is not None and hasattr(cand2, "open_by_key"):
            return cand2

    # Alternate attribute name in some objects
    ss2 = getattr(sheet, "_spreadsheet", None)
    if ss2 is not None:
        cand = getattr(ss2, "client", None)
        if cand is not None and hasattr(cand, "open_by_key"):
            return cand
        cand = getattr(ss2, "_client", None)
        if cand is not None and hasattr(cand, "open_by_key"):
            return cand

    # As a last resort, the sheet itself might have client
    cand = getattr(sheet, "client", None)
    if cand is not None and hasattr(cand, "open_by_key"):
        return cand

    raise AttributeError(
        "Unable to locate a gspread Client with open_by_key(). "
        "Inspect worksheet.spreadsheet and worksheet._spreadsheet attributes."
    )


def _pretty_person_name(first: str, last: str) -> str:
    """Trim + Title Case words while keeping spaces and Unicode letters."""
    first = (first or "").strip()
    last = (last or "").strip()

    def _title_words(s: str) -> str:
        # Use split() to collapse whitespace, then title-case each token.
        # `.title()` is Unicode-aware; it will keep accents.
        return " ".join(tok.title() for tok in s.split())

    first_t = _title_words(first)
    last_t = _title_words(last)
    if first_t and last_t:
        return f"{first_t} {last_t}"
    return first_t or last_t


def _build_partnership_display(
    leader_first: str,
    leader_last: str,
    follower_first: str,
    follower_last: str,
) -> str:
    leader = _pretty_person_name(leader_first, leader_last)
    follower = _pretty_person_name(follower_first, follower_last)
    if leader and follower:
        return f"{leader} & {follower}"
    return leader or follower


def _find_or_create_submitted_music_spreadsheet_id(
    drive, *, root_folder_id: str
) -> str:
    """Find or create the `_Submitted_Music` Google Sheet in the given root folder."""
    name = "_Submitted_Music"
    mime = "application/vnd.google-apps.spreadsheet"

    q = (
        f"name = '{name}' and mimeType = '{mime}' "
        f"and '{root_folder_id}' in parents and trashed = false"
    )

    resp = (
        drive.files()
        .list(
            q=q,
            fields="files(id,name)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )

    files = resp.get("files") or []
    if files:
        return files[0]["id"]

    # Create it if it doesn't exist.
    created = (
        drive.files()
        .create(
            body={"name": name, "mimeType": mime, "parents": [root_folder_id]},
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return created["id"]


def _ensure_division_tab_and_headers(spreadsheet: Any, *, division: str) -> Any:
    """Ensure a worksheet exists for `division` and has the expected 5-column header row."""
    headers = [
        "Timestamp",
        "Partnership",
        "Division",
        "Routine Name",
        "Descriptor",
        "Version",
    ]

    # Find or create worksheet
    try:
        ws = spreadsheet.worksheet(division)
    except Exception:
        # gspread expects row/col counts for new sheets
        ws = spreadsheet.add_worksheet(title=division, rows=200, cols=len(headers))
        apply_sheet_formatting(ws)

    # Ensure headers in first row
    try:
        existing = ws.row_values(1)
    except Exception:
        existing = []

    if existing[: len(headers)] != headers:
        # Overwrite first row with headers (only first 6 columns)
        ws.update("A1:F1", [headers])

    return ws


def _append_and_sort_submission_log_row(
    *,
    ws: Any,
    timestamp_value: Any,
    partnership: str,
    division: str,
    routine_name: str,
    descriptor: str,
    version: int,
) -> None:
    """Append a row to the division worksheet and sort by Version desc, then Partnership asc."""
    # Write timestamp as ISO string for stable sorting/visibility.
    if isinstance(timestamp_value, datetime):
        ts = timestamp_value.isoformat(sep=" ", timespec="seconds")
    else:
        ts = str(timestamp_value)

    row = [
        ts,
        partnership,
        (division or "").strip(),
        (routine_name or "").strip(),
        (descriptor or "").strip(),
        int(version),
    ]

    ws.append_row(row, value_input_option="RAW")

    # Sort deterministically by pulling rows, sorting in Python, and writing back.
    # Keys: Partnership (col 2) asc (grouping), then Version (col 6) numeric desc.
    try:
        values = ws.get_all_values()
        if not values or len(values) <= 2:
            return

        data_rows = values[1:]

        # Drop fully-empty rows (defensive)
        data_rows = [r for r in data_rows if any((c or "").strip() for c in r)]
        if not data_rows:
            return

        def _version_num(r: list[str]) -> int:
            try:
                return int((r[5] or "").strip())  # col 6
            except Exception:
                return 0

        def _partnership_key(r: list[str]) -> str:
            return (r[1] or "").strip().casefold()  # col 2

        data_rows.sort(key=lambda r: (_partnership_key(r), -_version_num(r)))

        end_row = 1 + len(data_rows)
        ws.batch_clear([f"A2:F{end_row}"])
        ws.update(f"A2:F{end_row}", data_rows, value_input_option="RAW")

    except Exception:
        log.exception(
            "Failed to sort _Submitted_Music tab after append: title=%s",
            getattr(ws, "title", "<unknown>"),
        )

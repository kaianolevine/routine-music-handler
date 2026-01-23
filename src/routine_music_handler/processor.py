from __future__ import annotations

"""Routine music handler processor.

Intent (confirmed):
- Read Google Form submissions from a worksheet.
- For each unprocessed row:
    1) Extract Drive file id from the submitted URL
    2) Build a deterministic base filename + season/year
    3) Download the original audio bytes
    4) Tag the audio (best-effort; preserve prior tags in comment)
    5) Upload the renamed/tagged file to the destination folder
    6) Log the submission to a per-division tab in _Submitted_Music
    7) Delete/trash/move the original (permission-friendly fallback)
    8) Mark the row as processed ('X') ONLY after all steps succeed

Safety property:
- Rows are never marked processed if any step fails; they will retry on the next run.

Architecture:
- Uses kaiano-common-utils facades (GoogleAPI, DriveFacade, SheetsFacade + SheetsFormatter).
- Worksheet access uses gspread internally (callers pass spreadsheet id + worksheet name).
"""

import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Sequence

from kaiano import logger as logger_mod
from kaiano.google import GoogleAPI
from kaiano.json import create_collection_snapshot, write_json_snapshot
from kaiano.mp3.rename import Mp3Renamer
from kaiano.mp3.tag import Mp3Tagger

log = logger_mod.get_logger()


# -----------------------------------------------------------------------------
# Sheet row layout
# -----------------------------------------------------------------------------


class FormCols:
    TIMESTAMP = 0
    EMAIL = 1
    LEADER_FIRST = 2
    LEADER_LAST = 3
    FOLLOWER_FIRST = 4
    FOLLOWER_LAST = 5
    DIVISION = 6
    ROUTINE_NAME = 7
    PERSONAL_DESCRIPTOR = 8
    AUDIO_FILE_URL = 9
    ACKNOWLEDGE = 10


INPUT_FORM_COL_COUNT = 11
PROCESSED_INDEX = INPUT_FORM_COL_COUNT + 1


@dataclass(frozen=True)
class Submission:
    timestamp: str
    leader_first: str
    leader_last: str
    follower_first: str
    follower_last: str
    division: str
    routine_name: str
    personal_descriptor: str
    audio_url: str


def write_submitted_music_snapshot(
    *,
    g: GoogleAPI,
    submitted_music_id: str,
) -> None:
    """
    Write a static JSON snapshot of the per-division submission log spreadsheet (_Submitted_Music),
    reading ALL worksheets/tabs in the spreadsheet.

    Output path can be configured via ROUTINE_MUSIC_JSON_OUTPUT_PATH; defaults to
    v1/routine-music/submitted_music.json (repo-relative).
    """
    json_output_path = (
        os.getenv("ROUTINE_MUSIC_JSON_OUTPUT_PATH")
        or "v1/routine-music/submitted_music.json"
    )

    ss = g.gspread.open_by_key(submitted_music_id)

    snapshot = create_collection_snapshot("divisions")
    divisions: list[dict[str, Any]] = []

    for ws in ss.worksheets():
        division_name = (getattr(ws, "title", "") or "").strip() or "UnknownDivision"
        try:
            values = ws.get_all_values() or []
        except Exception:
            log.exception("Failed to read worksheet for snapshot: %s", division_name)
            continue

        if not values or len(values) < 2:
            # header-only or empty
            continue

        headers = values[0]
        rows = values[1:]
        rows = [r for r in rows if any((c or "").strip() for c in r)]
        if not rows:
            continue

        # Normalize row widths to header width for consistent output.
        width = len(headers)
        norm_rows: list[list[str]] = []
        for r in rows:
            rr = list(r)
            if len(rr) < width:
                rr.extend([""] * (width - len(rr)))
            elif len(rr) > width:
                rr = rr[:width]
            norm_rows.append(rr)

        divisions.append(
            {
                "division": division_name,
                "headers": headers,
                "rows": norm_rows,
            }
        )

    snapshot["divisions"] = divisions

    try:
        write_json_snapshot(snapshot, json_output_path)
        log.info("🧾 Wrote _Submitted_Music JSON snapshot to: %s", json_output_path)
    except Exception:
        log.exception(
            "Failed to write _Submitted_Music JSON snapshot to: %s", json_output_path
        )


# -----------------------------------------------------------------------------
# Public entrypoint
# -----------------------------------------------------------------------------


def process_submission_sheet(
    *,
    g: GoogleAPI,
    submission_sheet_id: str,
    worksheet_name: Optional[str],
    submissions_folder_id: str,
    dest_root_folder_id: Optional[str] = None,
) -> None:
    """Process all unprocessed rows in the submission sheet.

    We mark 'X' only after:
      download -> tag -> upload -> delete original all succeed.

    This function opens the gspread worksheet internally (callers pass IDs only).
    """

    # Open the submission worksheet internally (callers pass IDs only).
    ss = g.gspread.open_by_key(submission_sheet_id)
    sheet = ss.worksheet(worksheet_name) if worksheet_name else ss.sheet1

    # Processed column is a fixed positional column (schema-by-convention).
    processed_col = PROCESSED_INDEX
    drive = g.drive

    log.info(
        "Starting submission processing: processed_col=%s submissions_folder_id=%s dest_root_folder_id=%s",
        processed_col,
        submissions_folder_id,
        dest_root_folder_id,
    )

    # Iterate only rows whose processed flag is not 'X'.
    for row_num, row in _iter_unprocessed_rows(sheet, processed_col):
        log.info("Processing row %s", row_num)
        try:
            # Normalize + parse the fixed-position form row into a Submission record.
            sub = _parse_submission_row(
                g.sheets.normalize_row(row[:INPUT_FORM_COL_COUNT])
            )

            if not sub.audio_url:
                log.info("Row %s: skipping (missing audio url)", row_num)
                continue

            # Extract Drive file id from the submitted URL (skip row if missing/unparseable).
            file_id = drive.extract_drive_file_id(sub.audio_url)
            if not file_id:
                log.warning("Row %s: skipping (could not extract file id)", row_num)
                continue

            # Build deterministic base filename (no version/ext yet) and season-year from timestamp.
            season_year = _parse_routine_season_year(sub.timestamp)
            base_no_ver_no_ext = Mp3Renamer.build_routine_filename(
                leader=_sanitize_user_entered_data_from_form(sub.leader_first)
                + _sanitize_user_entered_data_from_form(
                    sub.leader_last
                ),  # FirstnameLastname
                follower=_sanitize_user_entered_data_from_form(sub.follower_first)
                + _sanitize_user_entered_data_from_form(
                    sub.follower_last
                ),  # FirstnameLastname
                division=_sanitize_user_entered_data_from_form(sub.division),
                routine=_sanitize_user_entered_data_from_form(sub.routine_name),
                descriptor=_sanitize_user_entered_data_from_form(
                    sub.personal_descriptor
                ),
                season_year=season_year,
            )

            # Destination folder is <root>/<Division> (created if missing).
            root = dest_root_folder_id or submissions_folder_id
            division_folder_name = (
                _sanitize_user_entered_data_from_form(sub.division) or "UnknownDivision"
            )
            dest_folder_id = drive.ensure_folder(root, division_folder_name)

            # Download original file into memory (metadata + bytes).
            original = drive.download_file_bytes(file_id)
            ext = original.name.rsplit(".", 1)[1] if "." in original.name else ""

            # Choose next available _vN filename in destination folder.
            desired = f"{base_no_ver_no_ext}_v1" + (f".{ext}" if ext else "")
            final_filename, version = drive.resolve_versioned_filename(
                parent_folder_id=dest_folder_id,
                desired_filename=desired,
            )

            # Build tags for VirtualDJ workflow (intent confirmed).
            new_title = Mp3Tagger.build_routine_tag_title(
                leader_first=_sanitize_user_entered_data_from_form(sub.leader_first),
                leader_last=_sanitize_user_entered_data_from_form(sub.leader_last),
                follower_first=_sanitize_user_entered_data_from_form(
                    sub.follower_first
                ),
                follower_last=_sanitize_user_entered_data_from_form(sub.follower_last),
            )
            new_artist = Mp3Tagger.build_routine_tag_artist(
                version=version,
                division=_sanitize_user_entered_data_from_form(sub.division),
                season_year=season_year,
                routine_name=_sanitize_user_entered_data_from_form(sub.routine_name),
                personal_descriptor=_sanitize_user_entered_data_from_form(
                    sub.personal_descriptor
                ),
            )

            # Tag bytes best-effort; on failure returns original bytes unchanged.
            tagged_bytes = tag_audio_bytes_preserve_previous(
                filename_for_type=final_filename,
                audio_bytes=original.data,
                new_title=new_title,
                new_artist=new_artist,
            )

            # Upload bytes to destination folder with the resolved filename.
            new_file_id = drive.upload_bytes(
                parent_id=dest_folder_id,
                filename=final_filename,
                content=tagged_bytes,
                mime_type=original.mime_type,
            )
            log.info("Row %s uploaded: new_file_id=%s", row_num, new_file_id)

            # Log to _Submitted_Music (best-effort; failures do not block main pipeline).
            try:
                log_root_folder_id = dest_root_folder_id or submissions_folder_id
                submitted_music_id = drive.find_or_create_spreadsheet(
                    parent_folder_id=log_root_folder_id,
                    name="_Submitted_Music",
                )

                division_tab = (sub.division or "").strip() or "UnknownDivision"
                ws = _ensure_division_tab_and_headers(
                    g=g,
                    submitted_music_id=submitted_music_id,
                    division=division_tab,
                )

                _append_and_sort_submission_log_row(
                    ws=ws,
                    timestamp_value=sub.timestamp,
                    partnership=new_title,  # keeping the sheet partnership the same as the title tag
                    division=division_tab,
                    routine_name=sub.routine_name,
                    descriptor=sub.personal_descriptor,
                    version=version,
                )
            except Exception:
                log.exception(
                    "Row %s failed to log submission to _Submitted_Music", row_num
                )

            # Delete/trash/move original only after successful upload.
            drive.delete_file_with_fallback(
                file_id,
                fallback_remove_parent_id=submissions_folder_id,
            )

            # Mark processed last so partial failures retry next run.
            sheet.update_cell(row_num, processed_col, "X")

        except Exception:
            log.exception("Row %s failed to process", row_num)

    # Best-effort: publish a JSON snapshot of the _Submitted_Music spreadsheet for website consumption.
    write_submitted_music_snapshot(g=g, submitted_music_id=submission_sheet_id)

    log.info("Finished submission processing")


# -----------------------------------------------------------------------------
# Sheet helpers (gspread adapter layer)
# -----------------------------------------------------------------------------


def _iter_unprocessed_rows(sheet: Any, processed_col: int):
    """Yield (row_num, row_values) for rows whose processed flag != 'X'.

    Note: this is a generator; it naturally yields nothing when the sheet has only a header.
    """
    values = sheet.get_all_values()

    # values[0] is the header row; values[1:] is data. If there are no data rows, the loop simply won't run.
    for idx, row in enumerate(values[1:], start=2):
        # Ensure row is long enough to read the processed column safely.
        while len(row) < processed_col:
            row.append("")

        if (row[processed_col - 1] or "").strip().upper() == "X":
            continue

        yield idx, row


# -----------------------------------------------------------------------------
# Parsing and normalization
# -----------------------------------------------------------------------------


def _parse_submission_row(r: Sequence[Any]) -> Submission:
    if len(r) < INPUT_FORM_COL_COUNT:
        raise ValueError(
            f"Row too short: expected >= {INPUT_FORM_COL_COUNT}, got {len(r)}"
        )

    return Submission(
        timestamp=r[FormCols.TIMESTAMP],
        leader_first=r[FormCols.LEADER_FIRST],
        leader_last=r[FormCols.LEADER_LAST],
        follower_first=r[FormCols.FOLLOWER_FIRST],
        follower_last=r[FormCols.FOLLOWER_LAST],
        division=r[FormCols.DIVISION],
        routine_name=r[FormCols.ROUTINE_NAME],
        personal_descriptor=r[FormCols.PERSONAL_DESCRIPTOR],
        audio_url=r[FormCols.AUDIO_FILE_URL],
    )


# -----------------------------------------------------------------------------
# _Submitted_Music & Tagging helpers
# -----------------------------------------------------------------------------


def _ensure_division_tab_and_headers(
    *,
    g: GoogleAPI,
    submitted_music_id: str,
    division: str,
) -> Any:
    headers = [
        "Timestamp",
        "Partnership",
        "Division",
        "Routine Name",
        "Descriptor",
        "Version",
    ]
    ss = g.gspread.open_by_key(submitted_music_id)

    try:
        ws = ss.worksheet(division)
    except Exception:
        ws = ss.add_worksheet(title=division, rows=200, cols=len(headers))
        g.sheets.formatter.apply_sheet_formatting(ws)

    try:
        existing = ws.row_values(1)
    except Exception:
        existing = []

    if existing[: len(headers)] != headers:
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
    if isinstance(timestamp_value, datetime):
        ts = timestamp_value.isoformat(sep=" ", timespec="seconds")
    else:
        ts = str(timestamp_value)

    row = [
        ts,
        partnership,
        division.strip(),
        routine_name.strip(),
        descriptor.strip(),
        int(version),
    ]
    ws.append_row(row, value_input_option="RAW")

    try:
        values = ws.get_all_values()
        if not values or len(values) <= 2:
            return

        data_rows = values[1:]
        data_rows = [r for r in data_rows if any((c or "").strip() for c in r)]
        if not data_rows:
            return

        def _version_num(r: list[str]) -> int:
            try:
                return int((r[5] or "").strip())
            except Exception:
                return 0

        def _partnership_key(r: list[str]) -> str:
            return (r[1] or "").strip().casefold()

        data_rows.sort(key=lambda r: (_partnership_key(r), -_version_num(r)))
        end_row = 1 + len(data_rows)
        ws.batch_clear([f"A2:F{end_row}"])
        ws.update(f"A2:F{end_row}", data_rows, value_input_option="RAW")

    except Exception:
        log.exception(
            "Failed to sort _Submitted_Music tab after append: title=%s",
            getattr(ws, "title", "<unknown>"),
        )


def tag_audio_bytes_preserve_previous(
    *,
    filename_for_type: str,
    audio_bytes: bytes,
    new_title: str,
    new_artist: str,
) -> bytes:
    ext = (
        filename_for_type.rsplit(".", 1)[-1].lower() if "." in filename_for_type else ""
    )
    suffix = f".{ext}" if ext else ""

    tagger = Mp3Tagger()

    try:
        with tempfile.TemporaryDirectory() as td:
            tmp_path = os.path.join(td, f"audio{suffix}")
            with open(tmp_path, "wb") as f:
                f.write(audio_bytes)

            # Read existing tags (best-effort)
            existing = tagger.read(tmp_path).tags
            existing_title = Mp3Tagger.sanitize_string(existing.get("tracktitle", ""))
            existing_artist = Mp3Tagger.sanitize_string(existing.get("artist", ""))
            existing_album = Mp3Tagger.sanitize_string(existing.get("album", ""))
            existing_comment = Mp3Tagger.sanitize_string(existing.get("comment", ""))

            # Preserve previous tags in a stable, parseable format.
            # Intent (confirmed): store as prev[title,artist,album,comment].
            if any([existing_title, existing_artist, existing_album, existing_comment]):
                prev_concat = f"prev[{existing_title},{existing_artist},{existing_album},{existing_comment}]"
            else:
                prev_concat = ""

            metadata: dict[str, Any] = {
                "title": new_title,
                "artist": new_artist,
            }
            if prev_concat:
                metadata["comment"] = prev_concat

            # Write updated tags back to the temp file
            tagger.write(tmp_path, metadata, ensure_virtualdj_compat=True)

            with open(tmp_path, "rb") as f:
                return f.read()

    except Exception as e:
        log.debug(
            "tag_audio_bytes_preserve_previous: tagging failed; returning original bytes: %s",
            e,
        )
        return audio_bytes


_NON_ALNUM = re.compile(r"[^A-Za-z0-9_]+")


def _sanitize_user_entered_data_from_form(value: str) -> str:
    """trim -> lower -> TitleCaseWords -> remove spaces -> sanitize -> collapse underscores"""
    if not value:
        return ""

    # Start from trimmed input.
    raw = value.strip()
    if not raw:
        return ""

    # Treat underscores like whitespace separators for casing purposes.
    raw = raw.replace("_", " ")

    # Split on whitespace first, then on non-alphanumeric separators.
    # We title-case each alphanumeric segment so characters immediately following
    # special characters (e.g., '/', '-') remain capitalized.
    words = raw.split()
    pieces: list[str] = []
    for w in words:
        # Split on any non-alphanumeric/underscore. Underscore is preserved.
        segs = _NON_ALNUM.split(w)
        for seg in segs:
            if not seg:
                continue
            # Title-case each segment (first char upper, rest lower).
            pieces.append(seg[:1].upper() + seg[1:].lower())

    v = "".join(pieces)

    # Normalize any user-supplied underscores.
    v = re.sub(r"_+", "_", v).strip("_")
    return v


def _parse_routine_season_year(timestamp: str) -> str:
    """Google Forms timestamp 'M/D/YYYY HH:MM:SS'. If month >= 11, season year is next year."""
    dt = datetime.strptime(timestamp.strip(), "%m/%d/%Y %H:%M:%S")
    year = dt.year + (1 if dt.month >= 11 else 0)
    return str(year)

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Sequence

from kaiano import logger as logger_mod
from kaiano.google import GoogleAPI
from kaiano.helpers import (
    as_str,
    build_base_filename,
    build_tag_artist,
    build_tag_title,
    sanitize_part,
)
from kaiano.mp3.tag import Mp3Tagger

log = logger_mod.get_logger()

# -----------------------------------------------------------------------------
# Sheet row layout
# -----------------------------------------------------------------------------

INPUT_COL_COUNT = 11  # fixed positional input columns


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

    ss = g.gspread.open_by_key(submission_sheet_id)
    sheet = ss.worksheet(worksheet_name) if worksheet_name else ss.sheet1

    processed_col = get_processed_col_index()
    drive = g.drive

    log.info(
        "Starting submission processing: processed_col=%s submissions_folder_id=%s dest_root_folder_id=%s",
        processed_col,
        submissions_folder_id,
        dest_root_folder_id,
    )

    for row_num, row in iter_unprocessed_rows(sheet, processed_col):
        log.info("Processing row %s", row_num)
        try:
            sub = parse_submission_row(row[:INPUT_COL_COUNT])

            if not sub.audio_url:
                log.info("Row %s: skipping (missing audio url)", row_num)
                continue

            file_id = drive.extract_drive_file_id(sub.audio_url)
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

            root = dest_root_folder_id or submissions_folder_id
            division_folder_name = sanitize_part(sub.division) or "UnknownDivision"
            dest_folder_id = drive.ensure_folder(root, division_folder_name)

            original = drive.download_file_bytes(file_id)
            ext = original.name.rsplit(".", 1)[1] if "." in original.name else ""

            desired = f"{base_no_ver_no_ext}_v1" + (f".{ext}" if ext else "")
            final_filename, version = drive.resolve_versioned_filename(
                parent_folder_id=dest_folder_id,
                desired_filename=desired,
            )

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

            tagged_bytes = tag_audio_bytes_preserve_previous(
                filename_for_type=final_filename,
                audio_bytes=original.data,
                new_title=new_title,
                new_artist=new_artist,
            )

            new_file_id = drive.upload_bytes(
                parent_id=dest_folder_id,
                filename=final_filename,
                content=tagged_bytes,
                mime_type=original.mime_type,
            )
            log.info("Row %s uploaded: new_file_id=%s", row_num, new_file_id)

            # Log submission to _Submitted_Music
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
            except Exception:
                log.exception(
                    "Row %s failed to log submission to _Submitted_Music", row_num
                )

            # Delete original only after successful upload
            drive.delete_file_with_fallback(
                file_id,
                fallback_remove_parent_id=submissions_folder_id,
            )

            # Mark processed last
            mark_row_processed(sheet, row_num, processed_col)

        except Exception:
            log.exception("Row %s failed to process", row_num)

    log.info("Finished submission processing")


# -----------------------------------------------------------------------------
# Sheet helpers (gspread adapter layer)
# -----------------------------------------------------------------------------


def get_processed_col_index() -> int:
    return INPUT_COL_COUNT + 1


def iter_unprocessed_rows(sheet: Any, processed_col: int):
    values = sheet.get_all_values()
    if len(values) <= 1:
        return
    for idx, row in enumerate(values[1:], start=2):
        while len(row) < processed_col:
            row.append("")
        if (row[processed_col - 1] or "").strip().upper() == "X":
            continue
        yield idx, row


def mark_row_processed(sheet: Any, row_num: int, processed_col: int) -> None:
    sheet.update_cell(row_num, processed_col, "X")


# -----------------------------------------------------------------------------
# Parsing and normalization
# -----------------------------------------------------------------------------


def normalize_cell(v: Any) -> str:
    return "" if v is None else str(v).strip()


def normalize_row(row: Sequence[Any]) -> list[str]:
    return [normalize_cell(v) for v in row]


def parse_submission_row(row: Sequence[Any]) -> Submission:
    if len(row) < INPUT_COL_COUNT:
        raise ValueError(
            f"Row too short: expected >= {INPUT_COL_COUNT}, got {len(row)}"
        )

    r = normalize_row(row)

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
# _Submitted_Music helpers
# -----------------------------------------------------------------------------


def _pretty_person_name(first: str, last: str) -> str:
    first = (first or "").strip()
    last = (last or "").strip()
    return " ".join(tok.title() for tok in f"{first} {last}".split()).strip()


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


# -----------------------------------------------------------------------------
# Tagging helpers
# -----------------------------------------------------------------------------


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
            existing_title = as_str(existing.get("tracktitle", ""))
            existing_artist = as_str(existing.get("artist", ""))
            existing_album = as_str(existing.get("album", ""))
            existing_comment = as_str(existing.get("comment", ""))

            prev_concat = " | ".join(
                [
                    v
                    for v in [
                        existing_title,
                        existing_artist,
                        existing_album,
                        existing_comment,
                    ]
                    if v
                ]
            )

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

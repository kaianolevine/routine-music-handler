from __future__ import annotations

import logging
from typing import Optional

from .audio_tags import (
    build_tag_artist,
    build_tag_title,
    tag_audio_bytes_preserve_previous,
)
from .drive_ops import (
    delete_drive_file,
    download_drive_file,
    ensure_subfolder,
    extract_drive_file_id,
    resolve_versioned_filename,
    upload_new_file,
)
from .filenames import build_base_filename, sanitize_part
from .sheet_state import (
    ensure_processed_col_is_last,
    iter_unprocessed_rows,
    mark_row_processed,
)
from .submission_schema import INPUT_COL_COUNT, parse_submission_row

log = logging.getLogger(__name__)


def process_submission_sheet(
    *,
    sheet,
    drive,
    submissions_folder_id: str,
    dest_root_folder_id: Optional[str] = None,
) -> None:
    """Process all unprocessed rows in the submission sheet.

    Option A: processed flag is the last column. We mark 'X' only after:
      download -> tag -> upload -> delete original all succeed.
    """
    processed_col = ensure_processed_col_is_last(sheet)

    for row_num, row in iter_unprocessed_rows(sheet, processed_col):
        try:
            # Parse positional input
            sub = parse_submission_row(row[:INPUT_COL_COUNT])

            if not sub.audio_url:
                log.info("Row %s: missing audio url; skipping", row_num)
                continue

            file_id = extract_drive_file_id(sub.audio_url)
            if not file_id:
                log.warning("Row %s: could not extract file id; skipping", row_num)
                continue

            base_no_ver_no_ext, season_year = build_base_filename(sub)

            # Destination folder: root / DivisionSubfolder
            root = dest_root_folder_id or submissions_folder_id
            division_folder_name = sanitize_part(sub.division) or "UnknownDivision"
            dest_folder_id = ensure_subfolder(drive, root, division_folder_name)

            # Download original file
            original = download_drive_file(drive, file_id)
            ext = original.name.rsplit(".", 1)[1] if "." in original.name else ""

            desired = f"{base_no_ver_no_ext}_v1" + (f".{ext}" if ext else "")
            final_filename, version = resolve_versioned_filename(
                drive, parent_folder_id=dest_folder_id, desired_filename=desired
            )

            # Tag bytes (best-effort; returns original bytes on failure/unsupported)
            new_title = build_tag_title(
                leader_first=sub.leader_first,
                leader_last=sub.leader_last,
                follower_first=sub.follower_first,
                follower_last=sub.follower_last,
            )
            new_artist = build_tag_artist(
                division=sub.division,
                season_year=season_year,
                routine_name=sub.routine_name,
                personal_descriptor=sub.personal_descriptor,
            )

            tagged_bytes = tag_audio_bytes_preserve_previous(
                filename_for_type=final_filename,
                audio_bytes=original.data,
                new_title=new_title,
                new_artist=new_artist,
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
                "Row %s: uploaded %s (id=%s)", row_num, final_filename, new_file_id
            )

            # Delete original only after successful upload
            delete_drive_file(drive, file_id)
            log.info("Row %s: deleted original file %s", row_num, file_id)

            # Mark processed in the sheet last
            mark_row_processed(sheet, row_num, processed_col)
            log.info("Row %s: marked processed", row_num)

        except Exception as e:
            # Do not mark processed. This row will retry next run.
            log.exception("Row %s: failed to process: %s", row_num, e)

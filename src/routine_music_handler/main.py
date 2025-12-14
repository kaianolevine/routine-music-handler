from __future__ import annotations

import sys

from kaiano_common_utils import _google_credentials

from .processor import process_submission_sheet


def main() -> int:

    worksheet_name = "Form Responses 1"
    dest_root_folder_id = "1I7NxCM4RLYhXmQ1BaQjsNvKNWgMClgLH"
    submission_sheet_id = "1V2hupjlrsCtOTU4p_hALCTRKDaB4We84tLnGUCecLLU"
    submissions_folder_id = (
        "1CeUNN08N5SMgZf1RQGHfqPO_ZyrAJzc0M_i2xZCeVuR-TAbnDrncnz9CGJHWY4Af1IecJMi9"
    )

    drive = _google_credentials.get_drive_client()
    sheets = _google_credentials.get_gspread_client()

    ss = sheets.open_by_key(submission_sheet_id)
    sheet = ss.worksheet(worksheet_name) if worksheet_name else ss.sheet1

    process_submission_sheet(
        sheet=sheet,
        drive=drive,
        submissions_folder_id=submissions_folder_id,
        dest_root_folder_id=dest_root_folder_id,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())

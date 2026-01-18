from __future__ import annotations

import sys

from kaiano.google import GoogleAPI

from routine_music_handler.processor import process_submission_sheet


def main() -> int:

    worksheet_name = "Form Responses 1"
    dest_root_folder_id = "1I7NxCM4RLYhXmQ1BaQjsNvKNWgMClgLH"
    submission_sheet_id = "1V2hupjlrsCtOTU4p_hALCTRKDaB4We84tLnGUCecLLU"
    submissions_folder_id = (
        "1CeUNN08N5SMgZf1RQGHfqPO_ZyrAJzc0M_i2xZCeVuR-TAbnDrncnz9CGJHWY4Af1IecJMi9"
    )

    g = GoogleAPI.from_env()

    process_submission_sheet(
        g=g,
        submission_sheet_id=submission_sheet_id,
        worksheet_name=worksheet_name,
        submissions_folder_id=submissions_folder_id,
        dest_root_folder_id=dest_root_folder_id,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())

from __future__ import annotations

import logging
import os
import sys

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from .processor import process_submission_sheet


def build_clients():
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS env var is required (path to service account JSON)."
        )

    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)

    drive = build("drive", "v3", credentials=creds)

    # gspread can reuse google-auth creds
    sheets = gspread.authorize(creds)
    return drive, sheets


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    submission_sheet_id = os.environ.get("SUBMISSION_SHEET_ID")
    if not submission_sheet_id:
        raise RuntimeError("SUBMISSION_SHEET_ID env var is required.")

    submissions_folder_id = os.environ.get("SUBMISSIONS_FOLDER_ID")
    if not submissions_folder_id:
        raise RuntimeError("SUBMISSIONS_FOLDER_ID env var is required.")

    dest_root_folder_id = os.environ.get("DEST_ROOT_FOLDER_ID")  # optional
    worksheet_name = os.environ.get("WORKSHEET_NAME")  # optional

    drive, sheets = build_clients()

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

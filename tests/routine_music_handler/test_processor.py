# tests/routine_music_handler/test_processor.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from routine_music_handler import processor

# ----------------------------
# Lightweight fakes
# ----------------------------


@dataclass(frozen=True)
class _Downloaded:
    file_id: str
    name: str
    mime_type: str
    data: bytes


class FakeWorksheet:
    """Minimal gspread Worksheet-ish object for processor.py."""

    def __init__(self, title: str, values: list[list[str]]):
        self.title = title
        self._values = values[:]  # includes header row at index 0
        self.updated_cells: list[tuple[int, int, str]] = []
        self.updated_ranges: list[tuple[str, list[list[Any]]]] = []
        self.appended_rows: list[list[Any]] = []
        self.cleared_ranges: list[str] = []

    def get_all_values(self) -> list[list[str]]:
        return [row[:] for row in self._values]

    def update_cell(self, row: int, col: int, value: str) -> None:
        # gspread is 1-based
        while len(self._values) < row:
            self._values.append([])
        while len(self._values[row - 1]) < col:
            self._values[row - 1].append("")
        self._values[row - 1][col - 1] = value
        self.updated_cells.append((row, col, value))

    def row_values(self, row: int) -> list[str]:
        if row <= 0 or row > len(self._values):
            return []
        return self._values[row - 1][:]

    def update(
        self, a1: str, values: list[list[Any]], value_input_option: str = "RAW"
    ) -> None:
        self.updated_ranges.append((a1, values))

    def append_row(self, row: list[Any], value_input_option: str = "RAW") -> None:
        self.appended_rows.append(row)
        self._values.append([str(x) if x is not None else "" for x in row])

    def batch_clear(self, ranges: list[str]) -> None:
        self.cleared_ranges.extend(ranges)


class FakeGspreadSpreadsheet:
    def __init__(
        self,
        sheet1: FakeWorksheet,
        worksheets: Optional[dict[str, FakeWorksheet]] = None,
    ):
        self.sheet1 = sheet1
        self._worksheets = worksheets or {sheet1.title: sheet1}

    def worksheet(self, name: str) -> FakeWorksheet:
        if name not in self._worksheets:
            raise KeyError(name)
        return self._worksheets[name]

    def add_worksheet(self, title: str, rows: int, cols: int) -> FakeWorksheet:
        ws = FakeWorksheet(title=title, values=[[""] * cols])
        self._worksheets[title] = ws
        return ws


class FakeGspreadClient:
    def __init__(self, spreadsheets: dict[str, FakeGspreadSpreadsheet]):
        self._spreadsheets = spreadsheets

    def open_by_key(self, spreadsheet_id: str) -> FakeGspreadSpreadsheet:
        return self._spreadsheets[spreadsheet_id]


class FakeSheetsFormatter:
    def __init__(self):
        self.applied_to_ws_titles: list[str] = []

    def apply_sheet_formatting(self, ws: FakeWorksheet) -> None:
        self.applied_to_ws_titles.append(ws.title)


class FakeSheetsFacade:
    def __init__(self):
        self.formatter = FakeSheetsFormatter()


class FakeDriveFacade:
    def __init__(self):
        self.calls: list[tuple[str, Any]] = []
        self._next_upload_id = "new_file_id_1"

        # behavior knobs
        self.extracted_id: Optional[str] = "file_123"
        self.downloaded = _Downloaded(
            file_id="file_123",
            name="source.mp3",
            mime_type="audio/mpeg",
            data=b"ORIGINAL_BYTES",
        )
        self.final_filename = "Final_v1.mp3"
        self.final_version = 1
        self.created_spreadsheet_id = "submitted_music_sheet_id"
        self.raise_on_delete = False

    def extract_drive_file_id(self, url_or_id: str) -> Optional[str]:
        self.calls.append(("extract_drive_file_id", url_or_id))
        return self.extracted_id

    def ensure_folder(self, parent_id: str, name: str) -> str:
        self.calls.append(("ensure_folder", (parent_id, name)))
        return "dest_folder_id"

    def download_file_bytes(self, file_id: str) -> _Downloaded:
        self.calls.append(("download_file_bytes", file_id))
        return self.downloaded

    def resolve_versioned_filename(
        self, *, parent_folder_id: str, desired_filename: str
    ) -> tuple[str, int]:
        self.calls.append(
            ("resolve_versioned_filename", (parent_folder_id, desired_filename))
        )
        return self.final_filename, self.final_version

    def upload_bytes(
        self, *, parent_id: str, filename: str, content: bytes, mime_type: str
    ) -> str:
        self.calls.append(("upload_bytes", (parent_id, filename, content, mime_type)))
        return self._next_upload_id

    def find_or_create_spreadsheet(self, *, parent_folder_id: str, name: str) -> str:
        self.calls.append(("find_or_create_spreadsheet", (parent_folder_id, name)))
        return self.created_spreadsheet_id

    def delete_file_with_fallback(
        self, file_id: str, *, fallback_remove_parent_id: str | None = None
    ) -> None:
        self.calls.append(
            ("delete_file_with_fallback", (file_id, fallback_remove_parent_id))
        )
        if self.raise_on_delete:
            raise PermissionError("nope")


class FakeGoogleAPI:
    def __init__(
        self,
        *,
        gspread: FakeGspreadClient,
        drive: FakeDriveFacade,
        sheets: FakeSheetsFacade,
    ):
        self.gspread = gspread
        self.drive = drive
        self.sheets = sheets


# ----------------------------
# Helpers to build sheet rows
# ----------------------------


def _header_row() -> list[str]:
    # processor uses fixed positions but doesn't care about header contents
    return [f"H{i}" for i in range(processor.INPUT_COL_COUNT)] + ["Processed"]


def _submission_row(
    *,
    audio_url: str = "https://drive.google.com/file/d/file_123/view",
    division: str = "Novice",
    routine_name: str = "My Routine",
    descriptor: str = "Cool",
    processed: str = "",
) -> list[str]:
    # Must be at least INPUT_COL_COUNT columns (positions)
    row = [""] * processor.INPUT_COL_COUNT
    row[processor.FormCols.TIMESTAMP] = "01/18/2026 20:00:00"
    row[processor.FormCols.LEADER_FIRST] = "Alice"
    row[processor.FormCols.LEADER_LAST] = "Leader"
    row[processor.FormCols.FOLLOWER_FIRST] = "Bob"
    row[processor.FormCols.FOLLOWER_LAST] = "Follower"
    row[processor.FormCols.DIVISION] = division
    row[processor.FormCols.ROUTINE_NAME] = routine_name
    row[processor.FormCols.PERSONAL_DESCRIPTOR] = descriptor
    row[processor.FormCols.AUDIO_FILE_URL] = audio_url
    return row + [processed]


# ----------------------------
# Tests
# ----------------------------


def test_process_submission_sheet_happy_path_marks_processed(monkeypatch):
    # Make tagging deterministic and avoid touching music_tag
    monkeypatch.setattr(
        processor,
        "tag_audio_bytes_preserve_previous",
        lambda **kwargs: b"TAGGED_BYTES",
    )

    # Submission sheet (input)
    submission_sheet_id = "submission_sheet_id"
    ws = FakeWorksheet(
        title="Form Responses",
        values=[
            _header_row(),
            _submission_row(processed=""),
        ],
    )
    submission_ss = FakeGspreadSpreadsheet(sheet1=ws, worksheets={"Form Responses": ws})

    # _Submitted_Music sheet that will be created/used
    submitted_ws = FakeWorksheet(
        title="Novice",
        values=[
            [
                "Timestamp",
                "Partnership",
                "Division",
                "Routine Name",
                "Descriptor",
                "Version",
            ]
        ],
    )
    submitted_ss = FakeGspreadSpreadsheet(
        sheet1=submitted_ws, worksheets={"Novice": submitted_ws}
    )

    gspread = FakeGspreadClient(
        spreadsheets={
            submission_sheet_id: submission_ss,
            "submitted_music_sheet_id": submitted_ss,
        }
    )

    drive = FakeDriveFacade()
    sheets = FakeSheetsFacade()
    g = FakeGoogleAPI(gspread=gspread, drive=drive, sheets=sheets)

    processor.process_submission_sheet(
        g=g,
        submission_sheet_id=submission_sheet_id,
        worksheet_name="Form Responses",
        submissions_folder_id="submissions_folder_id",
        dest_root_folder_id=None,
    )

    # Marked processed (row 2, processed col is INPUT_COL_COUNT+1)
    processed_col = processor.get_processed_col_index()
    assert (2, processed_col, "X") in ws.updated_cells

    # Ensure key drive calls happened in expected flow
    op_names = [c[0] for c in drive.calls]
    assert "extract_drive_file_id" in op_names
    assert "download_file_bytes" in op_names
    assert "upload_bytes" in op_names
    assert "delete_file_with_fallback" in op_names


def test_process_submission_sheet_skips_if_missing_audio_url(monkeypatch):
    submission_sheet_id = "submission_sheet_id"
    ws = FakeWorksheet(
        title="Form Responses",
        values=[
            _header_row(),
            _submission_row(audio_url="", processed=""),
        ],
    )
    submission_ss = FakeGspreadSpreadsheet(sheet1=ws, worksheets={"Form Responses": ws})
    gspread = FakeGspreadClient(spreadsheets={submission_sheet_id: submission_ss})

    drive = FakeDriveFacade()
    sheets = FakeSheetsFacade()
    g = FakeGoogleAPI(gspread=gspread, drive=drive, sheets=sheets)

    processor.process_submission_sheet(
        g=g,
        submission_sheet_id=submission_sheet_id,
        worksheet_name="Form Responses",
        submissions_folder_id="submissions_folder_id",
        dest_root_folder_id=None,
    )

    # No drive calls, no processed mark
    assert drive.calls == []
    assert ws.updated_cells == []


def test_process_submission_sheet_skips_if_cannot_extract_file_id(monkeypatch):
    submission_sheet_id = "submission_sheet_id"
    ws = FakeWorksheet(
        title="Form Responses",
        values=[
            _header_row(),
            _submission_row(processed=""),
        ],
    )
    submission_ss = FakeGspreadSpreadsheet(sheet1=ws, worksheets={"Form Responses": ws})
    gspread = FakeGspreadClient(spreadsheets={submission_sheet_id: submission_ss})

    drive = FakeDriveFacade()
    drive.extracted_id = None
    sheets = FakeSheetsFacade()
    g = FakeGoogleAPI(gspread=gspread, drive=drive, sheets=sheets)

    processor.process_submission_sheet(
        g=g,
        submission_sheet_id=submission_sheet_id,
        worksheet_name="Form Responses",
        submissions_folder_id="submissions_folder_id",
        dest_root_folder_id=None,
    )

    assert (
        "extract_drive_file_id",
        "https://drive.google.com/file/d/file_123/view",
    ) in drive.calls
    assert all(op != "download_file_bytes" for op, _ in drive.calls)
    assert ws.updated_cells == []


def test_process_submission_sheet_does_not_mark_processed_if_delete_fails(monkeypatch):
    monkeypatch.setattr(
        processor,
        "tag_audio_bytes_preserve_previous",
        lambda **kwargs: b"TAGGED_BYTES",
    )

    submission_sheet_id = "submission_sheet_id"
    ws = FakeWorksheet(
        title="Form Responses",
        values=[_header_row(), _submission_row(processed="")],
    )
    submission_ss = FakeGspreadSpreadsheet(sheet1=ws, worksheets={"Form Responses": ws})

    submitted_ws = FakeWorksheet(
        title="Novice",
        values=[
            [
                "Timestamp",
                "Partnership",
                "Division",
                "Routine Name",
                "Descriptor",
                "Version",
            ]
        ],
    )
    submitted_ss = FakeGspreadSpreadsheet(
        sheet1=submitted_ws, worksheets={"Novice": submitted_ws}
    )

    gspread = FakeGspreadClient(
        spreadsheets={
            submission_sheet_id: submission_ss,
            "submitted_music_sheet_id": submitted_ss,
        }
    )

    drive = FakeDriveFacade()
    drive.raise_on_delete = True  # force failure after upload
    sheets = FakeSheetsFacade()
    g = FakeGoogleAPI(gspread=gspread, drive=drive, sheets=sheets)

    processor.process_submission_sheet(
        g=g,
        submission_sheet_id=submission_sheet_id,
        worksheet_name="Form Responses",
        submissions_folder_id="submissions_folder_id",
        dest_root_folder_id=None,
    )

    # Should NOT mark processed if delete fails (since exception caught in outer try)
    assert ws.updated_cells == []


def test_tag_audio_bytes_preserve_previous_returns_original_on_failure(monkeypatch):
    # Force music_tag to raise
    monkeypatch.setattr(
        processor.music_tag,
        "load_file",
        lambda _p: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    out = processor.tag_audio_bytes_preserve_previous(
        filename_for_type="x.mp3",
        audio_bytes=b"ORIG",
        new_title="t",
        new_artist="a",
    )
    assert out == b"ORIG"

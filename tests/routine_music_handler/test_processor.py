from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest

from routine_music_handler import processor

# -----------------------------------------------------------------------------
# Minimal fakes (gspread-ish + GoogleAPI-ish)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class _Downloaded:
    file_id: str
    name: str
    mime_type: str
    data: bytes


class FakeWorksheet:
    """Minimal gspread Worksheet-ish object used by processor.py."""

    def __init__(self, title: str, values: list[list[str]]):
        self.title = title
        self._values = [row[:] for row in values]  # includes header row at index 0

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


class FakeSpreadsheet:
    def __init__(
        self,
        sheet1: FakeWorksheet,
        worksheets: Optional[dict[str, FakeWorksheet]] = None,
    ):
        self.sheet1 = sheet1
        self._worksheets = worksheets or {sheet1.title: sheet1}

    def worksheet(self, name: str) -> FakeWorksheet:
        if name not in self._worksheets:
            raise Exception(name)
        return self._worksheets[name]

    def add_worksheet(self, title: str, rows: int, cols: int) -> FakeWorksheet:
        ws = FakeWorksheet(title=title, values=[[""] * cols])
        self._worksheets[title] = ws
        return ws


class FakeGspreadClient:
    def __init__(self, spreadsheets: dict[str, FakeSpreadsheet]):
        self._spreadsheets = spreadsheets

    def open_by_key(self, spreadsheet_id: str) -> FakeSpreadsheet:
        return self._spreadsheets[spreadsheet_id]


class FakeSheetsFormatter:
    def __init__(self):
        self.applied_to_titles: list[str] = []

    def apply_sheet_formatting(self, ws: FakeWorksheet) -> None:
        self.applied_to_titles.append(ws.title)


class FakeSheetsFacade:
    """SheetsFacade-ish surface used by processor.py: normalize_row + formatter."""

    def __init__(self):
        self.formatter = FakeSheetsFormatter()

    @staticmethod
    def normalize_row(row: list[Any]) -> list[str]:
        # Mirrors common intent: None -> "", strip whitespace.
        out: list[str] = []
        for v in row:
            if v is None:
                out.append("")
            else:
                out.append(str(v).strip())
        return out


class FakeDriveFacade:
    def __init__(self):
        self.calls: list[tuple[str, Any]] = []

        # knobs
        self.extracted_id: Optional[str] = "file_123"
        self.downloaded = _Downloaded(
            file_id="file_123",
            name="source.mp3",
            mime_type="audio/mpeg",
            data=b"ORIGINAL_BYTES",
        )
        self.resolve_filename = ("Base_v1.mp3", 1)
        self.upload_id = "uploaded_1"
        self.submitted_music_id = "submitted_music_sheet_id"
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
        return self.resolve_filename

    def upload_bytes(
        self, *, parent_id: str, filename: str, content: bytes, mime_type: str
    ) -> str:
        self.calls.append(("upload_bytes", (parent_id, filename, content, mime_type)))
        return self.upload_id

    def find_or_create_spreadsheet(self, *, parent_folder_id: str, name: str) -> str:
        self.calls.append(("find_or_create_spreadsheet", (parent_folder_id, name)))
        return self.submitted_music_id

    def delete_file_with_fallback(
        self, file_id: str, *, fallback_remove_parent_id: str | None = None
    ) -> None:
        self.calls.append(
            ("delete_file_with_fallback", (file_id, fallback_remove_parent_id))
        )
        if self.raise_on_delete:
            raise PermissionError("delete failed")


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


# -----------------------------------------------------------------------------
# Helpers to build sheet rows
# -----------------------------------------------------------------------------


def _header_row() -> list[str]:
    # processor uses fixed positions but doesn't validate header contents
    return [f"H{i}" for i in range(processor.INPUT_FORM_COL_COUNT)] + ["Processed"]


def _submission_row(
    *,
    audio_url: str = "https://drive.google.com/file/d/file_123/view",
    division: str = "Novice",
    routine_name: str = "My Routine",
    descriptor: str = "Cool",
    processed: str = "",
) -> list[str]:
    row = [""] * processor.INPUT_FORM_COL_COUNT

    # timestamp must match _parse_routine_season_year format: %m/%d/%Y %H:%M:%S
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


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


def test_process_submission_sheet_happy_path_marks_processed(monkeypatch):
    # Avoid actual tagging work
    monkeypatch.setattr(
        processor, "tag_audio_bytes_preserve_previous", lambda **kwargs: b"TAGGED_BYTES"
    )

    # Make routine filename deterministic for this test (avoid coupling to renamer internals)
    monkeypatch.setattr(
        processor.Mp3Renamer, "build_routine_filename", lambda **kwargs: "BaseName"
    )

    # Make tag strings deterministic
    monkeypatch.setattr(
        processor.Mp3Tagger, "build_routine_tag_title", lambda **kwargs: "TITLE"
    )
    monkeypatch.setattr(
        processor.Mp3Tagger, "build_routine_tag_artist", lambda **kwargs: "ARTIST"
    )

    submission_sheet_id = "submission_sheet_id"
    ws = FakeWorksheet(
        title="Form Responses",
        values=[
            _header_row(),
            _submission_row(processed=""),
        ],
    )
    submission_ss = FakeSpreadsheet(sheet1=ws, worksheets={"Form Responses": ws})

    # Submitted music spreadsheet (per-division tabs)
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
    submitted_ss = FakeSpreadsheet(
        sheet1=submitted_ws, worksheets={"Novice": submitted_ws}
    )

    gspread = FakeGspreadClient(
        spreadsheets={
            submission_sheet_id: submission_ss,
            "submitted_music_sheet_id": submitted_ss,
        }
    )

    drive = FakeDriveFacade()
    # deterministic versioned filename result
    drive.resolve_filename = ("Base_v1.mp3", 1)
    sheets = FakeSheetsFacade()
    g = FakeGoogleAPI(gspread=gspread, drive=drive, sheets=sheets)

    processor.process_submission_sheet(
        g=g,
        submission_sheet_id=submission_sheet_id,
        worksheet_name="Form Responses",
        submissions_folder_id="submissions_folder_id",
        dest_root_folder_id=None,
    )

    # Marked processed (row 2, processed col is INPUT_FORM_COL_COUNT+1)
    processed_col = processor.PROCESSED_INDEX
    assert (2, processed_col, "X") in ws.updated_cells

    # Ensure core drive calls happened
    op_names = [c[0] for c in drive.calls]
    assert op_names.count("extract_drive_file_id") == 1
    assert "download_file_bytes" in op_names
    assert "resolve_versioned_filename" in op_names
    assert "upload_bytes" in op_names
    assert "delete_file_with_fallback" in op_names

    # Submitted music tab should have appended at least one row
    assert (
        submitted_ws.appended_rows
    ), "expected a row to be appended to _Submitted_Music tab"


def test_process_submission_sheet_skips_if_missing_audio_url():
    submission_sheet_id = "submission_sheet_id"
    ws = FakeWorksheet(
        title="Form Responses",
        values=[_header_row(), _submission_row(audio_url="", processed="")],
    )
    submission_ss = FakeSpreadsheet(sheet1=ws, worksheets={"Form Responses": ws})

    # Submitted music spreadsheet (per-division tabs) needed for snapshot publishing.
    submitted_ws = FakeWorksheet(
        title="Sheet1",
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
    submitted_ss = FakeSpreadsheet(
        sheet1=submitted_ws, worksheets={"Sheet1": submitted_ws}
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

    # We still resolve _Submitted_Music up front to publish a snapshot, but we should not
    # process the submission row or mark it processed.
    assert [c[0] for c in drive.calls] == ["find_or_create_spreadsheet"]
    assert ws.updated_cells == []


def test_process_submission_sheet_skips_if_cannot_extract_file_id():
    submission_sheet_id = "submission_sheet_id"
    ws = FakeWorksheet(
        title="Form Responses", values=[_header_row(), _submission_row(processed="")]
    )
    submission_ss = FakeSpreadsheet(sheet1=ws, worksheets={"Form Responses": ws})
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
        processor, "tag_audio_bytes_preserve_previous", lambda **kwargs: b"TAGGED_BYTES"
    )
    monkeypatch.setattr(
        processor.Mp3Renamer, "build_routine_filename", lambda **kwargs: "BaseName"
    )
    monkeypatch.setattr(
        processor.Mp3Tagger, "build_routine_tag_title", lambda **kwargs: "TITLE"
    )
    monkeypatch.setattr(
        processor.Mp3Tagger, "build_routine_tag_artist", lambda **kwargs: "ARTIST"
    )

    submission_sheet_id = "submission_sheet_id"
    ws = FakeWorksheet(
        title="Form Responses", values=[_header_row(), _submission_row(processed="")]
    )
    submission_ss = FakeSpreadsheet(sheet1=ws, worksheets={"Form Responses": ws})

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
    submitted_ss = FakeSpreadsheet(
        sheet1=submitted_ws, worksheets={"Novice": submitted_ws}
    )

    gspread = FakeGspreadClient(
        spreadsheets={
            submission_sheet_id: submission_ss,
            "submitted_music_sheet_id": submitted_ss,
        }
    )

    drive = FakeDriveFacade()
    drive.raise_on_delete = True
    sheets = FakeSheetsFacade()
    g = FakeGoogleAPI(gspread=gspread, drive=drive, sheets=sheets)

    processor.process_submission_sheet(
        g=g,
        submission_sheet_id=submission_sheet_id,
        worksheet_name="Form Responses",
        submissions_folder_id="submissions_folder_id",
        dest_root_folder_id=None,
    )

    # If delete fails, outer try catches and we do NOT mark processed.
    assert ws.updated_cells == []


def test_iter_unprocessed_rows_header_only_yields_nothing():
    ws = FakeWorksheet(title="Form Responses", values=[_header_row()])
    rows = list(processor._iter_unprocessed_rows(ws, processor.PROCESSED_INDEX))
    assert rows == []


def test_process_submission_sheet_creates_division_tab_and_applies_formatting(
    monkeypatch,
):
    """Covers the branch where the division worksheet does not exist and must be created."""

    monkeypatch.setattr(
        processor, "tag_audio_bytes_preserve_previous", lambda **kwargs: b"TAGGED_BYTES"
    )
    monkeypatch.setattr(
        processor.Mp3Renamer, "build_routine_filename", lambda **kwargs: "BaseName"
    )
    monkeypatch.setattr(
        processor.Mp3Tagger, "build_routine_tag_title", lambda **kwargs: "TITLE"
    )
    monkeypatch.setattr(
        processor.Mp3Tagger, "build_routine_tag_artist", lambda **kwargs: "ARTIST"
    )

    submission_sheet_id = "submission_sheet_id"
    ws = FakeWorksheet(
        title="Form Responses",
        values=[_header_row(), _submission_row(division="NewDivision")],
    )
    submission_ss = FakeSpreadsheet(sheet1=ws, worksheets={"Form Responses": ws})

    # Submitted music spreadsheet has NO division tab initially.
    sheet1 = FakeWorksheet(title="Sheet1", values=[[""]])
    submitted_ss = FakeSpreadsheet(sheet1=sheet1, worksheets={"Sheet1": sheet1})

    gspread = FakeGspreadClient(
        spreadsheets={
            submission_sheet_id: submission_ss,
            "submitted_music_sheet_id": submitted_ss,
        }
    )

    drive = FakeDriveFacade()
    drive.resolve_filename = ("Base_v1.mp3", 1)
    sheets = FakeSheetsFacade()
    g = FakeGoogleAPI(gspread=gspread, drive=drive, sheets=sheets)

    processor.process_submission_sheet(
        g=g,
        submission_sheet_id=submission_sheet_id,
        worksheet_name="Form Responses",
        submissions_folder_id="submissions_folder_id",
        dest_root_folder_id=None,
    )

    # Formatter should have been applied to the newly created division tab.
    assert "NewDivision" in g.sheets.formatter.applied_to_titles


def test_sanitize_user_entered_data_collapses_and_strips_special_chars():
    s = processor._sanitize_user_entered_data_from_form

    assert s("") == ""
    assert s("   ") == ""
    assert s("Alice   Leader") == "AliceLeader"  # spaces removed per confirmed intent
    assert s("A/B") == "AB"
    assert s("Beyoncé") == "Beyonc"  # non-ascii removed by regex
    assert s("__A__") == "A"  # strip leading/trailing underscores


def test_parse_routine_season_year_parses_and_raises_on_invalid():
    assert processor._parse_routine_season_year("01/18/2026 20:00:00") == "2026"

    with pytest.raises(Exception):
        processor._parse_routine_season_year("bad")


def test_append_and_sort_submission_log_row_sorts_and_rewrites():
    """Directly covers the in-tab sort + rewrite behavior."""

    ws = FakeWorksheet(
        title="Novice",
        values=[
            [
                "Timestamp",
                "Partnership",
                "Division",
                "Routine Name",
                "Descriptor",
                "Version",
            ],
            ["t", "Zed & Z", "D", "R", "X", "1"],
            ["t", "Alice & A", "D", "R", "X", "3"],
            ["t", "Alice & A", "D", "R", "X", "1"],
        ],
    )

    # Append adds a new row and then triggers sort.
    processor._append_and_sort_submission_log_row(
        ws=ws,
        timestamp_value="t",
        partnership="Bob & B",
        division="D",
        routine_name="R",
        descriptor="X",
        version=2,
    )

    # After sort/rewrite, we should have issued a batch_clear + update.
    assert ws.cleared_ranges, "expected batch_clear"
    assert ws.updated_ranges, "expected update"


def test_append_and_sort_submission_log_row_swallow_errors(monkeypatch):
    """Covers the exception handler path inside _append_and_sort_submission_log_row."""

    ws = FakeWorksheet(
        title="Novice",
        values=[
            [
                "Timestamp",
                "Partnership",
                "Division",
                "Routine Name",
                "Descriptor",
                "Version",
            ],
            ["t", "A", "D", "R", "X", "1"],
        ],
    )

    # Force get_all_values to raise during sort.
    monkeypatch.setattr(
        ws, "get_all_values", lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    # Should not raise.
    processor._append_and_sort_submission_log_row(
        ws=ws,
        timestamp_value="t",
        partnership="B",
        division="D",
        routine_name="R",
        descriptor="X",
        version=1,
    )


def test_tag_audio_bytes_preserve_previous_returns_original_on_error(monkeypatch):
    """Covers best-effort tagging fallback."""

    # Force tagger read to fail.
    monkeypatch.setattr(
        processor.Mp3Tagger,
        "read",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    out = processor.tag_audio_bytes_preserve_previous(
        filename_for_type="x.mp3",
        audio_bytes=b"ORIG",
        new_title="T",
        new_artist="A",
    )
    assert out == b"ORIG"


def test_main_calls_processor(monkeypatch):
    """Covers routine_music_handler/main.py wiring."""

    import routine_music_handler.main as main_mod

    called = {}

    class _G:
        pass

    monkeypatch.setattr(main_mod.GoogleAPI, "from_env", lambda: _G())

    def _fake_process(**kwargs):
        called.update(kwargs)

    monkeypatch.setattr(main_mod, "process_submission_sheet", _fake_process)

    # Exercise main() with defaults. It should call process_submission_sheet with ids.
    rc = main_mod.main()
    assert rc == 0

    assert "g" in called
    assert "submission_sheet_id" in called
    assert "worksheet_name" in called
    assert "submissions_folder_id" in called

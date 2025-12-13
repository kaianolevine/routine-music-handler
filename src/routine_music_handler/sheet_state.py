from __future__ import annotations

from .submission_schema import INPUT_COL_COUNT


def ensure_processed_col_is_last(sheet) -> int:
    """Option A: processed flag is the last column; ensure at least one column after inputs."""
    min_cols = INPUT_COL_COUNT + 1
    if sheet.col_count < min_cols:
        sheet.resize(cols=min_cols)
    # processed column is the last column (1-based)
    return sheet.col_count


def iter_unprocessed_rows(sheet, processed_col: int):
    """Yield (row_num, row_values) for rows whose last column != 'X'."""
    values = sheet.get_all_values()
    if len(values) <= 1:
        return
    for idx, row in enumerate(values[1:], start=2):  # header is row 1
        while len(row) < processed_col:
            row.append("")
        if (row[processed_col - 1] or "").strip().upper() == "X":
            continue
        yield idx, row


def mark_row_processed(sheet, row_num: int, processed_col: int) -> None:
    sheet.update_cell(row_num, processed_col, "X")

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

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


def normalize_cell(v: Any) -> str:
    """Coerce to string and trim whitespace; None becomes empty string."""
    return "" if v is None else str(v).strip()


def normalize_row(row: Sequence[Any]) -> list[str]:
    """Normalize all values in a row (trim-by-default)."""
    return [normalize_cell(v) for v in row]


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


def parse_submission_row(row: Sequence[Any]) -> Submission:
    """Parse a raw sheet row using fixed positions; trims all fields by default."""
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

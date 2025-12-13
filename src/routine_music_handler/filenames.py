from __future__ import annotations

import re
from datetime import datetime

from .submission_schema import Submission

_NON_ALNUM = re.compile(r"[^A-Za-z0-9_]+")


def sanitize_part(value: str) -> str:
    """trim -> lower -> TitleCaseWords -> remove spaces -> sanitize -> collapse underscores"""
    if not value:
        return ""
    v = value.strip().lower()
    v = "".join(word.capitalize() for word in v.split())
    v = _NON_ALNUM.sub("_", v)
    v = re.sub(r"_+", "_", v).strip("_")
    return v


def infer_season_year(timestamp: str) -> str:
    """Google Forms timestamp 'M/D/YYYY HH:MM:SS'. If month >= 11, season year is next year."""
    dt = datetime.strptime(timestamp.strip(), "%m/%d/%Y %H:%M:%S")
    year = dt.year + (1 if dt.month >= 11 else 0)
    return str(year)


def build_base_filename(sub: Submission) -> tuple[str, str]:
    """Return (base_without_version_or_ext, season_year). Base includes season year and optional fields."""
    season_year = infer_season_year(sub.timestamp)

    prefix = "_".join(
        [
            sanitize_part(sub.leader_first + sub.leader_last),
            sanitize_part(sub.follower_first + sub.follower_last),
            sanitize_part(sub.division),
        ]
    )

    routine = sanitize_part(sub.routine_name)
    descriptor = sanitize_part(sub.personal_descriptor)

    tail_parts: list[str] = [season_year]
    if routine:
        tail_parts.append(routine)
    if descriptor:
        tail_parts.append(descriptor)

    return f"{prefix}_{'_'.join(tail_parts)}", season_year

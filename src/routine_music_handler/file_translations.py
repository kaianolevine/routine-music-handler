from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime
from typing import Any

import music_tag

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


def _parse_season_year(timestamp: str) -> str:
    """Google Forms timestamp 'M/D/YYYY HH:MM:SS'. If month >= 11, season year is next year."""
    dt = datetime.strptime(timestamp.strip(), "%m/%d/%Y %H:%M:%S")
    year = dt.year + (1 if dt.month >= 11 else 0)
    return str(year)


def build_base_filename(
    timestamp, leader, follower, division, routine, descriptor
) -> tuple[str, str]:
    """Return (base_without_version_or_ext, season_year). Base includes season year and optional fields."""
    season_year = _parse_season_year(timestamp)

    prefix = "_".join(
        [
            (leader),
            (follower),
            (division),
        ]
    )

    tail_parts: list[str] = [season_year]
    if routine:
        tail_parts.append(routine)
    if descriptor:
        tail_parts.append(descriptor)

    return f"{prefix}_{'_'.join(tail_parts)}", season_year


def _as_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def build_tag_title(
    *, leader_first: str, leader_last: str, follower_first: str, follower_last: str
) -> str:
    # LeaderFirstLeaderLast & FollowerFirstFollowerLast (no spaces between first/last)
    leader = f"{_as_str(leader_first)}{_as_str(leader_last)}"
    follower = f"{_as_str(follower_first)}{_as_str(follower_last)}"
    return f"{leader} & {follower}".strip()


def build_tag_artist(
    *,
    version: str,
    division: str,
    season_year: str,
    routine_name: str,
    personal_descriptor: str,
) -> str:
    base = f"v{_as_str(version)} | {_as_str(division)} {_as_str(season_year)}".strip()
    parts = [base]
    rn = _as_str(routine_name)
    pd = _as_str(personal_descriptor)
    if rn:
        parts.append(rn)
    if pd:
        parts.append(pd)
    return " | ".join([p for p in parts if p])


def tag_audio_bytes_preserve_previous(
    *,
    filename_for_type: str,
    audio_bytes: bytes,
    new_title: str,
    new_artist: str,
) -> bytes:
    """Best-effort tag application using `music-tag`.

    `music-tag` is a thin layer on top of mutagen that provides a consistent
    tag interface across formats.

    Behavior:
      - Read existing Title/Artist/Album/Comment (when available)
      - Concatenate non-empty values with " | " and store it in Comment
      - Set Title and Artist to the provided values
      - Leave Album unchanged

    If tagging fails or the format isn't supported, return original bytes.

    Note: music-tag operates on files, so we write to a temp file, edit tags,
    save, and read bytes back.
    """

    # Preserve original extension for better format detection.
    ext = (
        filename_for_type.rsplit(".", 1)[-1].lower() if "." in filename_for_type else ""
    )
    suffix = f".{ext}" if ext else ""

    try:
        with tempfile.TemporaryDirectory() as td:
            tmp_path = os.path.join(td, f"audio{suffix}")
            with open(tmp_path, "wb") as f:
                f.write(audio_bytes)

            mf = music_tag.load_file(tmp_path)

            # music-tag keys are flexible, but these are the canonical ones.
            existing_title = _as_str(getattr(mf.get("title"), "first", ""))
            existing_artist = _as_str(getattr(mf.get("artist"), "first", ""))
            existing_album = _as_str(getattr(mf.get("album"), "first", ""))
            existing_comment = _as_str(getattr(mf.get("comment"), "first", ""))

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

            # Set new tags
            mf["title"] = new_title
            mf["artist"] = new_artist
            if prev_concat:
                mf["comment"] = prev_concat

            mf.save()

            with open(tmp_path, "rb") as f:
                return f.read()

    except Exception:
        return audio_bytes

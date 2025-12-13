from __future__ import annotations

import io
from typing import Any

from mutagen import File as MutagenFile
from mutagen.flac import FLAC
from mutagen.id3 import COMM, ID3, TIT2, TPE1


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
    *, division: str, season_year: str, routine_name: str, personal_descriptor: str
) -> str:
    base = f"{_as_str(division)} {_as_str(season_year)}".strip()
    parts = [base]
    rn = _as_str(routine_name)
    pd = _as_str(personal_descriptor)
    if rn:
        parts.append(rn)
    if pd:
        parts.append(pd)
    return ", ".join([p for p in parts if p])


def tag_audio_bytes_preserve_previous(
    *,
    filename_for_type: str,
    audio_bytes: bytes,
    new_title: str,
    new_artist: str,
) -> bytes:
    """Best-effort tag application. If unsupported, return original bytes.

    Before writing:
      - Collect existing Title/Artist/Album/Comment (when available)
      - Concatenate non-empty values with " | "
      - Store that concatenation in Comment
    Then:
      - Set Title and Artist to the provided values.
      - Leave Album unchanged.
    """
    ext = (
        filename_for_type.rsplit(".", 1)[-1].lower() if "." in filename_for_type else ""
    )

    try:
        if ext == "mp3":
            return _tag_mp3(audio_bytes, new_title, new_artist)
        if ext == "flac":
            return _tag_flac(audio_bytes, new_title, new_artist)
        return _tag_generic(filename_for_type, audio_bytes, new_title, new_artist)
    except Exception:
        return audio_bytes


def _id3_first_text(id3: ID3, frame_id: str) -> str:
    frames = id3.getall(frame_id)
    if not frames:
        return ""
    try:
        txt = frames[0].text
        if isinstance(txt, list):
            return _as_str(txt[0]) if txt else ""
        return _as_str(txt)
    except Exception:
        return ""


def _id3_first_comment(id3: ID3) -> str:
    comms = id3.getall("COMM")
    if not comms:
        return ""
    for c in comms:
        try:
            if getattr(c, "lang", "") == "eng":
                return _as_str(c.text[0] if c.text else "")
        except Exception:
            continue
    try:
        c = comms[0]
        return _as_str(c.text[0] if c.text else "")
    except Exception:
        return ""


def _tag_mp3(audio_bytes: bytes, title: str, artist: str) -> bytes:
    bio = io.BytesIO(audio_bytes)
    try:
        id3 = ID3(bio)
    except Exception:
        id3 = ID3()

    existing_title = _id3_first_text(id3, "TIT2")
    existing_artist = _id3_first_text(id3, "TPE1")
    existing_album = _id3_first_text(id3, "TALB")
    existing_comment = _id3_first_comment(id3)

    prev_concat = " | ".join(
        [
            v
            for v in [existing_title, existing_artist, existing_album, existing_comment]
            if v
        ]
    )

    id3.delall("TIT2")
    id3.delall("TPE1")
    id3.delall("COMM")

    id3.add(TIT2(encoding=3, text=title))
    id3.add(TPE1(encoding=3, text=artist))
    if prev_concat:
        id3.add(COMM(encoding=3, lang="eng", desc="Comment", text=prev_concat))

    bio.seek(0)
    id3.save(bio, v2_version=3)
    return bio.getvalue()


def _tag_flac(audio_bytes: bytes, title: str, artist: str) -> bytes:
    bio = io.BytesIO(audio_bytes)
    flac = FLAC(bio)

    existing_title = _as_str(flac.get("TITLE", [""])[0])
    existing_artist = _as_str(flac.get("ARTIST", [""])[0])
    existing_album = _as_str(flac.get("ALBUM", [""])[0])
    existing_comment = _as_str(flac.get("COMMENT", [""])[0])

    prev_concat = " | ".join(
        [
            v
            for v in [existing_title, existing_artist, existing_album, existing_comment]
            if v
        ]
    )

    flac["TITLE"] = [title]
    flac["ARTIST"] = [artist]
    if prev_concat:
        flac["COMMENT"] = [prev_concat]

    flac.save(bio)
    return bio.getvalue()


def _generic_first(tags: Any, keys: list[str]) -> str:
    for k in keys:
        try:
            v = tags.get(k)
            if not v:
                continue
            if isinstance(v, list):
                return _as_str(v[0]) if v else ""
            return _as_str(v)
        except Exception:
            continue
    return ""


def _generic_set(tags: Any, keys: list[str], value: str) -> None:
    for k in keys:
        try:
            tags[k] = [value]
            return
        except Exception:
            continue


def _tag_generic(filename: str, audio_bytes: bytes, title: str, artist: str) -> bytes:
    bio = io.BytesIO(audio_bytes)
    audio = MutagenFile(bio, filename=filename)
    if audio is None:
        return audio_bytes
    if audio.tags is None:
        try:
            audio.add_tags()
        except Exception:
            return audio_bytes

    tags = audio.tags

    # Try common keys; MP4 uses ©nam, ©ART, ©alb, ©cmt
    existing_title = _generic_first(tags, ["TITLE", "©nam"])
    existing_artist = _generic_first(tags, ["ARTIST", "©ART"])
    existing_album = _generic_first(tags, ["ALBUM", "©alb"])
    existing_comment = _generic_first(tags, ["COMMENT", "©cmt"])

    prev_concat = " | ".join(
        [
            v
            for v in [existing_title, existing_artist, existing_album, existing_comment]
            if v
        ]
    )

    _generic_set(tags, ["TITLE", "©nam"], title)
    _generic_set(tags, ["ARTIST", "©ART"], artist)
    if prev_concat:
        _generic_set(tags, ["COMMENT", "©cmt"], prev_concat)

    audio.save(bio)
    return bio.getvalue()

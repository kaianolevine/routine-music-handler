from routine_music_handler.file_translations import (
    _parse_season_year,
    build_base_filename,
    sanitize_part,
)
from routine_music_handler.sheet_state import Submission


def test_season_year_november_rolls_forward():
    assert _parse_season_year("11/19/2025 23:16:40") == "2026"


def test_season_year_may_stays_same():
    assert _parse_season_year("5/19/2025 23:16:40") == "2025"


def test_base_filename_includes_year_and_optional_fields():
    sub = Submission(
        timestamp="5/19/2025 23:16:40",
        leader_first=" Kaiano ",
        leader_last=" Levine ",
        follower_first=" Libby ",
        follower_last=" Wooton ",
        division=" Novice Jack & Jill ",
        routine_name="",
        personal_descriptor="Sparkly Shoes",
        audio_url="https://drive.google.com/file/d/1234567890123456789012345/view",
    )
    base, year = build_base_filename(
        timestamp=sub.timestamp,
        leader=sanitize_part(sub.leader_first) + sanitize_part(sub.leader_last),
        follower=sanitize_part(sub.follower_first) + sanitize_part(sub.follower_last),
        division=sanitize_part(sub.division),
        routine=sanitize_part(sub.routine_name),
        descriptor=sanitize_part(sub.personal_descriptor),
    )
    assert year == "2025"
    assert base.endswith("_2025_SparklyShoes")

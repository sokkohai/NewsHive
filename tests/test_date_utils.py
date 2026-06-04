from datetime import datetime, timezone

from src.date_utils import parse_date_to_utc


def test_parse_english_date_to_utc():
    dt = parse_date_to_utc("22 January 2026")

    assert dt is not None
    assert dt == datetime(2026, 1, 22, tzinfo=timezone.utc)


def test_parse_german_date_to_utc():
    dt = parse_date_to_utc("22. Januar 2026")

    assert dt is not None
    assert dt == datetime(2026, 1, 22, tzinfo=timezone.utc)


def test_parse_iso_date_to_utc():
    dt = parse_date_to_utc("2026-01-22T10:30:45Z")

    assert dt is not None
    assert dt == datetime(2026, 1, 22, 10, 30, 45, tzinfo=timezone.utc)


def test_parse_empty_date_returns_none():
    assert parse_date_to_utc("   ") is None

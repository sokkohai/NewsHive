"""Shared date parsing helpers for NewsHive."""

from __future__ import annotations

import re
from datetime import datetime, timezone

from dateutil import parser as dateutil_parser

GERMAN_MONTHS_TO_ENGLISH = {
    "januar": "january",
    "februar": "february",
    "maerz": "march",
    "märz": "march",
    "april": "april",
    "mai": "may",
    "juni": "june",
    "juli": "july",
    "august": "august",
    "september": "september",
    "oktober": "october",
    "november": "november",
    "dezember": "december",
    "maer": "march",
    "mär": "march",
    "dez": "december",
    "okt": "october",
    "jan": "january",
    "feb": "february",
    "apr": "april",
    "jun": "june",
    "jul": "july",
    "aug": "august",
    "sep": "september",
    "nov": "november",
}

MONTH_NAMES = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

_MONTH_TOKEN = (
    r"(?:january|february|march|april|may|june|july|august|"
    r"september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)"
)

DAY_MONTH_YEAR_PATTERN = re.compile(rf"(\d{{1,2}})\s+({_MONTH_TOKEN})\s+(\d{{4}})", re.IGNORECASE)
MONTH_DAY_YEAR_PATTERN = re.compile(
    rf"({_MONTH_TOKEN})\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s+(\d{{4}})",
    re.IGNORECASE,
)
DD_MM_YYYY_PATTERN = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
ISO_DATE_PATTERN = re.compile(r"(\d{4})[-/](\d{2})[-/](\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2}))?")


def _replace_german_months(text: str) -> str:
    normalized = text
    for german_month, english_month in GERMAN_MONTHS_TO_ENGLISH.items():
        normalized = re.sub(
            rf"\b{re.escape(german_month)}\b",
            english_month,
            normalized,
            flags=re.IGNORECASE,
        )
    return normalized


def _build_utc_datetime(
    year: str,
    month: str,
    day: str,
    hour: str | None = None,
    minute: str | None = None,
    second: str | None = None,
) -> datetime | None:
    try:
        return datetime(
            int(year),
            int(month),
            int(day),
            int(hour) if hour else 0,
            int(minute) if minute else 0,
            int(second) if second else 0,
            tzinfo=timezone.utc,
        )
    except ValueError:
        return None


def parse_date_to_utc(date_str: str | None, *, fallback_dayfirst: bool = True) -> datetime | None:
    """Parse a date string into a timezone-aware UTC datetime."""
    if not date_str:
        return None

    stripped = date_str.strip()
    if not stripped:
        return None

    normalized = _replace_german_months(stripped.lower())

    match = DAY_MONTH_YEAR_PATTERN.search(normalized)
    if match:
        day = match.group(1)
        month_str = match.group(2).lower()
        year = match.group(3)
        month = MONTH_NAMES.get(month_str, 0)
        if 1 <= month <= 12 and 1 <= int(day) <= 31:
            dt = _build_utc_datetime(year, str(month), day)
            if dt:
                return dt

    match = MONTH_DAY_YEAR_PATTERN.search(normalized)
    if match:
        month_str = match.group(1).lower()
        day = match.group(2)
        year = match.group(3)
        month = MONTH_NAMES.get(month_str, 0)
        if 1 <= month <= 12 and 1 <= int(day) <= 31:
            dt = _build_utc_datetime(year, str(month), day)
            if dt:
                return dt

    match = DD_MM_YYYY_PATTERN.search(stripped)
    if match:
        day = match.group(1)
        month_text = match.group(2)
        year = match.group(3)
        if 1 <= int(month_text) <= 12 and 1 <= int(day) <= 31:
            dt = _build_utc_datetime(year, month_text, day)
            if dt:
                return dt

    match = ISO_DATE_PATTERN.search(stripped)
    if match:
        year = match.group(1)
        month_text = match.group(2)
        day = match.group(3)
        hour = match.group(4)
        minute = match.group(5)
        second = match.group(6)
        if 1 <= int(month_text) <= 12 and 1 <= int(day) <= 31:
            dt = _build_utc_datetime(year, month_text, day, hour, minute, second)
            if dt:
                return dt

    try:
        dt = dateutil_parser.parse(normalized, dayfirst=fallback_dayfirst, fuzzy=True)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

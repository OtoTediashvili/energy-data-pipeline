"""The request window must reach tomorrow's market day in every season."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from pipeline.entsoe import day_ahead_params

BRUSSELS = ZoneInfo("Europe/Brussels")
NL = "10YNL----------L"


def _market_day_utc(day: date) -> tuple[datetime, datetime]:
    """A market day is midnight to midnight in Brussels, expressed in UTC."""
    start = datetime(day.year, day.month, day.day, tzinfo=BRUSSELS)
    following = day + timedelta(days=1)
    end = datetime(following.year, following.month, following.day, tzinfo=BRUSSELS)
    return start.astimezone(UTC), end.astimezone(UTC)


@pytest.mark.parametrize(
    "day",
    [
        date(2026, 7, 15),  # summer, CEST
        date(2026, 1, 14),  # winter, CET: where a 23:00 end misses tomorrow
        date(2026, 10, 25),  # clocks go back: a 25-hour market day
        date(2026, 3, 29),  # clocks go forward: a 23-hour market day
    ],
)
def test_window_covers_today_and_tomorrows_market_days_only(day: date) -> None:
    params = day_ahead_params(NL, day)
    start = datetime.strptime(params["periodStart"], "%Y%m%d%H%M").replace(tzinfo=UTC)
    end = datetime.strptime(params["periodEnd"], "%Y%m%d%H%M").replace(tzinfo=UTC)

    def overlaps(market_day: date) -> bool:
        md_start, md_end = _market_day_utc(market_day)
        return md_start < end and md_end > start

    assert overlaps(day), "today's market day"
    assert overlaps(day + timedelta(days=1)), "tomorrow's market day, just published"
    assert not overlaps(day + timedelta(days=2))
    assert not overlaps(day - timedelta(days=1))


def test_zone_goes_in_both_domains() -> None:
    params = day_ahead_params(NL, date(2026, 9, 24))
    assert params["in_Domain"] == params["out_Domain"] == NL
    assert params["documentType"] == "A44"

"""ENTSO-E request parameters for one run's day."""

from __future__ import annotations

from datetime import date, timedelta

DAY_AHEAD_PRICES = "A44"


def day_ahead_params(eic: str, day: date) -> dict[str, str]:
    """Day-ahead price request for one bidding zone, for one run's UTC day.

    Market days run on Brussels time, so a UTC day overlaps two of them: today's
    and tomorrow's, which the auction has just published. The window runs from
    this UTC midnight to the next. Ending it at 23:00 instead still catches
    tomorrow's market day in summer, when it starts at 22:00 UTC, but misses it
    in winter, when it starts at 23:00 UTC: for half the year, every run would
    deliver tomorrow's prices a day late.
    """
    following = day + timedelta(days=1)
    return {
        "documentType": DAY_AHEAD_PRICES,
        "in_Domain": eic,
        "out_Domain": eic,
        "periodStart": f"{day:%Y%m%d}0000",
        "periodEnd": f"{following:%Y%m%d}0000",
    }

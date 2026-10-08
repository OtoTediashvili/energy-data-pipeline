"""Bidding-zone keys, and the partition layout every zone-aware file follows.

A zone key is short and path-safe (NL, BE, AT). It ends up inside file paths,
so it is validated: a key like "../x" or "NL/BE" would write outside its own
partition, or into another zone's.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

_ZONE_KEY = re.compile(r"^[A-Z0-9_]{1,16}$")


def validate_zone_key(key: str) -> str:
    """Return the key unchanged, or raise if it is not path-safe."""
    if not _ZONE_KEY.match(key):
        raise ValueError(f"invalid zone key {key!r}: expected 1-16 characters of A-Z, 0-9, _")
    return key


def partition_path(
    root: Path, dataset: str, logical_date: date, suffix: str, zone: str | None = None
) -> Path:
    """root/dataset=<dataset>[/zone=<zone>]/year=<Y>/month=<M>/<file>

    Deterministic, so a rerun of the same dataset, zone and date overwrites
    one file instead of adding another. Without a zone, the layout is the
    original single-zone one.
    """
    base = root / f"dataset={dataset}"
    stem = dataset
    if zone is not None:
        validate_zone_key(zone)
        base = base / f"zone={zone}"
        stem = f"{dataset}_{zone}"
    return (
        base
        / f"year={logical_date.year:04d}"
        / f"month={logical_date.month:02d}"
        / f"{stem}_{logical_date.isoformat()}{suffix}"
    )

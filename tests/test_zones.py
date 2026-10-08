"""Zone keys end up inside file paths, so they are validated."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from pipeline.zones import partition_path, validate_zone_key

DAY = date(2026, 9, 24)


@pytest.mark.parametrize("key", ["NL", "BE", "DE_LU", "SE3"])
def test_valid_zone_keys(key: str) -> None:
    assert validate_zone_key(key) == key


@pytest.mark.parametrize("key", ["", "nl", "../x", "NL/BE", "NL BE", "A" * 17])
def test_unsafe_zone_keys_are_rejected(key: str) -> None:
    with pytest.raises(ValueError, match="invalid zone key"):
        validate_zone_key(key)


def test_a_zone_adds_a_partition_and_names_the_file(tmp_path: Path) -> None:
    path = partition_path(tmp_path, "prices", DAY, ".xml", zone="NL")
    expected = "dataset=prices/zone=NL/year=2026/month=09/prices_NL_2026-09-24.xml"
    assert path.relative_to(tmp_path).as_posix() == expected


def test_without_a_zone_the_layout_is_unchanged(tmp_path: Path) -> None:
    path = partition_path(tmp_path, "prices", DAY, ".xml")
    assert (
        path.relative_to(tmp_path).as_posix()
        == "dataset=prices/year=2026/month=09/prices_2026-09-24.xml"
    )


def test_zones_never_share_a_file(tmp_path: Path) -> None:
    nl = partition_path(tmp_path, "prices", DAY, ".xml", zone="NL")
    assert nl != partition_path(tmp_path, "prices", DAY, ".xml", zone="BE")

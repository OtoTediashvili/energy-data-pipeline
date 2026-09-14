"""Extraction: pull from the source API, land raw bytes unmodified.

Two rules that make backfills safe:

1. The landing path is a pure function of (dataset, logical_date). Rerunning
   the same interval overwrites the same file instead of appending a duplicate.
2. Nothing is parsed or reshaped here. Whatever the source returned is what
   hits disk, so a downstream bug never costs you a re-fetch.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from pipeline.config import Settings, get_settings
from pipeline.logging_config import get_logger

log = get_logger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class TransientSourceError(RuntimeError):
    """Source failed in a way that is worth retrying."""


class PermanentSourceError(RuntimeError):
    """Source failed in a way that will not improve on retry."""


def landing_path(dataset: str, logical_date: date, settings: Settings | None = None) -> Path:
    """Return the deterministic landing location for one dataset partition.

    Hive-style partitioning so that DuckDB, Spark and Athena can all read the
    tree without an external catalogue.
    """
    cfg = settings or get_settings()
    return (
        cfg.landing_dir
        / f"dataset={dataset}"
        / f"year={logical_date.year:04d}"
        / f"month={logical_date.month:02d}"
        / f"{dataset}_{logical_date.isoformat()}.json"
    )


def _raise_for_status(response: httpx.Response) -> None:
    if response.status_code in RETRYABLE_STATUS:
        raise TransientSourceError(f"retryable status {response.status_code}")
    if response.status_code >= 400:
        raise PermanentSourceError(f"status {response.status_code}: {response.text[:200]}")


@retry(
    retry=retry_if_exception_type((TransientSourceError, httpx.TransportError)),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch(
    endpoint: str,
    params: dict[str, str] | None = None,
    settings: Settings | None = None,
) -> bytes:
    """GET one endpoint and return the raw response body.

    Retries on 429 and 5xx with exponential backoff. Fails immediately on 4xx,
    because a malformed request will not fix itself.
    """
    cfg = settings or get_settings()
    url = f"{cfg.source_base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    headers: dict[str, str] = {}
    if cfg.source_api_key.get_secret_value():
        headers["Authorization"] = f"Bearer {cfg.source_api_key.get_secret_value()}"

    log.info("fetch.start", url=url, params=params)
    with httpx.Client(timeout=cfg.request_timeout_seconds) as client:
        response = client.get(url, params=params, headers=headers)

    _raise_for_status(response)
    log.info("fetch.ok", url=url, bytes=len(response.content))
    return response.content


def land(
    payload: bytes,
    dataset: str,
    logical_date: date,
    settings: Settings | None = None,
) -> Path:
    """Write a payload to its deterministic landing path and return it."""
    cfg = settings or get_settings()
    target = landing_path(dataset, logical_date, cfg)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Write to a temp file then rename: a crash mid-write never leaves a
    # half-written partition that downstream tasks would happily read.
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_bytes(payload)
    tmp.replace(target)

    log.info("land.ok", path=str(target), bytes=len(payload))
    return target


def extract_to_landing(
    endpoint: str,
    dataset: str,
    logical_date: date,
    params: dict[str, str] | None = None,
    settings: Settings | None = None,
) -> Path:
    """Fetch one interval and land it. Safe to rerun for the same date."""
    payload = fetch(endpoint, params=params, settings=settings)
    return land(payload, dataset, logical_date, settings=settings)

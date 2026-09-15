"""Watch the pipeline work, end to end, with no API key and no Docker.

Spins up a throwaway HTTP server on localhost serving fake price data, then
runs the real extract and load code against it. Nothing is mocked: fetch()
makes genuine HTTP calls, tenacity genuinely retries, DuckDB genuinely stores.

    python scripts/demo.py

Then run it a second time and watch the row counts stay put. That is the
idempotency guarantee, which is the whole point of the design.
"""

from __future__ import annotations

import json
import os
import random
import threading
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import ClassVar
from urllib.parse import parse_qs, urlparse

from pydantic import SecretStr

from pipeline.config import Settings
from pipeline.extract import extract_to_landing
from pipeline.load import load_json_partition, row_count, warehouse
from pipeline.logging_config import configure_logging

COUNTRIES = ["GE", "DE", "FR", "IT", "ES"]
DAYS = 5
FLAKY_RATE = 0.35  # fraction of requests that fail, to exercise the retry path

BOLD, GREEN, YELLOW, GREY, RESET = "\033[1m", "\033[32m", "\033[33m", "\033[90m", "\033[0m"


def step(number: int, text: str) -> None:
    print(f"\n{BOLD}[{number}] {text}{RESET}")


class FakeSourceHandler(BaseHTTPRequestHandler):
    """Pretends to be a flaky upstream API."""

    attempts: ClassVar[dict[str, int]] = {}

    def do_GET(self) -> None:
        query = parse_qs(urlparse(self.path).query)
        day = query.get("date", ["1970-01-01"])[0]

        seen = self.attempts.get(day, 0)
        self.attempts[day] = seen + 1

        # Fail the first attempt for some days so the retry logic is visible.
        if seen == 0 and random.random() < FLAKY_RATE:
            print(f"    {YELLOW}source returned 503 for {day} (retry incoming){RESET}")
            self.send_response(503)
            self.end_headers()
            return

        rng = random.Random(day)
        rows = [
            {
                "id": int(f"{day.replace('-', '')}{i}"),
                "country": country,
                "value": round(rng.uniform(20, 120), 2),
            }
            for i, country in enumerate(COUNTRIES)
        ]
        body = "\n".join(json.dumps(r) for r in rows).encode()

        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        """Silence the default per-request logging."""


def start_fake_source() -> tuple[HTTPServer, str]:
    server = HTTPServer(("127.0.0.1", 0), FakeSourceHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{port}"


def show_tree(root: Path) -> None:
    files = sorted(root.rglob("*.json"))
    if not files:
        print(f"    {GREY}(empty){RESET}")
        return
    for file in files:
        size = file.stat().st_size
        print(f"    {file.relative_to(root)}  {GREY}{size} bytes{RESET}")


def main() -> None:
    # Quiet the pipeline's own INFO logging so the demo narration reads cleanly.
    # Settings alone does not do this: structlog needs configure_logging().
    os.environ["PIPELINE_LOG_LEVEL"] = "WARNING"
    configure_logging()

    server, base_url = start_fake_source()
    print(f"{BOLD}Pipeline demo{RESET}  {GREY}fake source at {base_url}{RESET}")

    settings = Settings(
        env="demo",
        source_base_url=base_url,
        source_api_key=SecretStr("demo-key"),
        data_dir=Path("data"),
        duckdb_path=Path("data/warehouse/warehouse.duckdb"),
    )
    settings.ensure_dirs()

    days = [date(2026, 9, 10) + timedelta(days=offset) for offset in range(DAYS)]

    step(1, f"EXTRACT — fetching {DAYS} days from the API")
    landed: list[tuple[date, Path]] = []
    for day in days:
        path = extract_to_landing(
            endpoint="prices",
            dataset="prices",
            logical_date=day,
            params={"date": day.isoformat()},
            settings=settings,
        )
        landed.append((day, path))
        print(f"    {GREEN}ok{RESET} {day}  ->  {path.name}")

    step(2, "LANDING ZONE — raw bytes on disk, partitioned by date")
    show_tree(settings.landing_dir)

    step(3, "LOAD — into DuckDB, one partition at a time")
    with warehouse(settings) as conn:
        for day, path in landed:
            rows = load_json_partition(conn, path, "prices", day)
            print(f"    {GREEN}ok{RESET} {day}  ->  {rows} rows")
        total = row_count(conn, "prices")

    print(f"\n    total rows in raw.prices: {BOLD}{total}{RESET}")

    step(4, "QUERY — it is a real warehouse now")
    with warehouse(settings) as conn:
        result = conn.execute("""
            SELECT country,
                   count(*)                AS days,
                   round(avg(value), 2)    AS avg_price,
                   round(max(value), 2)    AS peak_price
            FROM raw.prices
            GROUP BY country
            ORDER BY avg_price DESC
        """).fetchall()

    print(f"    {'country':<10}{'days':>6}{'avg':>10}{'peak':>10}")
    print(f"    {GREY}{'-' * 36}{RESET}")
    for country, day_count, avg_price, peak in result:
        print(f"    {country:<10}{day_count:>6}{avg_price:>10}{peak:>10}")

    step(5, "LINEAGE — every row knows where it came from")
    with warehouse(settings) as conn:
        sample = conn.execute("""
            SELECT id, country, value, _logical_date, _source_file
            FROM raw.prices ORDER BY id LIMIT 3
        """).fetchall()
    for row in sample:
        source = Path(str(row[4])).name
        print(f"    id={row[0]}  {row[1]}  {row[2]:>7}  {row[3]}  {GREY}{source}{RESET}")

    server.shutdown()

    print(f"\n{BOLD}Done.{RESET}")
    print(f"  {GREY}Run it again — row counts stay at {total}, not {total * 2}.{RESET}")
    print(f"  {GREY}That is idempotency: reruns replace, they do not duplicate.{RESET}")
    print("\nNext, build the star schema on top:")
    print(f"  {BOLD}cd dbt && dbt deps && dbt build --target dev{RESET}")


if __name__ == "__main__":
    main()

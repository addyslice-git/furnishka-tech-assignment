#!/usr/bin/env python3
"""CLI tool to fetch and normalize Meta Lead Ads data.

This script retrieves leads from the Meta Graph API for a given lead form,
normalizes the nested JSON into a flat schema, deduplicates using a local
SQLite database, and writes new leads to `new_leads.json` or `new_leads.csv`.

New: use `--view-db` to print the contents of the configured SQLite DB to the terminal.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import requests

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:
    # Simple fallback loader if python-dotenv is unavailable
    def load_dotenv(dotenv_path: str | None = None) -> None:
        path = dotenv_path or ".env"
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                key, value = stripped.split("=", 1)
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)

from utils.db import LeadDB


def parse_iso_timestamp(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp string into a naive datetime.

    Raises `ValueError` if the format is invalid.
    """
    try:
        if ts.endswith("Z"):
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        if len(ts) >= 5 and ts[-5] in ["+", "-"]:
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
        return datetime.fromisoformat(ts)
    except Exception as exc:
        raise ValueError(f"Invalid ISO timestamp: {ts}") from exc


def fetch_leads(
    access_token: str,
    form_id: str,
    api_version: str,
    limit: int = 25,
    since: Optional[str] = None,
    max_retries: int = 3,
) -> Iterator[List[Dict[str, Any]]]:
    """Fetch pages of raw leads from the Graph API.

    Yields a list of lead objects for each page. Retries transient 5xx/429 errors.
    """
    base_url = f"https://graph.facebook.com/v{api_version}/{form_id}/leads"
    params: Dict[str, Any] = {
        "access_token": access_token,
        "fields": "id,created_time,field_data",
        "limit": limit,
    }
    if since:
        params["since"] = since
    url: Optional[str] = base_url

    session = requests.Session()
    while url:
        attempt = 0
        while True:
            try:
                resp = session.get(url, params=params if url == base_url else None, timeout=30)
                # Treat 5xx as transient and handle 429 (rate-limit) specially
                if resp.status_code >= 500:
                    raise requests.exceptions.HTTPError(f"Server error {resp.status_code}")
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = int(retry_after)
                        except Exception:
                            delay = 2 ** attempt
                    else:
                        delay = 2 ** attempt
                    logging.warning("Rate limited (429). Sleeping %s seconds before retrying.", delay)
                    time.sleep(delay)
                    attempt += 1
                    if attempt > max_retries:
                        logging.error("Exceeded max retries after 429 responses")
                        resp.raise_for_status()
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    delay = 2 ** attempt
                    logging.warning("API request failed (%s). Retrying in %s seconds...", e, delay)
                    time.sleep(delay)
                    attempt += 1
                else:
                    logging.error("API request failed after %d retries: %s", max_retries, e)
                    raise
        leads = data.get("data", [])
        yield leads
        paging = data.get("paging", {})
        url = paging.get("next")
        params = None  # Use the `next` URL as-is on subsequent requests


def normalize_lead(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a raw lead into a flat dict.

    Returns None if both email and phone are missing.
    """
    lead_id: str = raw.get("id")
    created_time: str = raw.get("created_time")
    fields = {
        item.get("name"): (item.get("values") or [None])[0]
        for item in raw.get("field_data", [])
        if item.get("name")
    }
    # Name derivation
    name: Optional[str] = fields.get("full_name")
    if not name:
        first = fields.get("first_name") or ""
        last = fields.get("last_name") or ""
        combined = f"{first} {last}".strip()
        name = combined if combined else None
    email: Optional[str] = fields.get("email")
    phone: Optional[str] = (
        fields.get("phone")
        or fields.get("phone_number")
        or fields.get("phone_number_ext")
    )
    if not email and not phone:
        return None
    return {
        "lead_id": lead_id,
        "name": name,
        "email": email,
        "phone": phone,
        "created_time": created_time,
    }


def run_offline_sample(sample_path: str) -> Iterator[List[Dict[str, Any]]]:
    """Yield a single page of leads from a local JSON sample file."""
    if not os.path.isabs(sample_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.dirname(script_dir)
        sample_path = os.path.join(repo_root, sample_path)
    with open(sample_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    yield data.get("data", [])


def write_output(leads: List[Dict[str, Any]], output_format: str) -> None:
    """Write the list of normalized leads to `new_leads.json` or `new_leads.csv`."""
    if not leads:
        logging.info("No new leads to write.")
        return
    if output_format == "json":
        with open("new_leads.json", "w", encoding="utf-8") as f:
            json.dump(leads, f, indent=2, ensure_ascii=False)
        logging.info("Wrote %d new leads to new_leads.json", len(leads))
    else:
        fieldnames = ["lead_id", "name", "email", "phone", "created_time"]
        with open("new_leads.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(leads)
        logging.info("Wrote %d new leads to new_leads.csv", len(leads))


def view_db(db_path: str, max_rows_per_table: int = 200) -> None:
    """Print tables and rows from the SQLite DB to the terminal (read-only)."""
    if not os.path.exists(db_path):
        print(f"DB file does not exist: {db_path}")
        return
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cur.fetchall()]
        if not tables:
            print(f"No tables found in {db_path}")
            return
        for table in tables:
            print("\n" + "=" * 60)
            print(f"Table: {table}")
            print("=" * 60)
            cur.execute(f"PRAGMA table_info('{table}')")
            cols = [r[1] for r in cur.fetchall()]
            cur.execute(f"SELECT * FROM '{table}' LIMIT {max_rows_per_table}")
            rows = cur.fetchall()
            if not rows:
                print("(no rows)")
                continue
            # Print header
            print(", ".join(cols))
            # Print rows
            for r in rows:
                # decode bytes if necessary
                printable = []
                for x in r:
                    if isinstance(x, (bytes, bytearray)):
                        try:
                            printable.append(x.decode("utf-8"))
                        except Exception:
                            printable.append(repr(x))
                    else:
                        printable.append(str(x))
                print(", ".join(printable))
        print("\n-- End of DB preview --")
    except Exception as exc:
        print(f"Error reading DB {db_path}: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Meta leads from a Lead Ads form")
    parser.add_argument(
        "--since",
        help="Fetch leads created since the given ISO timestamp (e.g. 2025-07-01T00:00:00+0000)",
    )
    parser.add_argument(
        "--output",
        choices=["json", "csv"],
        default="json",
        help="Output format for new leads (default: json)",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use the offline sample file instead of calling the API",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Number of leads to request per page from the API",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (e.g. DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retries on transient API errors",
    )
    parser.add_argument(
        "--view-db",
        action="store_true",
        help="Print the contents of the configured SQLite DB to the terminal and exit",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    # Configure logging
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {args.log_level}")
    logging.basicConfig(level=numeric_level, format="%(asctime)s %(levelname)s: %(message)s")

    # Read environment variables
    access_token = os.getenv("META_ACCESS_TOKEN")
    form_id = os.getenv("LEAD_FORM_ID")
    api_version = os.getenv("API_VERSION", "16.0")
    db_path = os.getenv("DB_PATH", "data/seen_leads.db")
    offline_mode = args.offline or os.getenv("OFFLINE_MODE", "0") == "1"

    # If requested, view DB and exit early
    if args.view_db:
        view_db(db_path)
        return

    # Validate configuration in live mode
    if not offline_mode:
        missing = []
        if not access_token:
            missing.append("META_ACCESS_TOKEN")
        if not form_id:
            missing.append("LEAD_FORM_ID")
        if missing:
            logging.error(
                "Missing required environment variables: %s. Set them in .env or use --offline.",
                ", ".join(missing),
            )
            sys.exit(1)

    # Ensure DB directory exists
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    # Build SQLite helper
    db = LeadDB(db_path)

    # Determine since parameter
    since_param: Optional[str] = None
    if args.since:
        # Validate timestamp format early
        _ = parse_iso_timestamp(args.since)  # raises if invalid
        since_param = args.since

    # Iterate through pages
    new_leads: List[Dict[str, Any]] = []
    if offline_mode:
        pages = run_offline_sample("data/meta_leads_sample.json")
    else:
        pages = fetch_leads(
            access_token,
            form_id,
            api_version,
            limit=args.limit,
            since=since_param,
            max_retries=args.max_retries,
        )

    for page in pages:
        for raw in page:
            normalized = normalize_lead(raw)
            if normalized is None:
                logging.debug("Skipping lead %s: missing email and phone", raw.get("id"))
                continue
            lead_id = normalized["lead_id"]
            email = normalized.get("email") or ""
            phone = normalized.get("phone") or ""  
            # Apply since filter if offline mode (since API param is not used)
            if since_param and offline_mode:
                created_dt = parse_iso_timestamp(normalized["created_time"])
                if created_dt < parse_iso_timestamp(since_param):
                    continue
            if db.is_seen(lead_id, email, phone):
                continue
            db.mark_seen(normalized)
            new_leads.append(normalized)

    # Write output
    write_output(new_leads, args.output)
    logging.info("Total new leads written: %d", len(new_leads))


if __name__ == "__main__":
    main()

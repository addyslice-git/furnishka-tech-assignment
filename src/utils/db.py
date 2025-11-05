"""
SQLite helper for tracking processed leads.

This implementation stores full lead details (lead_id, name, email, phone,
created_time) while remaining backward-compatible with older code that only
inserted lead_id strings.

APIs:
- LeadDB(db_path)          # construct
- is_seen(lead_id) -> bool
- mark_seen(lead_or_id)    # accepts either a lead dict or a lead_id str
- fetch_all(limit=None) -> list[dict]   # returns rows as dicts
- close()
"""

import os
import sqlite3
from typing import Any, Dict, List, Optional, Union


class LeadDB:
    def __init__(self, db_path: str) -> None:
        # Ensure parent directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        # Connect to the database (allow same-thread usage)
        # Use row_factory to return rows as sqlite3.Row (mapping behavior)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._create_table()

    def _create_table(self) -> None:
        """Create the table if it doesn't already exist (new schema)."""
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_leads (
                    lead_id TEXT PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    phone TEXT,
                    created_time TEXT
                )
                """
            )

    def is_seen(self, lead_id: str, email: str, phone :str) -> bool:
        """Return True if the given lead ID has already been recorded."""
        cur = self._conn.cursor()
        try:
            # Check for existence of duplicate by lead_id, email or phone
            cur.execute("SELECT 1 FROM seen_leads WHERE lead_id=? OR email=? OR phone=?",(lead_id, email, phone))  
            return cur.fetchone() is not None
        finally:
            cur.close()

    def mark_seen(self, lead_or_id: Union[str, Dict[str, Any]]) -> None:
        """Record a lead as processed.

        Accepts either:
        - a lead_id string (backwards compatible), or
        - a lead dict with keys: lead_id, name, email, phone, created_time.

        Duplicate inserts are ignored.
        """
        # Normalize into fields
        if isinstance(lead_or_id, str):
            lead_id = lead_or_id
            name = email = phone = created_time = None
        elif isinstance(lead_or_id, dict):
            lead_id = lead_or_id.get("lead_id")
            name = lead_or_id.get("name")
            email = lead_or_id.get("email")
            phone = lead_or_id.get("phone")
            created_time = lead_or_id.get("created_time")
        else:
            raise TypeError("mark_seen expects a lead_id string or a lead dict")

        if not lead_id:
            # Nothing to do for invalid input
            return

        with self._conn:
            # Insert or ignore to preserve idempotency
            self._conn.execute(
                """
                INSERT OR IGNORE INTO seen_leads (lead_id, name, email, phone, created_time)
                VALUES (?, ?, ?, ?, ?)
                """,
                (lead_id, name, email, phone, created_time),
            )

    def fetch_all(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return all stored leads as a list of dictionaries.

        `limit` can be provided to restrict rows for previewing.
        """
        cur = self._conn.cursor()
        try:
            sql = "SELECT lead_id, name, email, phone, created_time FROM seen_leads ORDER BY rowid"
            if limit:
                sql = f"{sql} LIMIT {int(limit)}"
            cur.execute(sql)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            cur.close()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

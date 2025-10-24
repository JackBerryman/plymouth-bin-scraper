import sqlite3, json, time
from pathlib import Path

DB = Path("cache.sqlite")

def init_db():
    with sqlite3.connect(DB) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS results(
            key TEXT PRIMARY KEY,
            ts INTEGER NOT NULL,
            json TEXT NOT NULL
        )""")

def get_cache(key: str, ttl_seconds: int | None = None):
    with sqlite3.connect(DB) as con:
        cur = con.execute("SELECT ts, json FROM results WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row:
            return None
        ts, payload = row
        if ttl_seconds is not None and (time.time() - ts) > ttl_seconds:
            return None
        return json.loads(payload)

def set_cache(key: str, data: dict):
    with sqlite3.connect(DB) as con:
        con.execute("REPLACE INTO results(key, ts, json) VALUES (?, ?, ?)",
                    (key, int(time.time()), json.dumps(data)))
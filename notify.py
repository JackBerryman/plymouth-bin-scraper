#!/usr/bin/env python3
"""
notify.py — checks your Plymouth bin schedule (from GitHub Pages)
and sends a WhatsApp reminder via Twilio if tomorrow has a collection.
"""

import os
import json
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from base64 import b64encode

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    ZoneInfo = None

# ---- CONFIG ----
# If running locally, this can be blank (falls back to local JSON).
BIN_JSON_URL = os.getenv("BIN_JSON_URL", "").strip()
LOCAL_JSON_PATH = "public/PL6_5HX_72_Windermere.json"  # fallback path

# Twilio credentials (read from GitHub Secrets in Actions or .env locally)
TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID") or os.getenv("TWILIO_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH_TOKEN")
WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
WHATSAPP_TO = os.getenv("TWILIO_WHATSAPP_TO") or os.getenv("TO_NUMBER")

UK_TZ = ZoneInfo("Europe/London") if ZoneInfo else None


# ---- HELPERS ----

def now_uk_date():
    """Return today's date in UK timezone (handles BST/GMT)."""
    if UK_TZ:
        return datetime.now(UK_TZ).date()
    return datetime.utcnow().date()


def fetch_json():
    """Fetch JSON from GitHub Pages or fallback to local file."""
    if BIN_JSON_URL:
        print(f">>> Fetching live data from {BIN_JSON_URL}")
        req = Request(BIN_JSON_URL, headers={"User-Agent": "bins-notifier/1.0"})
        try:
            with urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except HTTPError as e:
            raise SystemExit(f"HTTP error fetching {BIN_JSON_URL}: {e.code} {e.reason}")
        except URLError as e:
            raise SystemExit(f"Network error fetching {BIN_JSON_URL}: {e.reason}")
    print(f">>> Using local data from {LOCAL_JSON_PATH}")
    with open(LOCAL_JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def send_whatsapp(body: str):
    """Send WhatsApp message using Twilio API."""
    if not (TWILIO_SID and TWILIO_AUTH and WHATSAPP_TO):
        raise SystemExit(
            "Missing Twilio env vars: need TWILIO_ACCOUNT_SID (or TWILIO_SID), "
            "TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_TO (or TO_NUMBER)"
        )

    api = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
    payload = (
        f"From={WHATSAPP_FROM}&To={WHATSAPP_TO}&Body={body}".encode("utf-8")
    )
    auth = b64encode(f"{TWILIO_SID}:{TWILIO_AUTH}".encode()).decode()
    req = Request(api, data=payload, method="POST", headers={
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/x-www-form-urlencoded"
    })

    try:
        with urlopen(req, timeout=20) as resp:
            print(">>> Twilio:", resp.status, resp.read().decode("utf-8"))
    except HTTPError as e:
        err = e.read().decode("utf-8", "ignore")
        raise SystemExit(f"Twilio HTTP {e.code}: {e.reason}\n{err}")
    except URLError as e:
        raise SystemExit(f"Twilio network error: {e.reason}")


def main():
    """Main logic: load data, find tomorrow’s bins, send message if needed."""
    data = fetch_json()
    postcode = data.get("postcode", "")
    hint = data.get("address_hint", "")
    collections = data.get("collections", {})

    today = now_uk_date()
    tomorrow = today + timedelta(days=1)

    # Gather tomorrow’s bins
    services = []
    for key, arr in collections.items():
        for s in (arr or []):
            try:
                d = datetime.strptime(s, "%Y-%m-%d").date()
            except ValueError:
                continue
            if d == tomorrow:
                label = {
                    "refuse": "Refuse (brown bin)",
                    "recycling": "Recycling (green bin)",
                    "garden": "Garden waste"
                }.get(key, key.title())
                services.append(label)

    if not services:
        print(">>> No collections tomorrow — no WhatsApp sent.")
        return

    nice_date = tomorrow.strftime("%A %d %B")
    lines = [
        f"Bin reminder for {postcode} — {hint}",
        f"Tomorrow ({nice_date}):"
    ] + [f"• {s}" for s in services]

    body = "\n".join(lines)
    print(">>> Sending WhatsApp message:\n" + body)
    send_whatsapp(body)
    print(">>> Message sent ✓")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
notify.py — fetches your published bin schedule JSON (with retry) and
sends a WhatsApp reminder via Twilio if tomorrow has a collection.

Requires:
  - requests (and certifi on macOS if needed)
Env vars (Actions or .env):
  BIN_JSON_URL              -> Full URL to your JSON on GitHub Pages
  TWILIO_ACCOUNT_SID        -> Your Twilio Account SID
  TWILIO_AUTH_TOKEN         -> Your Twilio Auth Token
  TWILIO_WHATSAPP_FROM      -> "whatsapp:+14155238886" (Twilio sandbox default)
  TWILIO_WHATSAPP_TO        -> e.g. "whatsapp:+44xxxxxxxxxx"
Optional (local fallback):
  LOCAL_JSON_PATH           -> path to local JSON (default: public/PL6_5HX_72_Windermere.json)
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    ZoneInfo = None

# -------- Config --------
BIN_JSON_URL = os.getenv("BIN_JSON_URL", "").strip()
LOCAL_JSON_PATH = os.getenv("LOCAL_JSON_PATH", "public/PL6_5HX_72_Windermere.json")

# Twilio
TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID") or os.getenv("TWILIO_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH_TOKEN")
WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
WHATSAPP_TO = os.getenv("TWILIO_WHATSAPP_TO") or os.getenv("TO_NUMBER")

UK_TZ = ZoneInfo("Europe/London") if ZoneInfo else None

# -------- Helpers --------
def now_uk_date():
    if UK_TZ:
        return datetime.now(UK_TZ).date()
    return datetime.utcnow().date()

def fetch_json_with_retry(url: str, tries: int = 20, sleep_s: int = 10) -> dict:
    """
    Fetch URL with retry; useful right after GitHub Pages deploy where a brief 404 is normal.
    """
    last_err = None
    headers = {"User-Agent": "bins-notifier/1.0"}
    for i in range(1, tries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code == 200:
                return r.json()
            last_err = Exception(f"HTTP {r.status_code}")
        except Exception as e:
            last_err = e
        print(f">>> fetch attempt {i}/{tries} failed ({last_err}); retrying in {sleep_s}s…")
        time.sleep(sleep_s)
    raise SystemExit(f"Failed to fetch after {tries} attempts: {last_err}")

def load_schedule() -> dict:
    if BIN_JSON_URL:
        print(f">>> Fetching live data from {BIN_JSON_URL}")
        return fetch_json_with_retry(BIN_JSON_URL)
    print(f">>> Using local data from {LOCAL_JSON_PATH}")
    with open(LOCAL_JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def twilio_whatsapp(body: str):
    if not (TWILIO_SID and TWILIO_AUTH and WHATSAPP_TO):
        raise SystemExit(
            "Missing Twilio env vars: need TWILIO_ACCOUNT_SID (or TWILIO_SID), "
            "TWILIO_AUTH_TOKEN, and TWILIO_WHATSAPP_TO (or TO_NUMBER)."
        )
    api = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
    data = {
        "From": WHATSAPP_FROM,
        "To": WHATSAPP_TO,
        "Body": body,
    }
    try:
        r = requests.post(api, data=data, auth=(TWILIO_SID, TWILIO_AUTH), timeout=20)
        r.raise_for_status()
        print(">>> Twilio response:", r.status_code, r.text)
    except requests.HTTPError as e:
        print(">>> Twilio error body:", getattr(e.response, "text", ""))
        raise SystemExit(f"Twilio HTTP error: {e}")
    except Exception as e:
        raise SystemExit(f"Twilio network error: {e}")

# -------- Main --------
def main():
    data = load_schedule()
    postcode = data.get("postcode", "")
    hint = data.get("address_hint", "")
    collections = data.get("collections", {})

    today = now_uk_date()
    tomorrow = today + timedelta(days=1)

    # Find services due tomorrow
    label_map = {
        "refuse": "Refuse (brown bin)",
        "recycling": "Recycling (green bin)",
        "garden": "Garden waste",
    }
    due = []
    for key, arr in (collections or {}).items():
        for s in arr or []:
            try:
                if datetime.strptime(s, "%Y-%m-%d").date() == tomorrow:
                    due.append(label_map.get(key, key.title()))
            except ValueError:
                continue

    if not due:
        print(">>> No collections tomorrow — no WhatsApp sent.")
        return

    nice_date = tomorrow.strftime("%A %d %B")
    body = "Bin reminder for {pc} — {hint}\nTomorrow ({date}):\n{lines}".format(
        pc=postcode, hint=hint, date=nice_date, lines="\n".join(f"• {x}" for x in due)
    )

    print(">>> Sending WhatsApp message:\n" + body)
    twilio_whatsapp(body)
    print(">>> Message sent ✓")

if __name__ == "__main__":
    main()

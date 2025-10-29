#!/usr/bin/env python3
"""
notify.py — checks your Plymouth bin schedule (from GitHub Pages)
and sends a WhatsApp reminder via Twilio if tomorrow has a collection.

Set FORCE_SEND=true in the workflow inputs/env to send a test ping even if
there's no collection tomorrow.
"""

import os
import json
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode
from base64 import b64encode

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    ZoneInfo = None

# ---- CONFIG ----
BIN_JSON_URL = os.getenv("BIN_JSON_URL", "").strip()
LOCAL_JSON_PATH = "public/PL6_5HX_72_Windermere.json"  # fallback path

# Twilio credentials (from GitHub Secrets or local .env)
TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID") or os.getenv("TWILIO_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH_TOKEN")
WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
# Accept either TWILIO_WHATSAPP_TO or WHATSAPP_TO
WHATSAPP_TO = (
    os.getenv("TWILIO_WHATSAPP_TO")
    or os.getenv("WHATSAPP_TO")
    or os.getenv("TO_NUMBER")
)

FORCE_SEND = os.getenv("FORCE_SEND", "").lower() in {"1", "true", "yes"}

UK_TZ = ZoneInfo("Europe/London") if ZoneInfo else None


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


def normalize_whatsapp(num: str) -> str:
    """Ensure number has whatsapp:+ prefix."""
    n = (num or "").strip()
    if not n:
        return n
    if not n.startswith("whatsapp:"):
        n = "whatsapp:" + n
    if not n.startswith("whatsapp:+"):
        # handle 'whatsapp:447...' -> 'whatsapp:+447...'
        if n.startswith("whatsapp:") and not n[len("whatsapp:"):].startswith("+"):
            n = "whatsapp:+" + n[len("whatsapp:"):]
    return n


def send_whatsapp(body: str):
    """Send WhatsApp message using Twilio API (properly URL-encoded)."""
    to = normalize_whatsapp(WHATSAPP_TO)
    from_ = normalize_whatsapp(WHATSAPP_FROM)

    if not (TWILIO_SID and TWILIO_AUTH and to):
        raise SystemExit(
            "Missing Twilio env vars: need TWILIO_ACCOUNT_SID (or TWILIO_SID), "
            "TWILIO_AUTH_TOKEN, and TWILIO_WHATSAPP_TO (or WHATSAPP_TO/TO_NUMBER)."
        )

    api = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"

    # IMPORTANT: url-encode so '+' is preserved as %2B (not turned into a space)
    form = urlencode({"From": from_, "To": to, "Body": body}).encode("utf-8")

    auth = b64encode(f"{TWILIO_SID}:{TWILIO_AUTH}".encode()).decode()
    req = Request(api, data=form, method="POST", headers={
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "bins-notifier/1.0",
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
                    "garden": "Garden waste",
                }.get(key, key.title())
                services.append(label)

    # Always allow a manual test if FORCE_SEND is on
    if FORCE_SEND:
        test = f"Test ping from workflow at {int(datetime.utcnow().timestamp())} ✅"
        body = f"{test}\n\n({postcode} — {hint})"
        print(">>> FORCE_SEND on — sending test WhatsApp:")
        print(body)
        send_whatsapp(body)
        print(">>> Test message sent ✓")
        return

    if not services:
        print(">>> No collections tomorrow — no WhatsApp sent.")
        return

    nice_date = tomorrow.strftime("%A %d %B")
    lines = [
        f"Bin reminder for {postcode} — {hint}",
        f"Tomorrow ({nice_date}):",
        *[f"• {s}" for s in services],
    ]
    body = "\n".join(lines)
    print(">>> Sending WhatsApp message:\n" + body)
    send_whatsapp(body)
    print(">>> Reminder sent ✓")


if __name__ == "__main__":
    print(f">>> FORCE_SEND={FORCE_SEND}  BIN_JSON_URL={'set' if BIN_JSON_URL else 'unset'}")
    print(f">>> Twilio: SID={'set' if TWILIO_SID else 'unset'}  FROM={'***' if WHATSAPP_FROM else 'unset'}  TO={'set' if WHATSAPP_TO else 'unset'}")
    main()

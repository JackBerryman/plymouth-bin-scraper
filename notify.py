#!/usr/bin/env python3
"""
notify.py — reads your published JSON and sends a WhatsApp reminder via Twilio.
Includes a FORCE_SEND test hook you can trigger via workflow input or env var.
"""

import os, json
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from base64 import b64encode

try:
    from zoneinfo import ZoneInfo  # 3.9+
except ImportError:
    ZoneInfo = None

# -------- helpers --------
def truthy(s: str) -> bool:
    if s is None:
        return False
    return str(s).strip().lower() in {"1", "true", "yes", "y", "on"}

UK_TZ = ZoneInfo("Europe/London") if ZoneInfo else None

def now_uk_date():
    if UK_TZ:
        return datetime.now(UK_TZ).date()
    return datetime.utcnow().date()

def fetch_json(url: str, local_fallback: str):
    if url:
        print(f">>> Fetching live data from {url}")
        req = Request(url, headers={"User-Agent": "bins-notifier/1.0"})
        try:
            with urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except HTTPError as e:
            raise SystemExit(f"HTTP error fetching {url}: {e.code} {e.reason}")
        except URLError as e:
            raise SystemExit(f"Network error fetching {url}: {e.reason}")
    print(f">>> Using local data from {local_fallback}")
    with open(local_fallback, "r", encoding="utf-8") as f:
        return json.load(f)

def send_whatsapp(account_sid: str, auth_token: str, from_whatsapp: str, to_whatsapp: str, body: str):
    if not (account_sid and auth_token and to_whatsapp):
        raise SystemExit(
            "Missing Twilio env vars: need TWILIO_ACCOUNT_SID (or TWILIO_SID), "
            "TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_TO (or TO_NUMBER)"
        )
    api = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
    payload = f"From={from_whatsapp}&To={to_whatsapp}&Body={body}".encode("utf-8")
    auth = b64encode(f"{account_sid}:{auth_token}".encode()).decode()
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

# -------- main --------
def main():
    # Inputs / env
    bin_json_url   = os.getenv("BIN_JSON_URL", "").strip()
    local_json     = "public/PL6_5HX_72_Windermere.json"

    # Twilio envs (support both naming styles)
    sid            = os.getenv("TWILIO_ACCOUNT_SID") or os.getenv("TWILIO_SID")
    token          = os.getenv("TWILIO_AUTH_TOKEN")
    from_whatsapp  = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
    to_whatsapp    = os.getenv("TWILIO_WHATSAPP_TO") or os.getenv("TO_NUMBER")

    # Test hook
    force_send     = truthy(os.getenv("FORCE_SEND"))
    test_text      = os.getenv("TEST_TEXT", "Test: bin reminder pipeline works ✅")

    # Debug (safe) log
    print(f">>> FORCE_SEND={force_send}  BIN_JSON_URL={'set' if bin_json_url else 'unset'}")
    print(f">>> Twilio: SID={'set' if sid else 'unset'}  FROM={from_whatsapp}  TO={'set' if to_whatsapp else 'unset'}")

    data = fetch_json(bin_json_url, local_json)
    postcode  = data.get("postcode", "")
    hint      = data.get("address_hint", "")
    colls     = data.get("collections", {})

    today     = now_uk_date()
    tomorrow  = today + timedelta(days=1)

    # FORCE SEND branch
    if force_send:
        body = f"{test_text}\n({postcode} — {hint})"
        print(">>> FORCE_SEND on — sending test WhatsApp:")
        print(body)
        send_whatsapp(sid, token, from_whatsapp, to_whatsapp, body)
        print(">>> Test message sent ✓")
        return

    # Normal path: find tomorrow’s bins
    services = []
    for key, arr in (colls or {}).items():
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

    if not services:
        print(">>> No collections tomorrow — no WhatsApp sent.")
        return

    nice_date = tomorrow.strftime("%A %d %B")
    body = "Bin reminder for {pc} — {hint}\nTomorrow ({date}):\n{lines}".format(
        pc=postcode, hint=hint, date=nice_date,
        lines="\n".join(f"• {s}" for s in services)
    )
    print(">>> Sending WhatsApp message:\n" + body)
    send_whatsapp(sid, token, from_whatsapp, to_whatsapp, body)
    print(">>> Message sent ✓")

if __name__ == "__main__":
    main()

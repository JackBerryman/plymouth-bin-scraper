# scraper.py
# Full, cleaned-up scraper (no ICS warning):
# - Robust iframe handling for Plymouth AchieveForms
# - Forced dotenv path loading (always finds your .env)
# - Optional manual selectors for tricky controls
# - Smart wait for results (polls until dates appear)
# - Clean JSON output grouped by bin type (refuse/recycling/garden)
# - .ics calendar generation using .serialize() (removes ics warning)
# - Cache control via CACHE_TTL_HOURS (0 = always fresh)
#
# Usage (local test):
#   python scraper.py
# Outputs (default): ./public/<POSTCODE>_<HINT>.json and .ics

import os
import re
import json
import asyncio
from pathlib import Path
from datetime import datetime, UTC

from dateutil import parser as dateparser
from playwright.async_api import async_playwright, Page, Frame

from cache import init_db, get_cache, set_cache

# ------------- .env (forced path) ------------------------------------------------
from dotenv import load_dotenv
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)
print(f">>> .env loaded from: {ENV_PATH}")

FORM_URL = os.getenv("FORM_URL") or (
    "https://plymouth-self.achieveservice.com/en/AchieveForms/"
    "?form_uri=sandbox-publish://AF-Process-31283f9a-3ae7-4225-af71-bf3884e0ac1b/"
    "AF-Stagedba4a7d5-e916-46b6-abdb-643d38bec875/definition.json"
    "&redirectlink=%2Fen&cancelRedirectLink=%2Fen&consentMessage=yes"
)
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
CACHE_TTL_HOURS = int(os.getenv("CACHE_TTL_HOURS", "24"))
DEBUG_PAUSE = os.getenv("DEBUG_PAUSE", "false").lower() == "true"

# Optional manual selectors if auto-detection struggles
POSTCODE_SELECTOR = os.getenv("POSTCODE_SELECTOR")        # e.g. input#postcode
FIND_BUTTON_SELECTOR = os.getenv("FIND_BUTTON_SELECTOR")  # e.g. input[value="Find address"]

# Optional outputs dir (default: ./public)
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR") or "public")

# ------------- Date patterns -----------------------------------------------------
MONTHS = r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|June|July|August|September|October|November|December"
DATE_PATTERNS = [
    re.compile(rf"\b\d{{1,2}}\s+(?:{MONTHS})\s+\d{{4}}\b", re.I),   # 27 October 2025
    re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b"),               # 27/10/2025 or 27-10-25
    re.compile(r"\b\d{4}-\d{2}-\d{2}T?\d{2}?:?\d{2}?:?\d{2}?\b"),   # 2025-10-29T00:00:00
]

# Map visible section headers to canonical services
HEADER_TO_SERVICE = [
    (re.compile(r"\bgreen\b.*\brecycling\b", re.I), "recycling"),
    (re.compile(r"\bgarden\b.*\bwaste\b", re.I),    "garden"),
    (re.compile(r"\bbrown\b.*\b(domestic|refuse)\b", re.I), "refuse"),
    (re.compile(r"\bdomestic\b", re.I),             "refuse"),
    (re.compile(r"\brefuse\b", re.I),               "refuse"),
]

# ------------- Utilities ---------------------------------------------------------
def normalise_postcode(pc: str) -> str:
    pc = pc.strip().upper().replace(" ", "")
    return pc[:-3] + " " + pc[-3:] if len(pc) > 3 else pc

async def click_cookies(page: Page):
    for label in ["Accept all", "Accept All", "I agree", "Agree", "Accept"]:
        try:
            btn = page.get_by_role("button", name=re.compile(label, re.I))
            if await btn.count() > 0:
                print(">>> Clicking consent:", await btn.first.inner_text())
                await btn.first.click()
                await page.wait_for_load_state("networkidle")
                break
        except Exception:
            pass

def looks_like_achieveforms(url: str) -> bool:
    u = (url or "").lower()
    return any(k in u for k in ["fillform", "achieveforms", "af-stage", "af-process", "achieveservice"])

async def find_postcode_frame(page: Page) -> Frame | Page:
    frames = list(page.frames)
    print(">>> Frames discovered:")
    for f in frames: print("   -", f.url)

    candidates = [f for f in frames if looks_like_achieveforms(f.url)]
    order = candidates + [f for f in frames if f not in candidates] + [page]

    patterns = [
        ("role:textbox(name='postcode')", lambda fr: fr.get_by_role("textbox", name=re.compile(r"post\s*code", re.I))),
        ("label('Postcode')",             lambda fr: fr.get_by_label(re.compile(r"post\s*code", re.I))),
        ("placeholder includes",          lambda fr: fr.get_by_placeholder(re.compile(r"post\s*code", re.I))),
        ("input[type=text]",              lambda fr: fr.locator("input[type='text']")),
        ("any input",                     lambda fr: fr.locator("input")),
    ]

    for fr in order:
        try:
            for tag, factory in patterns:
                loc = factory(fr)
                if await loc.count() > 0:
                    print(f">>> Using frame: {getattr(fr, 'url', None)} by pattern {tag}")
                    return fr
        except Exception:
            continue

    print(">>> WARNING: no frame with a clear textbox found; falling back to top page.")
    return page

async def find_postcode_input(fr: Frame | Page):
    if POSTCODE_SELECTOR:
        loc = fr.locator(POSTCODE_SELECTOR)
        if await loc.count() > 0:
            print(f">>> Using custom POSTCODE_SELECTOR: {POSTCODE_SELECTOR}")
            return loc.first
    for factory in [
        lambda: fr.get_by_role("textbox", name=re.compile(r"post\s*code", re.I)),
        lambda: fr.get_by_label(re.compile(r"post\s*code", re.I)),
        lambda: fr.get_by_placeholder(re.compile(r"post\s*code", re.I)),
        lambda: fr.locator("input[type='search']"),
        lambda: fr.locator("input[type='text']"),
        lambda: fr.locator("input"),
    ]:
        try:
            loc = factory()
            if await loc.count() > 0:
                return loc.first
        except Exception:
            pass
    return None

def text_has_date(s: str) -> bool:
    if not s: return False
    return any(rx.search(s) for rx in DATE_PATTERNS)

def parse_any_date(line: str):
    if not line: return None
    for rx in DATE_PATTERNS:
        m = rx.search(line)
        if m:
            try:
                return dateparser.parse(m.group(0), dayfirst=True, fuzzy=True)
            except Exception:
                continue
    return None

def classify_header(line: str) -> str | None:
    for rx, service in HEADER_TO_SERVICE:
        if rx.search(line):
            return service
    return None

# ------------- Browser flow ------------------------------------------------------
async def run_form(page: Page, postcode: str, address_hint: str = "") -> str:
    await page.goto(FORM_URL, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle")
    print(">>> Page URL:", page.url)

    await click_cookies(page)
    if DEBUG_PAUSE:
        print(">>> DEBUG_PAUSE: opening Inspector. Press ▶ to continue.")
        await page.pause()

    # 1) postcode + find
    target = await find_postcode_frame(page)
    print(">>> Searching for postcode input…")
    input_loc = await find_postcode_input(target)
    if not input_loc:
        raise RuntimeError(
            "Could not find the postcode input. "
            "If you can see it, set DEBUG_PAUSE=true, copy a selector, and set POSTCODE_SELECTOR in .env."
        )
    await input_loc.fill(postcode)
    print(f">>> Filled postcode: {postcode}")

    # 2) Click the "Find address" control
    clicked = False
    if FIND_BUTTON_SELECTOR:
        btn = target.locator(FIND_BUTTON_SELECTOR)
        if await btn.count() > 0:
            print(f">>> Using custom FIND_BUTTON_SELECTOR: {FIND_BUTTON_SELECTOR}")
            await btn.first.click()
            clicked = True

    if not clicked:
        button_labels = ["Find address", "Find Address", "Lookup", "Search", "Find"]
        for label in button_labels:
            for query in [
                f"button:has-text('{label}')",
                f"[role='button']:has-text('{label}')",
                f"input[type='submit'][value*='{label.split()[0]}']",
                f"input[type='button'][value*='{label.split()[0]}']",
                f"a:has-text('{label}')"
            ]:
                try:
                    btn = target.locator(query)
                    if await btn.count() > 0:
                        print(f">>> Clicking via query: {query}")
                        await btn.first.click()
                        clicked = True
                        break
                except Exception:
                    pass
            if clicked: break

    if not clicked:
        try:
            btn = target.locator(":text('Find')")
            if await btn.count() > 0:
                print(">>> Clicking any element containing 'Find'")
                await btn.first.click()
                clicked = True
        except Exception:
            pass

    if not clicked:
        print(">>> WARNING: Could not find a visible 'Find address' button.")

    # 3) Wait for address options to actually appear, then select
    cb = target.get_by_role("combobox").first
    await cb.wait_for(timeout=20000)

    # wait until more than just the placeholder option is present (up to ~12s)
    for _ in range(48):
        opts_now = [o.strip() for o in await cb.locator("option").all_text_contents()]
        real = [o for o in opts_now if o and "Select Address" not in o]
        if real: break
        await page.wait_for_timeout(250)

    options = await cb.locator("option").all_text_contents()
    print(">>> Address options (first few):", options[:5], "..." if len(options) > 5 else "")

    idx = 0
    if address_hint:
        for i, opt in enumerate(options):
            if address_hint.lower() in opt.lower():
                idx = i; break
    await cb.select_option(index=idx)
    try:
        print(f">>> Selected address: {options[idx]}")
    except Exception:
        pass

    # 4) Continue to results
    for label in ["Next", "Continue", "Submit", "Search", "Show", "Proceed"]:
        btn = target.get_by_role("button", name=re.compile(label, re.I))
        if await btn.count() > 0:
            print(">>> Clicking:", await btn.first.inner_text())
            await btn.first.click()
            break

    # 5) Patiently wait for the results page to actually include a date
    total_ms = 0
    text = ""
    for _ in range(90 * 4):  # 90s @ 250ms
        try:
            text = "\n".join(await target.locator("body").all_text_contents())
        except Exception:
            text = ""
        if not text:
            try:
                text = "\n".join(await page.locator("body").all_text_contents())
            except Exception:
                text = ""
        if text_has_date(text):
            print(">>> Dates detected in results.")
            break
        await page.wait_for_timeout(250)
        total_ms += 250
        if total_ms % 5000 == 0:
            print(f">>> Waiting for dates... {total_ms//1000}s")

    if not text or not text_has_date(text):
        try:
            Path("debug.html").write_text(await page.content(), encoding="utf-8")
            await page.screenshot(path="debug.png", full_page=True)
            print(">>> Saved debug.html and debug.png (no dates detected).")
        except Exception:
            pass

    return text or ""

# ------------- Clean parsing into grouped collections ----------------------------
def extract_collections(raw_text: str):
    """
    Convert the noisy page text into a clean structure:
    {
      "refuse":     ["2025-11-05", "2025-11-19", ...],
      "recycling":  ["2025-10-29", "2025-11-12", ...],
      "garden":     ["2025-11-06", "2025-11-20", ...]
    }
    """
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    collections = {"refuse": [], "recycling": [], "garden": []}
    current = None

    # Pass 1: walk the "Your next bin collection dates are" section
    for line in lines:
        # update current section when we hit a header
        sec = classify_header(line)
        if sec:
            current = sec
            continue

        # try to parse any date on the current line
        dt = parse_any_date(line)
        if dt:
            svc = current
            # If we haven't seen a header yet, guess service from text
            if not svc:
                lower = line.lower()
                if "recycling" in lower: svc = "recycling"
                elif "garden" in lower:  svc = "garden"
                elif "domestic" in lower or "refuse" in lower: svc = "refuse"
                else:
                    svc = None

            if svc in collections:
                collections[svc].append(dt.date())

    # Pass 2: parse the condensed “Round…Date…” stream (ISO-like)
    iso_dates = set()
    iso_rx = re.compile(r"\b(\d{4}-\d{2}-\d{2})T")
    for line in lines:
        for m in iso_rx.finditer(line):
            try:
                iso_dates.add(datetime.fromisoformat(m.group(1)).date())
            except Exception:
                pass

    # If we found ISO dates but some lists are empty and pass 1 didn't find any,
    # use a naive alternating pattern (recycling/refuse) to fill gaps.
    if iso_dates and not any(collections.values()):
        sorted_iso = sorted(iso_dates)
        alt = ["recycling", "refuse"]
        for i, d in enumerate(sorted_iso):
            collections[alt[i % 2]].append(d)

    # Deduplicate + sort + format yyyy-mm-dd
    for k in collections:
        dedup = sorted(set(collections[k]))
        collections[k] = [d.isoformat() for d in dedup]

    return collections

# ------------- Optional: build an .ics calendar ---------------------------------
def build_ics(collections: dict, title: str = "Bin Collections"):
    try:
        from ics import Calendar, Event
    except Exception:
        return None

    cal = Calendar()
    name_map = {"refuse": "Refuse (brown)", "recycling": "Recycling (green)", "garden": "Garden waste"}
    for svc, dates in collections.items():
        for ds in dates:
            ev = Event()
            ev.name = f"{name_map.get(svc, svc.title())} bin collection"
            ev.begin = ds
            ev.make_all_day()
            cal.events.add(ev)
    return cal

# ------------- Top-level scrape --------------------------------------------------
async def scrape(postcode: str, address_hint: str = "", ttl_hours: int = CACHE_TTL_HOURS):
    init_db()
    postcode = normalise_postcode(postcode)
    cache_key = f"{postcode}|{address_hint}".lower()

    use_cache = bool(ttl_hours and ttl_hours > 0)
    if use_cache:
        if cached := get_cache(cache_key, ttl_seconds=ttl_hours * 3600):
            print(">>> Using cached result")
            return cached
    else:
        print(f">>> Cache disabled (ttl={ttl_hours}) — forcing fresh scrape")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        page = await browser.new_page()
        raw = await run_form(page, postcode, address_hint)
        await browser.close()

    collections = extract_collections(raw)
    data = {
        "postcode": postcode,
        "address_hint": address_hint,
        "collections": collections,
        "scraped_at": datetime.now(UTC).isoformat()
    }

    if use_cache:
        set_cache(cache_key, data)

    return data

# ------------- CLI entrypoint ----------------------------------------------------
if __name__ == "__main__":
    # Adjust the default test inputs if needed:
    TEST_POSTCODE = "PL6 5HX"
    TEST_HINT = "72 Windermere"

    result = asyncio.run(scrape(TEST_POSTCODE, TEST_HINT))

    # Print clean JSON to console
    print(json.dumps(result, indent=2))

    # Also write JSON + ICS locally (to ./public by default)
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        stem = f"{TEST_POSTCODE.replace(' ', '_')}_{TEST_HINT.replace(' ', '_') or 'addr'}"
        json_text = json.dumps(result, indent=2)
        (OUTPUT_DIR / f"{stem}.json").write_text(json_text, encoding="utf-8")

        cal = build_ics(result["collections"], title=f"Bin Collections {TEST_POSTCODE} {TEST_HINT}")
        if cal is not None:
            ics_text = cal.serialize()  # <-- use serialize() to avoid ics warning
            (OUTPUT_DIR / f"{stem}.ics").write_text(ics_text, encoding="utf-8")
            # Stable aliases (optional)
            (OUTPUT_DIR / "latest.json").write_text(json_text, encoding="utf-8")
            (OUTPUT_DIR / "latest.ics").write_text(ics_text, encoding="utf-8")

        print(f">>> Wrote: {OUTPUT_DIR / (stem + '.json')}")
        if cal is not None:
            print(f">>> Wrote: {OUTPUT_DIR / (stem + '.ics')}")
    except Exception as e:
        print(">>> Output write skipped:", e)
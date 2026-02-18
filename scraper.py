#!/usr/bin/env python3
"""
Scrapes Plymouth Council's "Waste - Check your bin day" (AchieveForms) with Playwright,
parses visible results into per-service dates, filters out the "Today's date" line,
and writes JSON + ICS to /public.

ENV (optional)
--------------
FORM_URL        - AchieveForms URL (defaults to Plymouth "Check your bin day")
HEADLESS        - "true"/"false" (default true)
DEBUG_PAUSE     - "true" to open Playwright Inspector before interacting (default false)
OUTPUT_DIR      - output folder (default "public")

POSTCODE / ADDRESS_HINT                            (preferred)
POSTCODE_INPUT / ADDRESS_HINT_INPUT                (manual workflow inputs)
POSTCODE_DEFAULT / ADDRESS_HINT_DEFAULT            (scheduled workflow defaults)
"""

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dateutil.tz import gettz
from ics import Calendar, Event
from ics.grammar.parse import ContentLine
from playwright.async_api import async_playwright, TimeoutError as PWTimeout, Frame

# -----------------------------------
# Config
# -----------------------------------

DEFAULT_FORM_URL = (
    "https://plymouth-self.achieveservice.com/en/AchieveForms/"
    "?form_uri=sandbox-publish://AF-Process-31283f9a-3ae7-4225-af71-bf3884e0ac1b/"
    "AF-Stagedba4a7d5-e916-46b6-abdb-643d38bec875/definition.json"
    "&redirectlink=%2Fen&cancelRedirectLink=%2Fen&consentMessage=yes"
)

UK_TZ = gettz("Europe/London")

# Lines containing "today" (e.g., "Today's date: 24/10/2025") must NOT be treated as collections.
RE_TODAY_LINE = re.compile(r"\btoday\b", re.IGNORECASE)

# Flexible date patterns present on the page
DATE_PATTERNS = (
    r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*\d{2}/\d{2}/\d{4}\b",
    r"\b\d{2}/\d{2}/\d{4}\b",
)

DATE_ANY_REGEX = re.compile(DATE_PATTERNS[1])  # for quick presence checks

# Service section keywords (used to attribute dates)
# Tightened to avoid false matches.
SERVICE_KEYWORDS = {
    "refuse": (
        "brown domestic bin",
        "brown bin",
        "refuse",
    ),
    "recycling": (
        "green recycling bin",
        "recycling bin",
        "recycling",
    ),
    "garden": (
        "garden waste bin",
        "garden waste",
    ),
}

# How far back we search from a date line to find the nearest service heading.
LOOKBACK_LINES_FOR_SERVICE = 8

# -----------------------------------
# Helpers
# -----------------------------------

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y"}

def sanitize_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s)

def ddmmyyyy_to_iso(text: str) -> Optional[str]:
    """Extract first DD/MM/YYYY inside text and return as YYYY-MM-DD."""
    m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%d/%m/%Y").date().isoformat()
    except ValueError:
        return None

def classify_service_from_text(line: str) -> Optional[str]:
    """
    Returns a service key if this line looks like a service heading.
    (We keep it strict to avoid accidental matches.)
    """
    t = line.lower()
    for svc, keys in SERVICE_KEYWORDS.items():
        if any(k in t for k in keys):
            return svc
    return None

@dataclass
class ScrapeResult:
    postcode: str
    address_hint: str
    collections: Dict[str, List[str]] = field(default_factory=lambda: {
        "refuse": [], "recycling": [], "garden": []
    })
    scraped_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def add(self, service: str, iso_date: str):
        if service not in self.collections:
            self.collections[service] = []
        if iso_date not in self.collections[service]:
            self.collections[service].append(iso_date)

    def sort_dedupe(self):
        for k, arr in self.collections.items():
            arr[:] = sorted(sorted(set(arr)))

# -----------------------------------
# Playwright: drive the form
# -----------------------------------

async def run_form(page, form_url: str, postcode: str, address_hint: str) -> Frame:
    await page.goto(form_url, wait_until="domcontentloaded")
    print(f">>> Page URL: {page.url}")

    # Find the embedded form iframe
    frames = page.frames
    print(">>> Frames discovered:")
    for fr in frames:
        print(f"   - {fr.url}")

    form_frame = next((fr for fr in frames if "/fillform/" in fr.url), None)

    # If not already present, wait for iframe element and get its content frame
    if not form_frame:
        await page.wait_for_selector("iframe[src*='fillform']", timeout=30000)
        iframe_el = await page.query_selector("iframe[src*='fillform']")
        if not iframe_el:
            raise RuntimeError("AchieveForms iframe element not found.")
        form_frame = await iframe_el.content_frame()
        if not form_frame:
            raise RuntimeError("Iframe found, but content frame isn't available yet.")

    print(f">>> Using frame: {getattr(form_frame, 'url', '[frame]')}")

    # Locate postcode/street textbox
    textbox = None
    try:
        textbox = form_frame.get_by_role("textbox", name=re.compile("post.*code|street", re.I))
        await textbox.wait_for(state="visible", timeout=15000)
    except Exception:
        pass

    if not textbox:
        try:
            textbox = form_frame.locator("input[type='text']").first
            await textbox.wait_for(state="visible", timeout=15000)
        except Exception as e:
            raise RuntimeError("Could not find the postcode input.") from e

    await textbox.fill(postcode)
    print(f">>> Filled postcode: {postcode}")

    # Click Find (or press Enter)
    try:
        await form_frame.get_by_role("button", name=re.compile("find", re.I)).click(timeout=10000)
    except Exception:
        await textbox.press("Enter")

    # Wait for address dropdown
    select = form_frame.get_by_role("combobox").first
    await select.wait_for(state="visible", timeout=30000)

    # Log a few options
    all_texts = await select.all_inner_texts()
    first = all_texts[0] if all_texts else ""
    print(f">>> Address options (first few): {first.splitlines()[:5]} ...")

    # Pick best match, else first non-placeholder
    opt_xpath = (
        "//option[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
        f"'{address_hint.lower()}')]"
    )
    options = form_frame.locator(opt_xpath)
    if await options.count():
        value = await options.first.get_attribute("value")
        await select.select_option(value=value)
        chosen = await options.first.inner_text()
    else:
        await select.select_option(index=1)
        chosen = await select.input_value()
    print(f">>> Selected address: {chosen.strip()}")

    # Wait until Collection Details (or any date) appears
    try:
        await form_frame.get_by_text(re.compile(r"Collection Details", re.I)).wait_for(timeout=30000)
    except PWTimeout:
        await form_frame.get_by_text(re.compile(r"\b\d{2}/\d{2}/\d{4}\b")).wait_for(timeout=30000)
    print(">>> Dates detected in results.")

    return form_frame

# -----------------------------------
# Extract + parse text
# -----------------------------------

async def extract_text_content(form_frame: Frame) -> str:
    """
    Get all visible text from the SAME frame we interacted with.
    Ensure at least one DD/MM/YYYY is present (with a small wait loop).
    """
    content = ""
    for _ in range(10):
        content = await form_frame.evaluate(
            """
() => {
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  const out = [];
  while (walker.nextNode()) {
    const t = walker.currentNode.nodeValue;
    if (t && t.trim()) out.push(t.trim());
  }
  return out.join("\\n");
}
"""
        )
        if DATE_ANY_REGEX.search(content):
            return content
        await form_frame.wait_for_timeout(500)

    return content

def _nearest_service_for_date(lines: List[str], idx: int) -> Optional[str]:
    """
    Given a date line at lines[idx], scan backwards up to LOOKBACK_LINES_FOR_SERVICE
    to find the nearest service heading. This avoids relying on the exact DOM text order.
    """
    start = max(0, idx - LOOKBACK_LINES_FOR_SERVICE)
    for j in range(idx, start - 1, -1):
        svc = classify_service_from_text(lines[j])
        if svc:
            return svc
    return None

def parse_collections_from_text(full_text: str) -> Dict[str, List[str]]:
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
    collections = {"refuse": [], "recycling": [], "garden": []}
    re_dates = [re.compile(p) for p in DATE_PATTERNS]

    for i, ln in enumerate(lines):
        if RE_TODAY_LINE.search(ln):
            continue

        # Extract all date strings from this line
        matches: List[str] = []
        for rx in re_dates:
            matches.extend(rx.findall(ln))

        if not matches:
            continue

        # Attribute these dates to the nearest service heading above this line
        svc = _nearest_service_for_date(lines, i)
        if not svc or svc not in collections:
            continue

        for raw in matches:
            iso = ddmmyyyy_to_iso(raw)
            if not iso:
                continue
            if iso not in collections[svc]:
                collections[svc].append(iso)

    for k in collections.keys():
        collections[k] = sorted(sorted(set(collections[k])))

    return collections

# -----------------------------------
# ICS writer
# -----------------------------------

def build_ics(postcode: str, address_hint: str, collections: Dict[str, List[str]]) -> str:
    cal = Calendar()
    cal.extra.append(ContentLine(name="X-WR-CALNAME", value=f"Bin collections — {postcode} — {address_hint}"))

    titles = {
        "refuse": "Refuse (brown bin)",
        "recycling": "Recycling (green bin)",
        "garden": "Garden waste",
    }

    for service, dates in collections.items():
        title = titles.get(service, service.title())
        for iso in dates:
            d = datetime.strptime(iso, "%Y-%m-%d").date()
            ev = Event()
            ev.name = title
            ev.begin = d
            ev.make_all_day()
            ev.description = f"{title} — {postcode} — {address_hint}"
            cal.events.add(ev)

    return cal.serialize()

# -----------------------------------
# Main scrape + outputs
# -----------------------------------

async def scrape(postcode: str, address_hint: str, form_url: str, headless: bool) -> "ScrapeResult":
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        if env_bool("DEBUG_PAUSE", False):
            await page.pause()

        form_frame = await run_form(page, form_url, postcode, address_hint)
        text_blob = await extract_text_content(form_frame)

        if not DATE_ANY_REGEX.search(text_blob):
            preview = "\n".join(text_blob.splitlines()[:40])
            print(">>> WARNING: no DD/MM/YYYY detected in extracted text. Preview:")
            print(preview)

        collections = parse_collections_from_text(text_blob)

        await browser.close()

    res = ScrapeResult(postcode=postcode, address_hint=address_hint, collections=collections)
    res.sort_dedupe()
    return res

def write_outputs(res: ScrapeResult, outdir: Path) -> Tuple[Path, Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    base = f"{sanitize_filename(res.postcode.replace(' ', '_'))}_{sanitize_filename(res.address_hint.replace(' ', '_'))}"
    json_path = outdir / f"{base}.json"
    ics_path = outdir / f"{base}.ics"

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "postcode": res.postcode,
                "address_hint": res.address_hint,
                "collections": res.collections,
                "scraped_at": res.scraped_at,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    with ics_path.open("w", encoding="utf-8") as f:
        f.write(build_ics(res.postcode, res.address_hint, res.collections))

    return json_path, ics_path

# -----------------------------------
# Entrypoint
# -----------------------------------

if __name__ == "__main__":
    FORM_URL = os.getenv("FORM_URL", DEFAULT_FORM_URL)
    HEADLESS = env_bool("HEADLESS", True)
    OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "public"))

    postcode = (
        os.getenv("POSTCODE")
        or os.getenv("POSTCODE_INPUT")
        or os.getenv("POSTCODE_DEFAULT")
        or "PL6 5HX"
    )
    address_hint = (
        os.getenv("ADDRESS_HINT")
        or os.getenv("ADDRESS_HINT_INPUT")
        or os.getenv("ADDRESS_HINT_DEFAULT")
        or "72 Windermere"
    )

    print(f">>> Using FORM_URL: {FORM_URL}")
    print(f">>> Headless: {HEADLESS}")
    print(f">>> Postcode: {postcode} | Address hint: {address_hint}")

    result = asyncio.run(scrape(postcode, address_hint, FORM_URL, HEADLESS))
    print(json.dumps(
        {
            "postcode": result.postcode,
            "address_hint": result.address_hint,
            "collections": result.collections,
            "scraped_at": result.scraped_at,
        },
        indent=2,
    ))

    jp, ip = write_outputs(result, OUTPUT_DIR)
    print(f">>> Wrote: {jp}")
    print(f">>> Wrote: {ip}")

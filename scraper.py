#!/usr/bin/env python3
"""
Scrapes Plymouth Council's "Waste - Check your bin day" form with Playwright,
extracts collection dates per service, filters out the "Today's date" line,
and writes both JSON and ICS to /public.

ENV (optional):
  FORM_URL               - the form definition URL
  HEADLESS               - "true"/"false" (default true)
  DEBUG_PAUSE            - "true" to open Playwright Inspector (default false)
  CACHE_TTL_HOURS        - not used in this version (kept for compatibility)
  OUTPUT_DIR             - where to write outputs (default "public")
  POSTCODE               - postcode (e.g., "PL6 5HX")
  ADDRESS_HINT           - free-text to match the correct address option
                           (e.g., "72 Windermere")
"""

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ics import Calendar, Event
from dateutil.tz import gettz

# Playwright (async)
from playwright.async_api import async_playwright, TimeoutError as PWTimeout


# ---------- Config & helpers ----------

DEFAULT_FORM_URL = (
    "https://plymouth-self.achieveservice.com/en/AchieveForms/"
    "?form_uri=sandbox-publish://AF-Process-31283f9a-3ae7-4225-af71-bf3884e0ac1b/"
    "AF-Stagedba4a7d5-e916-46b6-abdb-643d38bec875/definition.json"
    "&redirectlink=%2Fen&cancelRedirectLink=%2Fen&consentMessage=yes"
)

UK_TZ = gettz("Europe/London")

DATE_PATTERNS = (
    # e.g. "Wednesday, 05/11/2025" or "Wed, 05/11/2025"
    r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*\d{2}/\d{2}/\d{4}\b",
    # e.g. "05/11/2025"
    r"\b\d{2}/\d{2}/\d{4}\b",
)

SERVICE_KEYWORDS = {
    "refuse": ("brown domestic bin", "domestic bin", "refuse"),
    "recycling": ("green recycling bin", "recycling"),
    "garden": ("garden waste bin", "garden"),
}

RE_TODAY_LINE = re.compile(r"\btoday\b", re.IGNORECASE)


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y"}


def sanitize_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s)


def ddmmyyyy_to_iso(s: str) -> Optional[str]:
    """Parse 'DD/MM/YYYY' (optionally with weekday prefix) -> 'YYYY-MM-DD'."""
    # Strip any leading weekday like 'Wed, '
    m = re.search(r"(\d{2}/\d{2}/\d{4})", s)
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1), "%d/%m/%Y").date()
        return dt.isoformat()
    except ValueError:
        return None


def classify_service_from_text(text_line: str, current_service: Optional[str]) -> Optional[str]:
    """Update current service section based on line content."""
    t = text_line.lower()
    for service, keys in SERVICE_KEYWORDS.items():
        if any(k in t for k in keys):
            return service
    return current_service


@dataclass
class ScrapeResult:
    postcode: str
    address_hint: str
    collections: Dict[str, List[str]] = field(default_factory=lambda: {
        "refuse": [],
        "recycling": [],
        "garden": [],
    })
    scraped_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def add_date(self, service: str, iso_date: str):
        if service not in self.collections:
            self.collections[service] = []
        if iso_date not in self.collections[service]:
            self.collections[service].append(iso_date)

    def sort_dedupe(self):
        for k, arr in self.collections.items():
            arr[:] = sorted(sorted(set(arr)))


# ---------- Playwright form runner ----------

async def run_form(page, form_url: str, postcode: str, address_hint: str) -> None:
    await page.goto(form_url, wait_until="domcontentloaded")
    print(f">>> Page URL: {page.url}")

    # Find the embedded form iFrame
    frames = page.frames
    print(">>> Frames discovered:")
    for fr in frames:
        print(f"   - {fr.url}")
    # choose the first that looks like fillform
    form_frame = None
    for fr in frames:
        if "/fillform/" in fr.url:
            form_frame = fr
            break
    if not form_frame:
        # sometimes the initial goto needs an extra wait for the fillform to appear
        await page.wait_for_selector("iframe[src*='fillform']", timeout=15000)
        form_frame = page.frame_locator("iframe[src*='fillform']").frame
    if not form_frame:
        raise RuntimeError("Could not locate the AchieveForms iframe.")

    print(f">>> Using frame: {form_frame.url if hasattr(form_frame, 'url') else '[frame]'}")

    # Postcode textbox (try robust selectors)
    # The page uses a labeled textbox "Postcode or street search"
    # Prefer role-based, fall back to input[type=text]
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

    # Click "Find" / trigger the address lookup
    # There is usually a "Find" button near the postcode field
    try:
        find_btn = form_frame.get_by_role("button", name=re.compile("find", re.I))
        await find_btn.click(timeout=10000)
    except Exception:
        # Fallback: press Enter
        await textbox.press("Enter")

    # Wait for the Select Address dropdown to be populated
    select = form_frame.get_by_role("combobox").first
    await select.wait_for(state="visible", timeout=20000)
    # open/select option that matches the address hint best
    options_text = await select.all_inner_texts()
    first_text = options_text[0] if options_text else ""
    print(f">>> Address options (first few): {first_text.splitlines()[:5]} ...")

    # AchieveForms uses <select> with many options; use JavaScript to pick the best match
    opt_xpath = f"//option[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{address_hint.lower()}')]"
    options = form_frame.locator(opt_xpath)
    count = await options.count()
    if count == 0:
        # Fallback: just pick first non-placeholder option
        await select.select_option(index=1)
        chosen = await select.input_value()
    else:
        # choose the first matching option
        value = await options.first.get_attribute("value")
        await select.select_option(value=value)
        chosen = await options.first.inner_text()
    print(f">>> Selected address: {chosen.strip()}")

    # After selecting an address the form loads "Collection Details".
    # Wait for any date to appear OR the section header
    try:
        await form_frame.get_by_text(re.compile(r"Collection Details", re.I)).wait_for(timeout=30000)
    except PWTimeout:
        # if header not found, wait for any date pattern
        date_regex = r"\b\d{2}/\d{2}/\d{4}\b"
        await form_frame.get_by_text(re.compile(date_regex)).wait_for(timeout=30000)
    print(">>> Dates detected in results.")


# ---------- Text extraction & parsing ----------

async def extract_text_content(page) -> str:
    """Get all visible text from the fillform iframe."""
    fr = None
    for f in page.frames:
        if "/fillform/" in f.url:
            fr = f
            break
    if not fr:
        raise RuntimeError("Frame disappeared after run_form.")
    # Pull large text blob
    content = await fr.evaluate(
        """
() => {
  const getText = (el) => {
    const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT, null, false);
    let out = [];
    while (walker.nextNode()) {
      const t = walker.currentNode.nodeValue;
      if (t && t.trim()) out.push(t.trim());
    }
    return out.join("\\n");
  };
  return getText(document.body);
}
"""
    )
    return content


def parse_collections_from_text(full_text: str) -> Dict[str, List[str]]:
    """
    Parse the big text blob into collections per service.
    Critically: we ignore any dates on lines that mention 'today' (the council page
    prints "Today's date: DD/MM/YYYY", which must NOT be treated as a collection).
    """
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
    collections: Dict[str, List[str]] = {"refuse": [], "recycling": [], "garden": []}

    current_service: Optional[str] = None

    # Pre-compile regexes
    re_dates = [re.compile(p) for p in DATE_PATTERNS]

    for ln in lines:
        # If line contains "today", skip any date on this line entirely
        if RE_TODAY_LINE.search(ln):
            continue

        # Track service section
        current_service = classify_service_from_text(ln, current_service)

        # Extract all dates on this line
        matches: List[str] = []
        for rx in re_dates:
            matches.extend(rx.findall(ln))

        for raw in matches:
            iso = ddmmyyyy_to_iso(raw)
            if not iso:
                continue
            # If we don't know the service yet, skip; we only add when under a known section
            if current_service in collections:
                if iso not in collections[current_service]:
                    collections[current_service].append(iso)

    # Sort + dedupe
    for k in collections.keys():
        collections[k] = sorted(sorted(set(collections[k])))

    return collections


# ---------- ICS writer ----------

def build_ics(postcode: str, address_hint: str, collections: Dict[str, List[str]]) -> str:
    """
    Build an ICS calendar string with all-day events for each collection date.
    """
    cal = Calendar()
    cal.extra.append(["X-WR-CALNAME", f"Bin collections — {postcode} — {address_hint}"])

    SERVICE_TITLES = {
        "refuse": "Refuse (brown bin)",
        "recycling": "Recycling (green bin)",
        "garden": "Garden waste",
    }

    for service, dates in collections.items():
        title = SERVICE_TITLES.get(service, service.title())
        for iso in dates:
            # All-day event (date only) in UK timezone
            dt = datetime.strptime(iso, "%Y-%m-%d").date()
            ev = Event()
            ev.name = title
            # For all-day events, use begin/end as date; ics library handles DTSTART/DTEND
            ev.begin = dt
            ev.make_all_day()
            ev.description = f"{title} — {postcode} — {address_hint}"
            cal.events.add(ev)

    # Serialize without deprecation warning
    return cal.serialize()


# ---------- Main runner ----------

async def scrape(postcode: str, address_hint: str, form_url: str, headless: bool) -> ScrapeResult:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        debug_pause = env_bool("DEBUG_PAUSE", False)
        if debug_pause:
            # Useful for manual inspection
            page.on("close", lambda _: print("Page closed"))
            await page.pause()

        await run_form(page, form_url, postcode, address_hint)
        text_blob = await extract_text_content(page)

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

    ics_text = build_ics(res.postcode, res.address_hint, res.collections)
    with ics_path.open("w", encoding="utf-8") as f:
        f.write(ics_text)

    return json_path, ics_path


if __name__ == "__main__":
    # Read env/config
    FORM_URL = os.getenv("FORM_URL", DEFAULT_FORM_URL)
    HEADLESS = env_bool("HEADLESS", True)
    OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "public"))

    # Inputs: prefer explicit envs (as used by GitHub Actions), else defaults for local testing
    postcode = os.getenv("POSTCODE") or os.getenv("POSTCODE_DEFAULT") or os.getenv("POSTCODE_INPUT") or "PL6 5HX"
    address_hint = os.getenv("ADDRESS_HINT") or os.getenv("ADDRESS_HINT_DEFAULT") or os.getenv("ADDRESS_HINT_INPUT") or "72 Windermere"

    print(f">>> Using FORM_URL: {FORM_URL}")
    print(f">>> Headless: {HEADLESS}")
    print(f">>> Postcode: {postcode} | Address hint: {address_hint}")

    result = asyncio.run(scrape(postcode, address_hint, FORM_URL, HEADLESS))
    # Echo result to console
    print(json.dumps(
        {
            "postcode": result.postcode,
            "address_hint": result.address_hint,
            "collections": result.collections,
            "scraped_at": result.scraped_at,
        },
        indent=2
    ))

    jp, ip = write_outputs(result, OUTPUT_DIR)
    print(f">>> Wrote: {jp}")
    print(f">>> Wrote: {ip}")

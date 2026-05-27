#!/usr/bin/env python3
"""
Scrapes Plymouth Council's "Waste - Check your bin day" AchieveForms form,
parses visible results into per-service dates, filters out the "Today's date" line,
and writes JSON + ICS to /public.
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
from playwright.async_api import async_playwright, Frame


# -----------------------------------
# Config
# -----------------------------------

DEFAULT_FORM_URL = (
    "https://plymouth-self.achieveservice.com/AchieveForms/"
    "?mode=fill&consentMessage=yes"
    "&form_uri=sandbox-publish://AF-Process-084d6742-3572-41ba-ac1a-430750451f9d/"
    "AF-Stage-67ba684d-0a5b-48f8-9c50-1c01cc43c396/definition.json"
    "&process=1"
    "&process_uri=sandbox-processes://AF-Process-084d6742-3572-41ba-ac1a-430750451f9d"
    "&process_id=AF-Process-084d6742-3572-41ba-ac1a-430750451f9d"
)

UK_TZ = gettz("Europe/London")

RE_TODAY_LINE = re.compile(r"\btoday\b", re.IGNORECASE)

DATE_PATTERNS = (
    r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*\d{2}/\d{2}/\d{4}\b",
    r"\b\d{2}/\d{2}/\d{4}\b",
)

DATE_ANY_REGEX = re.compile(DATE_PATTERNS[1])

SERVICE_KEYWORDS = {
    "refuse": (
        "brown domestic bin",
        "brown household bin",
        "domestic bin",
        "refuse",
        "general waste",
        "brown bin",
    ),
    "recycling": (
        "green recycling bin",
        "recycling",
        "green bin",
    ),
    "garden": (
        "garden waste collection",
        "garden waste bin",
        "garden waste",
        "green garden bin",
        "garden",
    ),
}


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
    m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
    if not m:
        return None

    try:
        return datetime.strptime(m.group(1), "%d/%m/%Y").date().isoformat()
    except ValueError:
        return None


def classify_service_from_text(line: str, current: Optional[str]) -> Optional[str]:
    t = line.lower()

    for svc, keys in SERVICE_KEYWORDS.items():
        if any(k in t for k in keys):
            return svc

    return current


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

    def add(self, service: str, iso_date: str):
        if service not in self.collections:
            self.collections[service] = []

        if iso_date not in self.collections[service]:
            self.collections[service].append(iso_date)

    def sort_dedupe(self):
        for k, arr in self.collections.items():
            arr[:] = sorted(set(arr))


# -----------------------------------
# Playwright: drive the form
# -----------------------------------

async def find_form_frame(page) -> Frame:
    """
    Find the frame that actually contains a visible postcode/street input.
    AchieveForms may render directly in the main frame or inside an about:blank iframe.
    """
    print(">>> Searching frames for visible postcode/street input...")

    for attempt in range(1, 41):  # approx 20 seconds
        for fr in page.frames:
            try:
                textbox = fr.get_by_role(
                    "textbox",
                    name=re.compile(r"post.*code|street|postcode", re.I),
                ).first

                try:
                    await textbox.wait_for(state="visible", timeout=300)
                    print(f">>> Found visible postcode/street textbox in frame: {fr.url}")
                    return fr
                except Exception:
                    pass

                inputs = fr.locator("input[type='text']")
                count = await inputs.count()

                for i in range(count):
                    inp = inputs.nth(i)
                    try:
                        await inp.wait_for(state="visible", timeout=300)
                        print(f">>> Found visible text input in frame: {fr.url}")
                        return fr
                    except Exception:
                        continue

            except Exception:
                continue

        if attempt in (1, 10, 20, 30, 40):
            print(f">>> Still looking for visible input... attempt {attempt}/40")
            print(">>> Current frames:")
            for fr in page.frames:
                print(f"   - {fr.url}")

        await page.wait_for_timeout(500)

    raise RuntimeError("Could not find a visible postcode/street input in any frame.")


async def find_visible_textbox(form_frame: Frame):
    """
    Return the visible postcode/street textbox from the selected frame.
    """
    try:
        textbox = form_frame.get_by_role(
            "textbox",
            name=re.compile(r"post.*code|street|postcode", re.I),
        ).first
        await textbox.wait_for(state="visible", timeout=5000)
        return textbox
    except Exception:
        pass

    inputs = form_frame.locator("input[type='text']")
    count = await inputs.count()

    for i in range(count):
        inp = inputs.nth(i)
        try:
            await inp.wait_for(state="visible", timeout=1000)
            return inp
        except Exception:
            continue

    raise RuntimeError("Could not find the postcode input.")


async def run_form(page, form_url: str, postcode: str, address_hint: str) -> Frame:
    # Retry because AchieveService can intermittently return /forbidden.
    for attempt in range(1, 4):
        await page.goto(form_url, wait_until="domcontentloaded")
        print(f">>> Page URL attempt {attempt}: {page.url}")

        if "/forbidden" not in page.url.lower():
            break

        print(">>> Hit /forbidden; retrying after 5 seconds...")
        await page.wait_for_timeout(5000)
    else:
        raise RuntimeError(
            "Plymouth/AchieveService returned /forbidden after 3 attempts. "
            "This means the runner/browser is being blocked before the form loads."
        )

    print(">>> Frames discovered:")
    for fr in page.frames:
        print(f"   - {fr.url}")

    form_frame = await find_form_frame(page)
    print(f">>> Using frame: {getattr(form_frame, 'url', '[frame]')}")

    textbox = await find_visible_textbox(form_frame)

    await textbox.fill(postcode)
    print(f">>> Filled postcode: {postcode}")

    try:
        await form_frame.get_by_role("button", name=re.compile("find|search", re.I)).click(timeout=10000)
    except Exception:
        await textbox.press("Enter")

    select = form_frame.get_by_role("combobox").first
    await select.wait_for(state="visible", timeout=30000)

    try:
        all_texts = await select.all_inner_texts()
        first = all_texts[0] if all_texts else ""
        print(f">>> Address options (first few): {first.splitlines()[:5]} ...")
    except Exception:
        pass

    opt_xpath = (
        "//option[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
        f"'{address_hint.lower()}')]"
    )
    options = form_frame.locator(opt_xpath)

    if await options.count():
        value = await options.first.get_attribute("value")
        await select.select_option(value=value)
        chosen = (await options.first.inner_text()).strip()
    else:
        await select.select_option(index=1)
        chosen = (await select.input_value()).strip()

    print(f">>> Selected address: {chosen}")

    await form_frame.get_by_text(re.compile(r"Collection Details", re.I)).wait_for(timeout=30000)

    # Garden waste can load separately; wait for refuse/recycling headings too.
    for _ in range(30):
        brown = await form_frame.get_by_role(
            "heading",
            name=re.compile(r"Brown domestic bin", re.I),
        ).count()
        green = await form_frame.get_by_role(
            "heading",
            name=re.compile(r"Green recycling bin", re.I),
        ).count()

        if brown > 0 or green > 0:
            break

        await form_frame.wait_for_timeout(500)

    # Belt and braces: AchieveForms can render the garden waste section shortly after refuse/recycling.
    await form_frame.wait_for_timeout(3000)

    print(">>> Main collection details detected.")
    return form_frame


# -----------------------------------
# Extract + parse text
# -----------------------------------

async def extract_text_content(form_frame: Frame) -> str:
    """
    Extract visible text from the full form frame using innerText.
    Wait until the main collection details section is present, then wait for the
    visible text to stabilise so later-loaded garden waste data is not missed.
    """
    last_content = ""
    stable_count = 0
    best_content = ""

    for _ in range(30):  # approx 15 seconds
        content = await form_frame.evaluate(
            """
() => (document.body && document.body.innerText) ? document.body.innerText : ""
"""
        )
        content = (content or "").replace("\r\n", "\n").strip()

        has_main_collection = (
            "Collection Details" in content
            and (
                "Brown domestic bin" in content
                or "Green recycling bin" in content
            )
        )

        if has_main_collection:
            best_content = content

            # Wait until the content stops changing. This catches garden waste if it loads slightly later.
            if content == last_content:
                stable_count += 1
            else:
                stable_count = 0

            if stable_count >= 3:
                return content

        last_content = content
        await form_frame.wait_for_timeout(500)

    return best_content or last_content


def parse_collections_from_text(full_text: str) -> Dict[str, List[str]]:
    lines = [ln.strip() for ln in full_text.splitlines() if ln and ln.strip()]
    collections: Dict[str, List[str]] = {
        "refuse": [],
        "recycling": [],
        "garden": [],
    }
    current_service: Optional[str] = None
    re_dates = [re.compile(p) for p in DATE_PATTERNS]

    for ln in lines:
        if RE_TODAY_LINE.search(ln):
            continue

        current_service = classify_service_from_text(ln, current_service)

        matches: List[str] = []
        for rx in re_dates:
            matches.extend(rx.findall(ln))

        if not current_service:
            continue

        for raw in matches:
            iso = ddmmyyyy_to_iso(raw)
            if not iso:
                continue

            if iso not in collections[current_service]:
                collections[current_service].append(iso)

    for k in collections:
        collections[k] = sorted(set(collections[k]))

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
        browser = await pw.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
            locale="en-GB",
            timezone_id="Europe/London",
            extra_http_headers={
                "Accept-Language": "en-GB,en;q=0.9",
            },
        )

        page = await context.new_page()

        if env_bool("DEBUG_PAUSE", False):
            await page.pause()

        form_frame = await run_form(page, form_url, postcode, address_hint)
        text_blob = await extract_text_content(form_frame)

        print(">>> Extracted text preview:")
        print("\n".join(text_blob.splitlines()[:160]))

        if not DATE_ANY_REGEX.search(text_blob):
            preview = "\n".join(text_blob.splitlines()[:80])
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

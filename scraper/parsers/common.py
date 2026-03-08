import re
import unicodedata
from typing import Any, Dict, List, Optional

from playwright.async_api import Page

PRICE_TOKEN_RE = re.compile(r"[£Ł]?\s*\d{2,4}(?:,\d{3})*(?:\.\d{1,2})?")
ROOMISH_RE = re.compile(r"\b(room|studio|suite|ensuite|en-suite|flat|apartment|bedroom|twodio|classic|premium|luxury|vip)\b", re.IGNORECASE)
CONTRACT_RE = re.compile(r"\b\d{1,2}\s*(?:weeks?|months?)\b", re.IGNORECASE)
ACADEMIC_YEAR_RE = re.compile(r"\b(?:AY\s*)?((?:20)?\d{2})\s*[/\-]\s*((?:20)?\d{2})\b", re.IGNORECASE)

INCENTIVE_PATTERNS = [
    r"plus\s+bookings\s+get\s+free\s+annual\s+bus\s+pass",
    r"book\s+today\s*(?:&|and)?\s*get\s+a?\s*free\s+kitchen\s*(?:&|and)\s*bedding\s+pack\s+worth\s*[£Ł]?\s*\d+",
    r"kitchen\s*(?:&|and)\s*bedding\s+pack\s+worth\s*[£Ł]?\s*\d+",
    r"[£Ł]\s*\d+(?:[.,]\d{1,2})?\s*cashback",
    r"cashback",
    r"free\s+annual\s+bus\s+pass",
    r"free\s+bus\s+pass",
    r"bedding\s+pack(?:\s+included)?",
    r"kitchen\s+pack(?:\s+included|(?:\s+worth\s+[£Ł]?\s*\d+)?)?",
    r"voucher",
    r"discount",
]
INCENTIVE_RE = re.compile("(" + "|".join(INCENTIVE_PATTERNS) + ")", re.IGNORECASE)

ROOM_STRIP_RE = re.compile(
    r"([£Ł]\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|per week|weekly|pcm|per month|monthly)|\bview room\b|\bbook now\b|\bjoin waitlist\b|\bavailable\b|\bsold out\b|\bfrom\b|\bcontact us\b|\b\d{1,2}\s*weeks?\b)",
    re.IGNORECASE,
)

FLOOR_NUM_WORD = {
    0: "Ground",
    1: "First",
    2: "Second",
    3: "Third",
    4: "Fourth",
    5: "Fifth",
    6: "Sixth",
    7: "Seventh",
    8: "Eighth",
    9: "Ninth",
    10: "Tenth",
}


def normalize_space(text: Any) -> str:
    if text is None:
        return ""
    value = str(text).strip()
    if value.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", value).strip()


def normalize_currency_text(text: Any) -> str:
    value = normalize_space(text)
    for bad in ("Â£", "Ã‚Â£", "Å", "Ł"):
        value = value.replace(bad, "£")
    return value


def normalize_key(text: str) -> str:
    value = unicodedata.normalize("NFKD", normalize_space(text).lower())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "-", value).strip("-")


def is_room_like(text: str) -> bool:
    return bool(ROOMISH_RE.search(normalize_space(text)))


def clean_room_name(raw: Any) -> str:
    text = normalize_currency_text(raw)
    text = ROOM_STRIP_RE.sub(" ", text)
    text = INCENTIVE_RE.sub(" ", text)
    text = re.sub(r"\b(availability|features|prices?|details?|offer|review|google|faq)\b", " ", text, flags=re.IGNORECASE)
    text = normalize_space(text)
    if len(text) > 80:
        return ""
    if not is_room_like(text):
        return ""
    return text


def parse_price_to_weekly_numeric(text: Any) -> Optional[float]:
    if text is None:
        return None
    if isinstance(text, (int, float)):
        try:
            return round(float(text), 2)
        except Exception:
            return None

    value = normalize_currency_text(text).lower()
    if not value:
        return None

    weekly = bool(re.search(r"\b(pw|p/w|per\s*week|weekly|/week)\b", value))
    monthly = bool(re.search(r"\b(pcm|per\s*month|monthly|/month)\b", value))
    if weekly and monthly:
        return None

    m = PRICE_TOKEN_RE.search(value)
    if not m:
        return None

    amount = re.sub(r"[^\d.]", "", m.group(0).replace(",", ""))
    if not amount:
        return None
    try:
        number = float(amount)
    except ValueError:
        return None

    if weekly:
        return round(number, 2)
    if monthly:
        return round((number * 12) / 52, 2)
    return None


def extract_contract_length(text: Any) -> str:
    m = CONTRACT_RE.search(normalize_space(text))
    return normalize_space(m.group(0)).upper() if m else ""


def normalise_academic_year(text: Any) -> str:
    m = ACADEMIC_YEAR_RE.search(normalize_space(text))
    if not m:
        return ""

    start = normalize_space(m.group(1))
    end = normalize_space(m.group(2))
    if len(start) == 2:
        start = f"20{start}"
    if len(end) == 4:
        end = end[2:]
    if len(end) == 1:
        end = f"0{end}"

    if not (start.isdigit() and len(start) == 4 and end.isdigit() and len(end) == 2):
        return ""

    start_year = int(start)
    expected_end = (start_year + 1) % 100
    if int(end) != expected_end:
        return ""
    return f"{start}/{end}"


def normalise_floor_level(text: Any) -> str:
    value = normalize_space(text)
    if not value:
        return ""
    low = value.lower()

    rng = re.search(r"(?:floors?|levels?)\s*(\d+)\s*(?:-|to)\s*(\d+)", low)
    if rng:
        a = FLOOR_NUM_WORD.get(int(rng.group(1)), "")
        b = FLOOR_NUM_WORD.get(int(rng.group(2)), "")
        return f"{a} to {b}" if a and b else ""

    if re.search(r"\blower\s+ground|\bground(?:\s*floor)?\b", low):
        return "Ground"

    n = re.search(r"(?:floor|level)?\s*(\d+)(?:st|nd|rd|th)?(?:\s*floor)?", low)
    if n:
        return FLOOR_NUM_WORD.get(int(n.group(1)), "")

    for word in ["first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth"]:
        if word in low:
            return word.title()
    return ""


def extract_and_normalise_incentives(*texts: Any) -> str:
    hay = " ".join(normalize_currency_text(t) for t in texts if normalize_space(t))
    found: List[str] = []
    for m in INCENTIVE_RE.finditer(hay):
        token = normalize_currency_text(m.group(0))
        if token and token.lower() not in [x.lower() for x in found]:
            found.append(token)

    out: List[str] = []
    for token in found:
        low = token.lower()
        if any(low in other.lower() and low != other.lower() for other in found):
            continue
        out.append(token)
    return " | ".join(out)


def infer_availability(text: Any) -> str:
    value = normalize_space(text).lower()
    if not value:
        return ""
    if re.search(r"sold out|fully booked|unavailable", value):
        return "Sold Out"
    if re.search(r"available|last few|selling fast", value):
        return "Available"
    return ""


def proper_case_property(value: str, fallback: str) -> str:
    text = normalize_space(value) or normalize_space(fallback)
    if not text:
        return ""
    if text.isupper() or text.lower() == text:
        return " ".join(part.capitalize() for part in re.split(r"\s+", text))
    return text


async def safe_goto(page: Page, url: str, timeout: int = 90000) -> bool:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        await page.wait_for_timeout(1500)
        return True
    except Exception:
        return False


async def click_common(page: Page) -> None:
    labels = [
        "Accept",
        "Accept All",
        "Allow all",
        "View Rooms",
        "Rooms",
        "Book Now",
        "Book",
        "View",
        "Show more",
        "Load more",
        "Check availability",
    ]
    for label in labels:
        try:
            loc = page.get_by_role("button", name=re.compile(re.escape(label), re.IGNORECASE))
            cnt = min(await loc.count(), 8)
            for i in range(cnt):
                btn = loc.nth(i)
                if await btn.is_visible():
                    await btn.click(timeout=1200)
                    await page.wait_for_timeout(150)
        except Exception:
            continue


async def parse_cards_by_selectors(page: Page, title_selectors: List[str], scope_selectors: List[str]) -> List[Dict[str, str]]:
    return await page.evaluate(
        r"""
        ({titleSelectors, scopeSelectors}) => {
          const uniq = new Set();
          const out = [];
          for (const scopeSel of scopeSelectors) {
            const scopes = document.querySelectorAll(scopeSel);
            for (const node of scopes) {
              const text = (node.innerText || '').replace(/\s+/g, ' ').trim();
              if (!text || text.length < 20 || text.length > 1000) continue;
              let title = '';
              for (const tSel of titleSelectors) {
                const t = node.querySelector(tSel);
                if (t && t.innerText) { title = t.innerText.replace(/\s+/g, ' ').trim(); break; }
              }
              const priceNode = node.querySelector('[class*="price"], .price, [data-testid*="price"]');
              const availabilityNode = node.querySelector('[class*="avail"], .available, .sold-out, [class*="status"]');
              const item = {
                title,
                text,
                price: (priceNode?.innerText || '').replace(/\s+/g, ' ').trim(),
                availability: (availabilityNode?.innerText || '').replace(/\s+/g, ' ').trim(),
              };
              const key = JSON.stringify(item);
              if (!uniq.has(key)) {
                uniq.add(key);
                out.push(item);
              }
            }
          }
          return out.slice(0, 500);
        }
        """,
        {"titleSelectors": title_selectors, "scopeSelectors": scope_selectors},
    )

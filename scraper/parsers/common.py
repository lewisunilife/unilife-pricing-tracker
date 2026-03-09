import re
import unicodedata
from typing import Any, Dict, List, Optional

from playwright.async_api import Page

CURRENCY_CHARS = r"[£Ł]"
MONEY_WITH_CURRENCY_RE = re.compile(rf"{CURRENCY_CHARS}\s*(\d{{2,7}}(?:,\d{{3}})*(?:\.\d{{1,2}})?)")
WEEKLY_PRICE_RE = re.compile(
    rf"{CURRENCY_CHARS}\s*(\d{{2,5}}(?:,\d{{3}})*(?:\.\d{{1,2}})?)\s*(?:pppw|ppw|pw|p/w|per\s*week|weekly|/week)\b",
    re.IGNORECASE,
)
MONTHLY_PRICE_RE = re.compile(
    rf"{CURRENCY_CHARS}\s*(\d{{2,5}}(?:,\d{{3}})*(?:\.\d{{1,2}})?)\s*(?:pcm|per\s*calendar\s*month|per\s*month|monthly|/month)\b",
    re.IGNORECASE,
)
WEEKLY_PRICE_NO_CURRENCY_RE = re.compile(
    r"\b(\d{2,5}(?:,\d{3})*(?:\.\d{1,2})?)\s*(?:pppw|ppw|pw|p/w|per\s*week|weekly|/week)\b",
    re.IGNORECASE,
)
MONTHLY_PRICE_NO_CURRENCY_RE = re.compile(
    r"\b(\d{2,5}(?:,\d{3})*(?:\.\d{1,2})?)\s*(?:pcm|per\s*calendar\s*month|per\s*month|monthly|/month)\b",
    re.IGNORECASE,
)

ROOMISH_RE = re.compile(
    r"\b(room|studio|suite|ensuite|en-suite|flat|apartment|bedroom|twodio|classic|premium|luxury|vip)\b",
    re.IGNORECASE,
)
CONTRACT_RE = re.compile(r"\b\d{1,3}\s*(?:weeks?|months?|days?)\b", re.IGNORECASE)
FLEXIBLE_STAY_RE = re.compile(r"\bflexible\s*stay\b", re.IGNORECASE)
ACADEMIC_YEAR_RE = re.compile(r"\b(?:AY\s*)?((?:20)?\d{2})\s*[/\-]\s*((?:20)?\d{2})\b", re.IGNORECASE)

INCENTIVE_PATTERNS = [
    r"plus\s+bookings\s+get\s+free\s+annual\s+bus\s+pass",
    r"book\s+today\s*(?:&|and)?\s*get\s*a?\s*free\s+kitchen\s*(?:&|and)\s*bedding\s+pack\s+worth\s*[\u00A3\u0141]?\s*\d+",
    r"kitchen\s*(?:&|and)\s*bedding\s+pack\s+worth\s*[\u00A3\u0141]?\s*\d+",
    r"[\u00A3\u0141]\s*\d+(?:[.,]\d{1,2})?\s*cashback",
    r"cashback",
    r"free\s+annual\s+bus\s+pass",
    r"free\s+bus\s+pass",
    r"free\s+laundry",
    r"refer\s+a\s+friend",
    r"bedding\s+pack(?:\s+included)?",
    r"kitchen\s+pack(?:\s+included|(?:\s+worth\s+[\u00A3\u0141]?\s*\d+)?)?",
    r"voucher",
    r"discount",
]
INCENTIVE_RE = re.compile("(" + "|".join(INCENTIVE_PATTERNS) + ")", re.IGNORECASE)

ROOM_STRIP_RE = re.compile(
    r"("
    + rf"{CURRENCY_CHARS}\s*\d{{2,5}}(?:[.,]\d{{1,2}})?\s*(?:pppw|ppw|pw|p/w|per week|weekly|pcm|per month|monthly)"
    + r"|\bview room\b|\bbook now\b|\bjoin waitlist\b|\bfrom\b|\bcontact us\b"
    + r"|\bavailable\b|\bsold out\b|\blimited availability\b"
    + r"|\b\d{1,2}\s*weeks?\b|\b\d+\s*sqm\b"
    + r")",
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
    bad_tokens = [
        "\u00c2\u00a3",
        "\u00c3\u00a2\u00c2\u00a3",
        "\u00c5\u0081",
        "\u0142",
        "\u0141",
    ]
    for bad in bad_tokens:
        value = value.replace(bad, "\u00a3")
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
    if len(text) > 100:
        return ""
    if not is_room_like(text):
        return ""
    return text


def _parse_amount(raw: str) -> Optional[float]:
    cleaned = raw.replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_price_to_weekly_numeric(text: Any) -> Optional[float]:
    if text is None:
        return None
    if isinstance(text, (int, float)):
        try:
            return round(float(text), 2)
        except Exception:
            return None

    value = normalize_currency_text(text)
    if not value:
        return None
    low = value.lower()

    weekly_hit = WEEKLY_PRICE_RE.search(value)
    if weekly_hit:
        amount = _parse_amount(weekly_hit.group(1))
        return round(amount, 2) if amount is not None else None

    monthly_hit = MONTHLY_PRICE_RE.search(value)
    if monthly_hit:
        amount = _parse_amount(monthly_hit.group(1))
        if amount is None:
            return None
        return round((amount * 12) / 52, 2)

    weekly_no_currency = WEEKLY_PRICE_NO_CURRENCY_RE.search(value)
    if weekly_no_currency:
        amount = _parse_amount(weekly_no_currency.group(1))
        return round(amount, 2) if amount is not None else None

    monthly_no_currency = MONTHLY_PRICE_NO_CURRENCY_RE.search(value)
    if monthly_no_currency:
        amount = _parse_amount(monthly_no_currency.group(1))
        if amount is None:
            return None
        return round((amount * 12) / 52, 2)

    has_weekly_marker = bool(re.search(r"\b(pppw|ppw|pw|p/w|per\s*week|weekly|/week)\b", low))
    has_monthly_marker = bool(re.search(r"\b(pcm|per\s*calendar\s*month|per\s*month|monthly|/month)\b", low))
    if has_weekly_marker or has_monthly_marker:
        return None

    bare = re.fullmatch(r"\d{2,5}(?:\.\d{1,2})?", low)
    if bare:
        amount = _parse_amount(bare.group(0))
        return round(amount, 2) if amount is not None else None
    return None


def parse_contract_value_numeric(text: Any) -> Optional[float]:
    value = normalize_currency_text(text)
    if not value:
        return None
    low = value.lower()
    if "total" not in low and "contract value" not in low and "contract rent" not in low:
        return None

    patterns = [
        re.compile(
            rf"(?:rent\s*[:\-]?\s*)?{CURRENCY_CHARS}\s*(\d{{2,7}}(?:,\d{{3}})*(?:\.\d{{1,2}})?)\s*(?:total(?:\s+for\s+the\s+contract)?|for\s+the\s+contract|contract\s+total)",
            re.IGNORECASE,
        ),
        re.compile(
            r"(?:rent\s*[:\-]?\s*)(\d{2,7}(?:,\d{3})*(?:\.\d{1,2})?)\s*(?:total(?:\s+for\s+the\s+contract)?|for\s+the\s+contract|contract\s+total)",
            re.IGNORECASE,
        ),
        re.compile(
            rf"(?:total(?:\s+rent)?|contract(?:\s+value|\s+total|\s+rent)?)\s*[:\-]?\s*{CURRENCY_CHARS}\s*(\d{{2,7}}(?:,\d{{3}})*(?:\.\d{{1,2}})?)",
            re.IGNORECASE,
        ),
        re.compile(
            r"(?:total(?:\s+rent)?|contract(?:\s+value|\s+total|\s+rent)?)\s*[:\-]?\s*(\d{2,7}(?:,\d{3})*(?:\.\d{1,2})?)",
            re.IGNORECASE,
        ),
    ]
    for pattern in patterns:
        hit = pattern.search(value)
        if hit:
            amount = _parse_amount(hit.group(1))
            if amount is not None:
                return round(amount, 2)
    return None


def extract_contract_length(text: Any) -> str:
    value = normalize_space(text)
    match = CONTRACT_RE.search(value)
    if match:
        return normalize_space(match.group(0)).upper()
    if re.search(r"\bsummer\b", value, flags=re.IGNORECASE):
        return "SUMMER"
    if FLEXIBLE_STAY_RE.search(value):
        return "FLEXIBLE STAY"
    return ""


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
        return "Unknown"
    if re.search(r"\b(waitlist|wait\s*list)\b", value):
        return "Waitlist"
    if re.search(r"\b(sold out|fully booked|booked out|no rooms left)\b", value):
        return "Sold Out"
    if re.search(r"\b(last few|selling fast|limited|limited availability|few remaining)\b", value):
        return "Limited Availability"
    if re.search(r"\b(unavailable|no availability|not available)\b", value):
        return "Unavailable"
    if re.search(r"\b(available|available from|book now|in stock)\b", value):
        return "Available"
    return "Unknown"


def classify_missing_price_reason(text: Any, availability: Any) -> str:
    avail = normalize_space(availability).lower()
    hay = normalize_space(text).lower()
    if avail == "sold out":
        return "sold_out"
    if avail == "unavailable":
        return "unavailable_no_contract_options"
    if re.search(r"\b(captcha|access denied|forbidden|cloudflare)\b", hay):
        return "blocked"
    if re.search(r"\b(pppw|ppw|pw|p/w|per week|weekly|pcm|monthly|per month)\b", hay):
        return "ambiguous_period"
    if re.search(r"\b(book|reserve|check availability|tenancy|contract)\b", hay):
        return "hidden_deeper_in_flow"
    return "not_shown_publicly"


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
        "Reserve a studio",
        "Choose room",
    ]
    for label in labels:
        try:
            loc = page.get_by_role("button", name=re.compile(re.escape(label), re.IGNORECASE))
            cnt = min(await loc.count(), 10)
            for i in range(cnt):
                btn = loc.nth(i)
                if await btn.is_visible():
                    await btn.click(timeout=1200)
                    await page.wait_for_timeout(160)
        except Exception:
            continue


async def parse_cards_by_selectors(page: Page, title_selectors: List[str], scope_selectors: List[str]) -> List[Dict[str, str]]:
    return await page.evaluate(
        r"""
        ({titleSelectors, scopeSelectors}) => {
          const uniq = new Set();
          const out = [];
          const bookingHints = ['book', 'reserve', 'availability', 'contract', 'tenancy', 'signing', 'portal', 'view-room', '/room', '/rooms'];
          for (const scopeSel of scopeSelectors) {
            const scopes = document.querySelectorAll(scopeSel);
            for (const node of scopes) {
              const text = (node.innerText || '').replace(/\s+/g, ' ').trim();
              if (!text || text.length < 20 || text.length > 1400) continue;
              let title = '';
              for (const tSel of titleSelectors) {
                const t = node.querySelector(tSel);
                if (t && t.innerText) { title = t.innerText.replace(/\s+/g, ' ').trim(); break; }
              }
              const priceNode = node.querySelector('[class*="price"], .price, [data-testid*="price"]');
              const availabilityNode = node.querySelector('[class*="avail"], .available, .sold-out, [class*="status"]');

              let bookingUrl = '';
              const anchors = node.querySelectorAll('a[href]');
              for (const a of anchors) {
                const href = (a.href || '').trim();
                const t = (a.innerText || '').replace(/\s+/g, ' ').trim().toLowerCase();
                const h = href.toLowerCase();
                if (!href) continue;
                if (bookingHints.some(k => t.includes(k) || h.includes(k))) {
                  bookingUrl = href;
                  break;
                }
              }

              const item = {
                title,
                text,
                price: (priceNode?.innerText || '').replace(/\s+/g, ' ').trim(),
                availability: (availabilityNode?.innerText || '').replace(/\s+/g, ' ').trim(),
                booking_url: bookingUrl,
              };
              const key = JSON.stringify(item);
              if (!uniq.has(key)) {
                uniq.add(key);
                out.push(item);
              }
            }
          }
          return out.slice(0, 800);
        }
        """,
        {"titleSelectors": title_selectors, "scopeSelectors": scope_selectors},
    )


async def collect_booking_links(
    page: Page,
    title_selectors: List[str],
    scope_selectors: List[str],
    max_links: int = 30,
) -> List[Dict[str, str]]:
    links = await page.evaluate(
        r"""
        ({titleSelectors, scopeSelectors}) => {
          const out = [];
          const seen = new Set();
          const bookingHints = ['book', 'reserve', 'availability', 'contract', 'tenancy', 'signing', 'portal', 'view-room', '/room', '/rooms'];

          const pushLink = (href, text, roomHint) => {
            if (!href) return;
            const key = href;
            if (seen.has(key)) return;
            seen.add(key);
            out.push({href, text, room_hint: roomHint || ''});
          };

          for (const scopeSel of scopeSelectors) {
            document.querySelectorAll(scopeSel).forEach(scope => {
              let roomHint = '';
              for (const tSel of titleSelectors) {
                const titleNode = scope.querySelector(tSel);
                if (titleNode && titleNode.innerText) {
                  roomHint = titleNode.innerText.replace(/\s+/g, ' ').trim();
                  break;
                }
              }
              scope.querySelectorAll('a[href]').forEach(a => {
                const href = (a.href || '').trim();
                const text = (a.innerText || '').replace(/\s+/g, ' ').trim();
                const lowHref = href.toLowerCase();
                const lowText = text.toLowerCase();
                if (bookingHints.some(k => lowHref.includes(k) || lowText.includes(k))) {
                  pushLink(href, text, roomHint);
                }
              });
            });
          }

          document.querySelectorAll('a[href]').forEach(a => {
            const href = (a.href || '').trim();
            const text = (a.innerText || '').replace(/\s+/g, ' ').trim();
            const lowHref = href.toLowerCase();
            const lowText = text.toLowerCase();
            if (bookingHints.some(k => lowHref.includes(k) || lowText.includes(k))) {
              pushLink(href, text, '');
            }
          });

          return out.slice(0, 200);
        }
        """,
        {"titleSelectors": title_selectors, "scopeSelectors": scope_selectors},
    )
    return links[:max_links]


async def parse_contract_rows_from_page(
    page: Page,
    source_url: str,
    room_hint: str = "",
    default_incentives: str = "",
) -> List[Dict[str, Any]]:
    await click_common(page)
    await page.wait_for_timeout(900)
    body = normalize_currency_text(await page.inner_text("body"))
    page_ay = normalise_academic_year(body)
    availability_default = infer_availability(body)

    room_name = clean_room_name(room_hint)
    if not room_name:
        heading = await page.evaluate(
            r"""
            () => {
              const nodes = [...document.querySelectorAll('h1,h2,h3,.title,[class*="room"]')];
              for (const n of nodes) {
                const t = (n.innerText || '').replace(/\s+/g, ' ').trim();
                if (t && t.length < 120) return t;
              }
              return '';
            }
            """
        )
        room_name = clean_room_name(heading)

    option_texts: List[str] = await page.evaluate(
        r"""
        () => {
          const out = [];
          const seen = new Set();
          const sel = [
            'label',
            '[id^="availability-"]',
            '[class*="contract"]',
            '[class*="tenancy"]',
            '[class*="availability"]',
          ].join(',');
          for (const node of document.querySelectorAll(sel)) {
            const text = (node.innerText || '').replace(/\s+/g, ' ').trim();
            if (!text || text.length < 8 || text.length > 700) continue;
            if (!/(week|pppw|pw|p\/w|per week|pcm|monthly|available from|sold out|waitlist|contract|tenancy|flexible)/i.test(text)) continue;
            if (!seen.has(text)) {
              seen.add(text);
              out.push(text);
            }
          }
          return out.slice(0, 120);
        }
        """
    )

    rows: List[Dict[str, Any]] = []
    for text in option_texts:
        contract = extract_contract_length(text)
        ay = normalise_academic_year(text) or page_ay
        price = parse_price_to_weekly_numeric(text)
        contract_value = parse_contract_value_numeric(text)
        availability = infer_availability(text)
        incentives = extract_and_normalise_incentives(text, default_incentives, body)
        if not any([contract, ay, price is not None, contract_value is not None, incentives, availability != "Unknown"]):
            continue
        rows.append(
            {
                "Room Name": room_name,
                "Contract Length": contract,
                "Academic Year": ay,
                "Price": price,
                "Contract Value": contract_value,
                "Floor Level": normalise_floor_level(text),
                "Incentives": incentives,
                "Availability": availability if availability else availability_default,
                "Source URL": page.url or source_url,
                "__missing_price_reason": classify_missing_price_reason(text, availability),
            }
        )

    if rows:
        deduped: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            key = (
                normalize_space(row.get("Room Name", "")),
                normalize_space(row.get("Contract Length", "")),
                normalize_space(row.get("Academic Year", "")),
                row.get("Price"),
                row.get("Contract Value"),
                normalize_space(row.get("Availability", "")),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    # Fallback: line-based parse for pages that render options as plain text.
    for line in [normalize_space(x) for x in body.splitlines() if normalize_space(x)]:
        if not re.search(r"(pppw|ppw|pw|per week|weekly|pcm|monthly)", line, flags=re.IGNORECASE):
            continue
        price = parse_price_to_weekly_numeric(line)
        if price is None:
            continue
        rows.append(
            {
                "Room Name": room_name,
                "Contract Length": extract_contract_length(line),
                "Academic Year": normalise_academic_year(line) or page_ay,
                "Price": price,
                "Contract Value": parse_contract_value_numeric(line),
                "Floor Level": normalise_floor_level(line),
                "Incentives": extract_and_normalise_incentives(line, default_incentives),
                "Availability": infer_availability(line) or availability_default,
                "Source URL": page.url or source_url,
            }
        )
    return rows


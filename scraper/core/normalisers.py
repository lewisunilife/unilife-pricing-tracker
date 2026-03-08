import re
from typing import Any, Iterable, List, Optional


CURRENCY_CHARS = r"[\u00A3\u0141]"
MONEY_WITH_CURRENCY_RE = re.compile(rf"{CURRENCY_CHARS}\s*(\d{{2,5}}(?:,\d{{3}})*(?:\.\d{{1,2}})?)")
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

ACADEMIC_RE = re.compile(r"\b(?:AY\s*)?((?:20)?\d{2})\s*[/\-]\s*((?:20)?\d{2})\b", re.IGNORECASE)
CONTRACT_RE = re.compile(r"\b\d{1,2}\s*(?:weeks?|months?)\b", re.IGNORECASE)
FLEXIBLE_STAY_RE = re.compile(r"\bflexible\s*stay\b", re.IGNORECASE)

CTA_RE = re.compile(
    r"\b(view room|book now|join waitlist|reserve a studio|check availability|from|contact us|select room|choose room|apply now)\b",
    re.IGNORECASE,
)
INCENTIVE_RE = re.compile(
    r"(premium plus bookings get free annual bus pass|plus bookings get free annual bus pass|book today\s*(?:&|and)?\s*get\s*a?\s*free\s*kitchen\s*(?:&|and)\s*bedding\s+pack\s+worth\s*[\u00A3\u0141]?\s*\d+|kitchen\s*(?:&|and)\s*bedding\s+pack\s+worth\s*[\u00A3\u0141]?\s*\d+|[\u00A3\u0141]\s*\d+(?:[.,]\d{1,2})?\s*cashback|cashback|free\s+annual\s+bus\s+pass|free\s+bus\s+pass|free\s+laundry|bedding\s+pack(?:\s+included)?|kitchen\s+pack(?:\s+included)?|voucher|discount|offer)",
    re.IGNORECASE,
)

FLOOR_WORDS = {
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


def normalize_space(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", text).strip()


def normalize_currency(value: Any) -> str:
    text = normalize_space(value)
    bad_tokens = [
        "\u00c2\u00a3",
        "\u00c3\u00a2\u00c2\u00a3",
        "\u00c5\u0081",
        "\u0142",
        "\u0141",
    ]
    for bad in bad_tokens:
        text = text.replace(bad, "\u00a3")
    return text


def _parse_amount(raw: str) -> Optional[float]:
    cleaned = raw.replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_price_to_weekly_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return round(float(value), 2)
        except Exception:
            return None

    text = normalize_currency(value)
    if not text:
        return None
    low = text.lower()

    weekly_hit = WEEKLY_PRICE_RE.search(text)
    if weekly_hit:
        amount = _parse_amount(weekly_hit.group(1))
        return round(amount, 2) if amount is not None else None

    monthly_hit = MONTHLY_PRICE_RE.search(text)
    if monthly_hit:
        amount = _parse_amount(monthly_hit.group(1))
        if amount is None:
            return None
        return round((amount * 12) / 52, 2)

    weekly_no_currency = WEEKLY_PRICE_NO_CURRENCY_RE.search(text)
    if weekly_no_currency:
        amount = _parse_amount(weekly_no_currency.group(1))
        return round(amount, 2) if amount is not None else None

    monthly_no_currency = MONTHLY_PRICE_NO_CURRENCY_RE.search(text)
    if monthly_no_currency:
        amount = _parse_amount(monthly_no_currency.group(1))
        if amount is None:
            return None
        return round((amount * 12) / 52, 2)

    # If period marker exists but no currency-tied value, treat as ambiguous.
    has_weekly_marker = bool(re.search(r"\b(pppw|ppw|pw|p/w|per\s*week|weekly|/week)\b", low))
    has_monthly_marker = bool(re.search(r"\b(pcm|per\s*calendar\s*month|per\s*month|monthly|/month)\b", low))
    if has_weekly_marker or has_monthly_marker:
        return None

    # Bare numeric fallback only when the whole value is numeric.
    bare = re.fullmatch(r"\d{2,5}(?:\.\d{1,2})?", low)
    if bare:
        amount = _parse_amount(bare.group(0))
        return round(amount, 2) if amount is not None else None
    return None


def parse_contract_value_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return round(float(value), 2)
        except Exception:
            return None

    text = normalize_currency(value)
    if not text:
        return None
    low = text.lower()
    if "total" not in low and "contract value" not in low and "contract rent" not in low:
        return None

    patterns = [
        re.compile(
            rf"(?:rent\s*[:\-]?\s*)?{CURRENCY_CHARS}\s*(\d{{2,7}}(?:,\d{{3}})*(?:\.\d{{1,2}})?)\s*(?:total(?:\s+for\s+the\s+contract)?|for\s+the\s+contract|contract\s+total)",
            re.IGNORECASE,
        ),
        re.compile(
            rf"(?:total(?:\s+rent)?|contract(?:\s+value|\s+total|\s+rent)?)\s*[:\-]?\s*{CURRENCY_CHARS}\s*(\d{{2,7}}(?:,\d{{3}})*(?:\.\d{{1,2}})?)",
            re.IGNORECASE,
        ),
    ]

    for pattern in patterns:
        hit = pattern.search(text)
        if not hit:
            continue
        amount = _parse_amount(hit.group(1))
        if amount is not None:
            return round(amount, 2)
    return None


def normalise_academic_year(value: Any) -> str:
    text = normalize_space(value)
    if not text:
        return ""
    m = ACADEMIC_RE.search(text)
    if not m:
        return ""
    start = m.group(1)
    end = m.group(2)
    if len(start) == 2:
        start = f"20{start}"
    if len(end) == 4:
        end = end[2:]
    if len(end) == 1:
        end = f"0{end}"
    if not (start.isdigit() and len(start) == 4 and end.isdigit() and len(end) == 2):
        return ""
    start_i = int(start)
    expected = (start_i + 1) % 100
    if int(end) != expected:
        return ""
    return f"{start}/{end}"


def validate_academic_year(value: Any) -> bool:
    return bool(normalise_academic_year(value))


def _floor_word(num: int) -> str:
    return FLOOR_WORDS.get(num, "")


def normalise_floor_level(value: Any) -> str:
    text = normalize_space(value).lower()
    if not text:
        return ""

    if re.search(r"\blower\s+ground|\bground(?:\s*floor)?\b", text):
        return "Ground"

    range_match = re.search(
        r"(?:floors?|levels?)\s*(\d+)\s*(?:-|to)\s*(\d+)|(\d+)(?:st|nd|rd|th)?\s*floor\s*(?:-|to)\s*(\d+)(?:st|nd|rd|th)?\s*floor",
        text,
    )
    if range_match:
        left = range_match.group(1) or range_match.group(3)
        right = range_match.group(2) or range_match.group(4)
        if left and right:
            a = _floor_word(int(left))
            b = _floor_word(int(right))
            if a and b:
                return f"{a} to {b}"

    num_match = re.search(r"(?:level|floor)?\s*(\d+)(?:st|nd|rd|th)?(?:\s*floor)?", text)
    if num_match:
        floor = _floor_word(int(num_match.group(1)))
        if floor:
            return floor

    for word in ("first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth"):
        if word in text:
            return word.title()
    return ""


def clean_property_name(value: Any) -> str:
    text = normalize_space(value)
    if not text:
        return ""
    text = re.sub(r"\bunilife\b", "", text, flags=re.IGNORECASE)
    text = normalize_space(text)
    if text.isupper() or text.lower() == text:
        return " ".join(part.capitalize() for part in re.split(r"\s+", text))
    return text


def clean_room_name(value: Any) -> str:
    text = normalize_currency(value)
    if not text:
        return ""
    text = CTA_RE.sub(" ", text)
    text = INCENTIVE_RE.sub(" ", text)
    text = re.sub(rf"{CURRENCY_CHARS}\s*\d{{2,5}}(?:[.,]\d{{1,2}})?\s*(?:pppw|ppw|pw|p/w|per week|weekly|pcm|monthly)?", " ", text, flags=re.IGNORECASE)
    text = CONTRACT_RE.sub(" ", text)
    text = re.sub(r"\b\d+\s*sqm\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d{1,2}\s*[/-]\s*\d{1,2}\b", " ", text)
    text = normalize_space(text)
    if not text:
        return ""
    if len(text) > 100:
        return ""
    return text


def extract_and_assign_incentives(
    room_name: str,
    booking_text: str = "",
    room_text: str = "",
    property_text: str = "",
) -> str:
    joined = " ".join(normalize_currency(x) for x in [booking_text, room_text, property_text] if normalize_space(x))
    tokens: List[str] = []
    for m in INCENTIVE_RE.finditer(joined):
        token = normalize_currency(m.group(0))
        if token.lower() not in [t.lower() for t in tokens]:
            tokens.append(token)

    room_low = normalize_space(room_name).lower()
    scoped: List[str] = []
    for token in tokens:
        low = token.lower()
        if "premium plus" in low and "premium plus" not in room_low:
            continue
        if "bus pass" in low and "premium plus" not in room_low:
            continue
        if ("twodio" in low or "kitchen & bedding pack" in low) and "twodio" not in room_low:
            continue
        scoped.append(token)
    return " | ".join(scoped)


def normalise_availability(value: Any) -> str:
    text = normalize_space(value).lower()
    if not text:
        return "Unknown"
    if re.search(r"\b(waitlist|wait\s*list)\b", text):
        return "Waitlist"
    if re.search(r"\b(sold out|fully booked|no rooms left|booked out)\b", text):
        return "Sold Out"
    if re.search(r"\b(last few|selling fast|limited|limited availability|few remaining)\b", text):
        return "Limited Availability"
    if re.search(r"\b(unavailable|no availability|not available)\b", text):
        return "Unavailable"
    if re.search(r"\b(available|book now|available from|in stock)\b", text):
        return "Available"
    return "Unknown"


def extract_contract_length(value: Any) -> str:
    text = normalize_space(value)
    match = CONTRACT_RE.search(text)
    if match:
        return normalize_space(match.group(0)).upper()
    if FLEXIBLE_STAY_RE.search(text):
        return "FLEXIBLE STAY"
    return ""


def unique_join(values: Iterable[str]) -> str:
    out: List[str] = []
    for value in values:
        token = normalize_space(value)
        if token and token.lower() not in [x.lower() for x in out]:
            out.append(token)
    return " | ".join(out)

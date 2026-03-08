import re
from typing import Any, Iterable, List, Optional


PRICE_RE = re.compile(r"[£Ł]?\s*\d{2,4}(?:,\d{3})*(?:\.\d{1,2})?")
ACADEMIC_RE = re.compile(r"\b(?:AY\s*)?((?:20)?\d{2})\s*[/\-]\s*((?:20)?\d{2})\b", re.IGNORECASE)
CONTRACT_RE = re.compile(r"\b\d{1,2}\s*(?:weeks?|months?)\b", re.IGNORECASE)
CTA_RE = re.compile(
    r"\b(view room|book now|join waitlist|reserve a studio|check availability|available|sold out|from)\b",
    re.IGNORECASE,
)
INCENTIVE_RE = re.compile(
    r"(premium plus bookings get free annual bus pass|plus bookings get free annual bus pass|book today\s*(?:&|and)?\s*get\s*a?\s*free\s*kitchen\s*(?:&|and)\s*bedding\s+pack\s+worth\s*[£Ł]?\s*\d+|kitchen\s*(?:&|and)\s*bedding\s+pack\s+worth\s*[£Ł]?\s*\d+|[£Ł]\s*\d+(?:[.,]\d{1,2})?\s*cashback|cashback|free\s+annual\s+bus\s+pass|free\s+bus\s+pass|bedding\s+pack(?:\s+included)?|kitchen\s+pack(?:\s+included)?|voucher|discount)",
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
    for bad in ("Â£", "Ã‚Â£", "Å", "Ł"):
        text = text.replace(bad, "£")
    return text


def parse_price_to_weekly_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return round(float(value), 2)
        except Exception:
            return None

    text = normalize_currency(value).lower()
    if not text:
        return None

    weekly = bool(re.search(r"\b(pw|p/w|per\s*week|weekly|/week)\b", text))
    monthly = bool(re.search(r"\b(pcm|per\s*month|monthly|/month)\b", text))
    if weekly and monthly:
        return None

    m = PRICE_RE.search(text)
    if not m:
        return None
    raw = re.sub(r"[^\d.]", "", m.group(0).replace(",", ""))
    if not raw:
        return None
    try:
        amount = float(raw)
    except ValueError:
        return None

    if weekly:
        return round(amount, 2)
    if monthly:
        return round((amount * 12) / 52, 2)
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


def normalise_floor_level(value: Any) -> str:
    text = normalize_space(value).lower()
    if not text:
        return ""
    if re.search(r"\blower\s+ground|ground(?:\s*floor)?\b", text):
        return "Ground"

    m_range = re.search(r"(?:floors?|levels?)\s*(\d+)\s*(?:-|to)\s*(\d+)", text)
    if m_range:
        a = FLOOR_WORDS.get(int(m_range.group(1)), "")
        b = FLOOR_WORDS.get(int(m_range.group(2)), "")
        return f"{a} to {b}" if a and b else ""

    m_num = re.search(r"(?:floor|level)?\s*(\d+)(?:st|nd|rd|th)?(?:\s*floor)?", text)
    if m_num:
        return FLOOR_WORDS.get(int(m_num.group(1)), "")

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
    text = re.sub(r"[£Ł]\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|per week|weekly|pcm|monthly)?", " ", text, flags=re.IGNORECASE)
    text = CONTRACT_RE.sub(" ", text)
    text = normalize_space(text)
    if not text:
        return ""
    if len(text) > 80:
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
        return ""
    if re.search(r"sold out|fully booked|unavailable", text):
        return "Sold Out"
    if re.search(r"available|last few|selling fast", text):
        return "Available"
    return normalize_space(value)


def extract_contract_length(value: Any) -> str:
    m = CONTRACT_RE.search(normalize_space(value))
    return normalize_space(m.group(0)).upper() if m else ""


def unique_join(values: Iterable[str]) -> str:
    out: List[str] = []
    for value in values:
        token = normalize_space(value)
        if token and token.lower() not in [x.lower() for x in out]:
            out.append(token)
    return " | ".join(out)

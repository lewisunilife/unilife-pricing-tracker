import hashlib
import re
import unicodedata


def slugify(value: str) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


def hall_id(operator: str, property_name: str) -> str:
    op = slugify(operator)
    prop = slugify(property_name)
    if not op or not prop:
        return ""
    digest = hashlib.sha1(f"{op}|{prop}".encode("utf-8")).hexdigest()[:8]
    return f"hall-{op}-{prop}-{digest}"


def room_id(operator: str, property_name: str, room_name: str) -> str:
    op = slugify(operator)
    prop = slugify(property_name)
    room = slugify(room_name)
    if not op or not prop or not room:
        return ""
    digest = hashlib.sha1(f"{op}|{prop}|{room}".encode("utf-8")).hexdigest()[:8]
    return f"room-{op}-{prop}-{room}-{digest}"

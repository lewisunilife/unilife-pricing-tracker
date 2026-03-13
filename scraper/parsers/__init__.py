from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Tuple

from . import (
    abodus_parser,
    canvas_parser,
    capitol_parser,
    collegiate_parser,
    crm_parser,
    every_student_parser,
    hello_student_parser,
    homes_for_students_parser,
    host_parser,
    host_students_parser,
    mezzino_parser,
    now_students_parser,
    prestige_parser,
    prestige_student_living_parser,
    student_roost_parser,
    unilife_parser,
    unite_parser,
    vita_parser,
    yugo_parser,
)

ParserFn = Callable[[Any, Dict[str, str]], Awaitable[Tuple[list, str]]]


@dataclass
class ParserAdapter:
    name: str
    parse_dom: ParserFn
    parse_interactive: ParserFn
    skip_generic_api_detection: bool = False


def _adapter(name: str, fn: ParserFn, skip_generic_api_detection: bool = False) -> ParserAdapter:
    # Existing operator parsers already use Playwright interactions internally.
    return ParserAdapter(
        name=name,
        parse_dom=fn,
        parse_interactive=fn,
        skip_generic_api_detection=skip_generic_api_detection,
    )


def _adapter_pair(
    name: str,
    dom_fn: ParserFn,
    interactive_fn: ParserFn,
    skip_generic_api_detection: bool = False,
) -> ParserAdapter:
    return ParserAdapter(
        name=name,
        parse_dom=dom_fn,
        parse_interactive=interactive_fn,
        skip_generic_api_detection=skip_generic_api_detection,
    )


ADAPTERS: Dict[str, ParserAdapter] = {
    "abodus": _adapter("abodus", abodus_parser.parse),
    "canvas": _adapter("canvas", canvas_parser.parse),
    "capitol": _adapter("capitol", capitol_parser.parse),
    "collegiate": _adapter("collegiate", collegiate_parser.parse),
    "crm": _adapter("crm", crm_parser.parse),
    "every_student": _adapter("every_student", every_student_parser.parse),
    "hello_student": _adapter("hello_student", hello_student_parser.parse),
    "homes_for_students": _adapter("homes_for_students", homes_for_students_parser.parse),
    "host": _adapter("host", host_parser.parse),
    "mezzino": _adapter("mezzino", mezzino_parser.parse),
    "now_students": _adapter("now_students", now_students_parser.parse),
    "prestige": _adapter("prestige", prestige_parser.parse),
    "student_roost": _adapter("student_roost", student_roost_parser.parse),
    "unilife": _adapter_pair("unilife", unilife_parser.parse_dom, unilife_parser.parse_interactive),
    "unite": _adapter("unite", unite_parser.parse, skip_generic_api_detection=True),
    "vita": _adapter("vita", vita_parser.parse),
    "yugo": _adapter("yugo", yugo_parser.parse),
}


def get_adapter(name: str) -> ParserAdapter | None:
    return ADAPTERS.get((name or "").strip().lower())

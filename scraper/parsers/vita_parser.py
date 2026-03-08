from typing import Dict, List, Tuple
from playwright.async_api import Page
from .base import parse_with_selector_plan


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, str]], str]:
    return await parse_with_selector_plan(
        page,
        src,
        title_selectors=['h3','h4','.room-title','.title','[data-testid*="room"]'],
        scope_selectors=['.room-card','[class*="room"]','article','[class*="booking"]'],
    )

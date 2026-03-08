from typing import Dict, List, Tuple
from playwright.async_api import Page
from .base import parse_with_selector_plan


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, str]], str]:
    return await parse_with_selector_plan(
        page,
        src,
        title_selectors=['[data-testid*="room"] h3','h3.room-card__title','h3','.room-title','.title'],
        scope_selectors=['[data-testid*="room-card"]','.room-card','[class*="room-card"]','article'],
    )

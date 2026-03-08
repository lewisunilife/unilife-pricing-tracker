import re

from playwright.async_api import Page


async def safe_goto(page: Page, url: str, timeout: int = 90000) -> tuple[bool, str]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        await page.wait_for_timeout(1400)
        return True, ""
    except Exception as exc:
        return False, str(exc)


async def click_common(page: Page) -> None:
    labels = [
        "Accept",
        "Accept All",
        "Allow all",
        "View Rooms",
        "Rooms",
        "Book Now",
        "Book",
        "Show more",
        "Load more",
        "Check availability",
        "Reserve a studio",
    ]
    for label in labels:
        try:
            loc = page.get_by_role("button", name=re.compile(re.escape(label), re.IGNORECASE))
            cnt = min(await loc.count(), 8)
            for i in range(cnt):
                button = loc.nth(i)
                if await button.is_visible():
                    await button.click(timeout=1200)
                    await page.wait_for_timeout(140)
        except Exception:
            continue

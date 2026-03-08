import re
from typing import Dict, List, Tuple
from playwright.async_api import Page
from . import common


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, str]], str]:
    await common.click_common(page)
    body = await page.inner_text('body')
    ay = common.normalise_academic_year(body)
    rows: List[Dict[str, str]] = []
    for room, amt in re.findall(r'(En-suite Rooms|Studio Rooms)\s+from\s+[£Ł]?\s*(\d{2,4}(?:[.,]\d{1,2})?)\s+per\s+week', body, flags=re.IGNORECASE):
        rows.append({
            'Room Name': common.clean_room_name(room) or room.title(),
            'Contract Length': common.extract_contract_length(body),
            'Price': common.parse_price_to_weekly_numeric(f'£{amt} per week'),
            'Floor Level': '',
            'Academic Year': ay,
            'Incentives': common.extract_and_normalise_incentives(body),
            'Availability': common.infer_availability(body),
            'Source URL': src['url'],
        })
    if rows:
        return rows, ''
    return [], 'no extractable room rows'

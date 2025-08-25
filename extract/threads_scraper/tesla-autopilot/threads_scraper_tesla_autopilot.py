# 스크롤 기능 구현.
import json
import re
import os
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Set
from playwright.sync_api import sync_playwright


# DOM에서 관련 항목을 수집하기 위한 SELECTOR 정의
_CARD_SELECTOR = "div[data-pressable-container='true']:has(time[datetime])"
_TEXT_CONTAINER_SELECTOR = "div.x1a6qonq"
_USERNAME_SELECTOR = "a[href^='/@'] span, a[href^='/@']"
_TIME_SELECTOR = "time[datetime]"

# graphql 형식 정의
_GRAPHQL_URL_RE = re.compile(r"graphql", re.I)

# 상대시간에 대한 토큰 형식 정규식
_REL_TIME_PREFIX_RE = re.compile(r"^\s*(\d+\s*(분|시간|일)|\d+\s*phút|\d+\s*giờ|\d+\s*ngày)\s*$")


def _iso_to_epoch_s(iso: str) -> Optional[int]:
    if not iso:
        return None
    try:
        s = iso.strip()
        # Z → +00:00 보정
        if s.endswith(("Z", "z")):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        # tz 없는 ISO면 UTC로 가정
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        # UTC로 정규화 후 epoch(초)
        return int(dt.astimezone(timezone.utc).timestamp())
    except Exception:
        return None
    

def _clean_text(text: str, username: Optional[str]) -> str:
    """
    수집된 텍스트에 최소 전처리 수행.
    텍스트에 username이 삽입 되는 경우 제거.
    텍스트를 줄 단위로 쪼개고 앞뒤공백 제거.
    """
    if not text:
        return text
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    cleaned: List[str] = []
    for line in lines:
        orig = line
        if username:
            tokens = line.split()
            cut_idx = 0
            while cut_idx < len(tokens) and tokens[cut_idx] == username:
                cut_idx += 1
            if cut_idx > 0:
                line = " ".join(tokens[cut_idx:]).strip()
        if _REL_TIME_PREFIX_RE.fullmatch(line):
            line = ""
        if line:
            cleaned.append(line)
        if orig != line:
            print(f"[clean_text] '{orig[:30]}' -> '{line[:30]}'")
    return "\n".join(cleaned).strip()


def _extract_text(container) -> str:
    """
    Playwright를 이용하여 특정 DOM에서 본문 텍스트만 추출.
    """
    if not container:
        return ""
    lines: List[str] = container.eval_on_selector_all(
        ":scope > span > span",
        "nodes => nodes.map(n => (n.textContent || '').trim()).filter(Boolean)"
    )
    return "\n".join(lines).strip()


def _parse_dom(page) -> List[Dict]:
    """
    메인 크롤러.
    Playwright의 page에서 DOM을 읽어오고, 게시물을 정리된 딕셔너리 형태로 반환.
    """
    print("[parse_dom] DOM에서 게시물 파싱 시작")
    # CARD_SELECTOR 로 정의된 CSS 선택자에 맞는 모든 DOM 요소들을 탐색
    cards = page.query_selector_all(_CARD_SELECTOR)
    print(f"[parse_dom] 카드 후보 수: {len(cards)}")

    results: List[Dict] = []
    seen_keys: set[Optional[str]] = set()  # uploaded_time 기준 중복 제거

    # 루프를 돌며 각 게시물(card) 처리
    for i, card in enumerate(cards):
        try:
            username_el = card.query_selector(_USERNAME_SELECTOR)
            username = (username_el.inner_text().strip() if username_el else None)

            time_el = card.query_selector(_TIME_SELECTOR)
            uploaded_time = time_el.get_attribute("datetime") if time_el else None

            text_container = card.query_selector(_TEXT_CONTAINER_SELECTOR)
            raw_text = _extract_text(text_container)
            # 텍스트 기본 전처리
            text = _clean_text(raw_text, username)

            # 값이 없거나 중복 수집된 결과는 skip
            if not (username or text):
                continue
            
            if uploaded_time in seen_keys:
                print(f"[parse_dom] 중복 스킵 #{i} time={uploaded_time}")
                continue
            seen_keys.add(uploaded_time)

            item = {
                "username": username,
                "collected_time": int(datetime.now(timezone.utc).timestamp()), 
                "uploaded_time": _iso_to_epoch_s(uploaded_time),
                "channel": "threads",
                "text": text
            }
            results.append(item)

        except Exception as e:
            print(f"[parse_dom] error at card {i}: {e}")

    print(f"[parse_dom] 추출된 카드 수: {len(results)}")
    return results


def _scroll_once(page):
    before = len(page.query_selector_all(_CARD_SELECTOR))
    page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
    page.wait_for_timeout(random.randint(8000, 15000))  # 8~15초 대기
    after = len(page.query_selector_all(_CARD_SELECTOR))
    return after - before


def _parse_date_to_epoch(date_str: Optional[str]) -> Optional[int]:
    """
    YYYY-MM-DD 형식 문자열을 epoch(초)로 변환.
    UTC 기준 00:00:00 시각을 기준으로 함.
    """
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def _infinite_scroll_collect(page,
                            #  max_items: int = 200,
                             start_date: Optional[str] = None,
                             end_date: Optional[str] = None,
                             buffer_days: int = 3,
                             patience_rounds: int = 3) -> List[Dict]:
    collected = {}
    consecutive_no_dom_no_growth = 0
    no_in_range_rounds = 0
    saw_in_range_post = False  # ✅ 범위 내 게시물이 나온 적 있는지

    start_epoch = _parse_date_to_epoch(start_date)
    end_epoch = _parse_date_to_epoch(end_date)
    cutoff_epoch = start_epoch - buffer_days * 86400 if start_epoch else None

    round_num = 0
    while True:
        round_num += 1
        page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
        delay = random.randint(15000, 25000)
        print(f"[INFO] 스크롤 후 {delay/1000:.1f}초 대기")
        page.wait_for_timeout(delay)

        batch = _parse_dom(page)
        added_in_range = 0
        stop_due_to_cutoff = False

        for item in batch:
            up_epoch = item.get("uploaded_time")
            if up_epoch is None:
                continue

            # end_date 이후 → skip
            if end_epoch and up_epoch >= end_epoch:
                continue

            # start_date 이전
            if start_epoch and up_epoch < start_epoch:
                if cutoff_epoch and up_epoch < cutoff_epoch:
                    print("[scroll] start_date-버퍼 이전 게시물 발견 → 종료 후보")
                    stop_due_to_cutoff = True
                continue

            # ✅ 범위 내 게시물 수집
            key = (up_epoch, item.get("username"))
            if key not in collected:
                collected[key] = item
                added_in_range += 1
                saw_in_range_post = True  # ✅ 최소 한 번이라도 확보됨

            # if len(collected) >= max_items:
            #     print(f"[collect] round={round_num}, 이번={added_in_range}, 누적={len(collected)}")
            #     return list(collected.values())

        print(f"[collect] round={round_num}, 이번={added_in_range}, 누적={len(collected)}")

        # 범위 내 새 게시물이 없을 경우 처리
        if added_in_range == 0:
            if saw_in_range_post:  # ✅ 이미 최소 하나 확보된 상태에서만 patience 카운트
                no_in_range_rounds += 1
        else:
            no_in_range_rounds = 0

        delta = _scroll_once(page)
        if delta <= 0:
            consecutive_no_dom_no_growth += 1
        else:
            consecutive_no_dom_no_growth = 0

        # 종료 조건
        if stop_due_to_cutoff and saw_in_range_post:  
            # ✅ 최소 1개 확보 전에는 종료하지 않음
            print("[scroll] start_date-버퍼 이전 게시물 도달 → 종료")
            break
        if saw_in_range_post and no_in_range_rounds >= patience_rounds:
            print(f"[scroll] 범위 내 새 게시물 {patience_rounds}번 연속 없음 → 종료")
            break
        if saw_in_range_post and consecutive_no_dom_no_growth >= 3:
            # ✅ 최소 1개 확보 후에만 DOM 증가 없음 종료
            print("[scroll] DOM 증가 없음 → 종료")
            break

        page.wait_for_timeout(1000)

    return list(collected.values())



def scrape_search(keyword: str,
                #   max_items: int,
                  start_date: str, end_date: str, cookies_override: Optional[List[dict]] = None) -> dict:
    """
    키워드 검색 크롤링 (시간 제한 없음, 최대 max_items까지).
    """
    if cookies_override is None:
        raise RuntimeError("쿠키 로드 실패")

    with sync_playwright() as pw:
        print("[scrape_search] 브라우저 실행")
        browser = pw.chromium.launch(headless=False, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.0.0 Safari/537.36"),
            locale="en-US",   # 언어
            timezone_id="America/New_York",   # 미국 시간대 (NY 기준)
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"}
        )
        context.add_cookies(cookies_override)
        page = context.new_page()

        url = f"https://www.threads.com/search?q={keyword}&serp_type=default&filter=recent&hl=en"
        print("[scrape_search] 이동:", url)
        page.goto(url, wait_until="domcontentloaded")

        page.wait_for_selector("a[href^='/@'] span", timeout=15000)
        page.wait_for_selector("time[datetime]", timeout=15000)

        results = _infinite_scroll_collect(
            page,
            # max_items=max_items,
            start_date=start_date,
            end_date=end_date
        )

        browser.close()
        return {"threads": results}

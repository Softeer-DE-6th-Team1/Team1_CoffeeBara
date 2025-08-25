import re
import random
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from playwright.sync_api import sync_playwright

# DOM에서 관련 항목을 수집하기 위한 SELECTOR 정의
_CARD_SELECTOR = "div[data-pressable-container='true']:has(time[datetime])"
_TEXT_CONTAINER_SELECTOR = "div.x1a6qonq"
_USERNAME_SELECTOR = "a[href^='/@'] span, a[href^='/@']"
_TIME_SELECTOR = "time[datetime]"

# 상대시간에 대한 토큰 형식 정규식
_REL_TIME_PREFIX_RE = re.compile(r"^\s*(\d+\s*(분|시간|일)|\d+\s*phút|\d+\s*giờ|\d+\s*ngày)\s*$")


def _iso_to_epoch_s(iso: str) -> Optional[int]:
    """
    ISO8601 형식 문자열을 UTC 기준 epoch(초 단위)로 변환한다.
    
    Args:
        iso (str): ISO8601 datetime 문자열

    Returns:
        Optional[int]: epoch time(초). 실패하면 None.
    """
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
    

def _epoch_to_iso(epoch: Optional[int]) -> str:
    if not epoch:
        return ""
    try:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).replace(microsecond=0).isoformat()
    except Exception:
        return ""


def _clean_text(text: str, username: Optional[str]) -> str:
    """
    수집된 텍스트를 간단하게만 정리하는 전처리 함수.
    - username이 본문 앞에 중복 삽입된 경우 제거
    - 불필요한 상대시간 토큰("3시간 전" 등) 제거
    - 줄 단위 공백 정리
    
    Args:
        text (str): 원본 텍스트
        username (Optional[str]): 작성자 username

    Returns:
        str: 정제된 텍스트
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
    DOM 컨테이너에서 텍스트만 추출한다.
    
    Args:
        container: Playwright element handle
    
    Returns:
        str: 추출된 텍스트
    """
    if not container:
        return ""
    lines: List[str] = container.eval_on_selector_all(
        ":scope > span > span",
        "nodes => nodes.map(n => (n.textContent || '').trim()).filter(Boolean)"
    )
    return "\n".join(lines).strip()


def _parse_dom(page, keyword: str) -> List[Dict]:
    """
    페이지 내의 post 카드들을 파싱하여 게시물 정보를 추출한다.
    
    Args:
        page: Playwright page 객체
    
    Returns:
        List[Dict]: 게시물 리스트. 각 항목은 username, uploaded_time, collected_time, channel, text 포함.
    """
    print("[parse_dom] DOM에서 게시물 파싱 시작")
    cards = page.query_selector_all(_CARD_SELECTOR)
    print(f"[parse_dom] 카드 후보 수: {len(cards)}")

    results: List[Dict] = []
    seen_keys: set[Optional[str]] = set()  # uploaded_time 기준 중복 제거

    for i, card in enumerate(cards):
        try:
            username_el = card.query_selector(_USERNAME_SELECTOR)
            username = (username_el.inner_text().strip() if username_el else None)

            time_el = card.query_selector(_TIME_SELECTOR)
            uploaded_time = time_el.get_attribute("datetime") if time_el else None

            text_container = card.query_selector(_TEXT_CONTAINER_SELECTOR)
            raw_text = _extract_text(text_container)
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
                "uploaded_time": _iso_to_epoch_s(uploaded_time),
                "collected_time": int(datetime.now(timezone.utc).timestamp()),
                "channel": "threads",
                "query": keyword,
                "text": text
            }
            results.append(item)

        except Exception as e:
            print(f"[parse_dom] error at card {i}: {e}")

    print(f"[parse_dom] 추출된 카드 수: {len(results)}")
    return results


def _scroll_once(page) -> int:
    """
    한 번 스크롤을 내리고 DOM의 증가량을 측정한다.
    
    Args:
        page: Playwright page 객체
    
    Returns:
        int: 새로 추가된 카드 수
    """
    before = len(page.query_selector_all(_CARD_SELECTOR))
    page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
    page.wait_for_timeout(random.randint(8000, 15000))  # 8~15초 대기
    after = len(page.query_selector_all(_CARD_SELECTOR))
    return after - before


def _infinite_scroll_collect_recent(
    page,
    keyword: str,
    within_minutes: int = 10,
    patience_rounds: int = 3,
    max_no_growth_rounds: int = 3,
    max_scrolls: int = 200,
) -> List[Dict]:
    """
    무한 스크롤을 수행하며 최근 within_minutes 이내 게시물만 수집한다.
    
    Args:
        page: Playwright page 객체
        within_minutes (int): 수집 기준 시간(분)
        patience_rounds (int): 새 게시물 없을 때 종료 전 허용 횟수
        max_no_growth_rounds (int): DOM이 성장하지 않을 때 종료 허용 횟수
        max_scrolls (int): 최대 스크롤 횟수
    
    Returns:
        List[Dict]: 수집된 게시물 목록
    """
    collected: Dict[Tuple[int, Optional[str]], Dict] = {}
    consecutive_no_dom_no_growth = 0
    saw_in_range_post = False

    cutoff_hits = 0  # 연속 cutoff 횟수 카운트
    no_results_rounds = 0 # 연속 수집 개수 0개 카운트

    round_num = 0
    while round_num < max_scrolls:
        round_num += 1

        page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
        delay = random.randint(15000, 25000)
        print(f"[INFO] 스크롤 후 {delay/1000:.1f}초 대기")
        page.wait_for_timeout(delay)

        now_epoch = int(datetime.now(timezone.utc).timestamp())
        cutoff_epoch = now_epoch - within_minutes * 60

        batch = _parse_dom(page, keyword)
        added_in_range = 0

        for item in batch:
            up_epoch = item.get("uploaded_time")
            if up_epoch is None:
                continue

            # cutoff(10분 이전)
            if up_epoch < cutoff_epoch:
                continue

            if up_epoch > now_epoch:
                continue

            key = (up_epoch, item.get("username"))
            if key not in collected:
                collected[key] = item
                added_in_range += 1
                saw_in_range_post = True

        print(f"[collect] round={round_num}, 이번={added_in_range}, 누적={len(collected)}")

        # 수집 여부 체크
        # === 새 종료 조건 ===
        if added_in_range == 0:
            no_results_rounds += 1
            if not saw_in_range_post and no_results_rounds >= 3:
                print("[scroll] 3회 연속 아무 게시물도 수집 안됨 → 종료")
                break
        else:
            no_results_rounds = 0

        # cutoff 여부 체크
        if added_in_range == 0 and saw_in_range_post:
            cutoff_hits += 1
            print(f"[scroll] cutoff hit {cutoff_hits}회")
        else:
            cutoff_hits = 0  # 새 글 나오면 리셋

        # 종료 조건
        if cutoff_hits >= 2:  # 2번 연속 cutoff
            print("[scroll] cutoff(10분) 이전 게시물 2번 연속 → 종료")
            break
        if saw_in_range_post:
            print(f"[scroll] 10분 이내 새 게시물 {patience_rounds}번 연속 없음 → 종료")
            break
        if saw_in_range_post and consecutive_no_dom_no_growth >= max_no_growth_rounds:
            print("[scroll] DOM 증가 없음 → 종료")
            break

        delta = _scroll_once(page)
        if delta <= 0:
            consecutive_no_dom_no_growth += 1
        else:
            consecutive_no_dom_no_growth = 0

        page.wait_for_timeout(1000)

    return list(collected.values())


def scrape_search(
    keyword: str,
    within_minutes: int = 10,
    cookies_override: Optional[List[dict]] = None
) -> dict:
    """
    특정 키워드에 대해 Threads 검색 페이지에서 최근 게시물 크롤링.
    
    Args:
        keyword (str): 검색 키워드
        within_minutes (int): 최근 N분 이내 게시물만 수집
        cookies_override (Optional[List[dict]]): 로그인 쿠키
    
    Returns:
        dict: {"threads": [게시물 dict들]} 형태
    """
    if cookies_override is None:
        raise RuntimeError("쿠키 로드 실패")

    with sync_playwright() as pw:
        print("[scrape_search] 브라우저 실행")
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu" , 
                "--no-sandbox" , 
                "--single-process" , 
                "--disable-dev-shm-usage" , 
                "--no-zygote" , 
                "--disable-setuid-sandbox" , 
                "--disable-accelerated-2d-canvas" , 
                "--disable-dev-shm-usage" , 
                "--no-first-run" , 
                "--no-default-browser-check" , 
                "--disable-background-networking" , 
                "--disable-background-timer-throttling" , 
                "--disable-client-side-phishing-detection" , 
                "--disable-component-update" , 
                "--disable-default-apps" , 
                "--disable-domain-reliability" , 
                "--disable-features=AudioServiceOutOfProcess" , 
                "--disable-hang-monitor" , 
                "--disable-ipc-flooding-protection" , 
                "--disable-popup-blocking "
                "--disable-prompt-on-repost" , 
                "--disable-renderer-backgrounding" , 
                "--disable-sync" ,
                "--force-color-profile=srgb" , 
                "--metrics-recording-only" , 
                "--mute-audio" , 
                "--no-pings" , 
                "--use-gl=swiftshader" , 
                "--window-size=1280,1696"
            ],
        )
        context = browser.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.0.0 Safari/537.36"),
            locale="en-US",
            timezone_id="America/New_York",   # 미국 동부시간
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"}
        )
        context.add_cookies(cookies_override)
        page = context.new_page()

        url = f"https://www.threads.com/search?q={keyword}&serp_type=default&filter=recent&hl=en"
        print("[scrape_search] 이동:", url)
        page.goto(url, wait_until="domcontentloaded")

        page.wait_for_selector("a[href^='/@'] span", timeout=15000)
        page.wait_for_selector("time[datetime]", timeout=15000)

        results = _infinite_scroll_collect_recent(
            page,
            keyword,
            within_minutes=within_minutes,
            patience_rounds=3,
            max_no_growth_rounds=3,
            max_scrolls=200,
        )
        
        browser.close()

        final_results = []
        for r in results:
            final_results.append({
                **r,
                "uploaded_time": _epoch_to_iso(r["uploaded_time"]),
                "collected_time": _epoch_to_iso(r["collected_time"]),
            })

        return {"threads": final_results}

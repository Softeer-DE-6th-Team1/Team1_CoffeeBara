# lambda_function.py
import os, asyncio, logging, csv, httpx
from datetime import datetime, timezone, timedelta
import boto3, botocore
from twikit import Client
from twikit.errors import TooManyRequests, BadRequest
from httpx import ReadTimeout, ConnectTimeout, TimeoutException, ConnectError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)   # ← httpx INFO 로그 숨김

# ===== 환경변수 =====
USERNAME = os.getenv('X_USERNAME', '')
EMAIL = os.getenv('X_EMAIL', '')
PASSWORD = os.getenv('X_PASSWORD', '')

LEGACY_KEYWORD = os.getenv('KEYWORD', '')
S3_BUCKET = os.getenv('S3_BUCKET', 'softeer-de-6th-team1')
S3_PREFIX = os.getenv('S3_KEY_PREFIX', 'raw-data')
COOKIES_KEY = os.getenv("COOKIES_KEY", "configs/x_cookies.json")
KEYWORDS_KEY = os.getenv("KEYWORDS_KEY", "configs/keywords.txt") 
WINDOW_MINUTES = int(os.getenv("WINDOW_MINUTES", "30"))
TARGET = int(os.getenv("TARGET", "400"))
CHANNEL = os.getenv('CHANNEL', 'X')

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def _s3_upload(local_path: str, key: str):
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET env is empty")
    s3.upload_file(local_path, S3_BUCKET, key)
    logging.info(f"uploaded to s3://{S3_BUCKET}/{key}")


def _csv_header(path: str):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=[
                "username","uploaded_time","collected_time","channel","query","text"
            ])
            w.writeheader()

# DynamoDB 카운터 업데이트 및 완료 처리 함수 추가
def _update_and_finalize_job(job_info: dict, bucket: str, prefix: str):
    """DynamoDB 카운터를 업데이트하고, 모든 작업 완료 시 _SUCCESS 파일을 생성합니다."""
    job_id = job_info.get('jobId')
    total_files = job_info.get('totalFiles')
    table_name = job_info.get('tableName')

    # job_info가 없으면 DynamoDB 로직을 건너뜁니다 (테스트 등 예외 상황용).
    if not all([job_id, total_files, table_name]):
        logging.warning("job_info is missing. Skipping DynamoDB counter update.")
        return

    table = dynamodb.Table(table_name)
    try:
        # 카운터 원자적으로 1 증가
        response = table.update_item(
            Key={'jobId': job_id},
            UpdateExpression="SET completedFiles = completedFiles + :val",
            ExpressionAttributeValues={':val': 1},
            ReturnValues="UPDATED_NEW"
        )
        
        new_count = response.get('Attributes', {}).get('completedFiles')
        logging.info(f"Incremented counter for job {job_id}. Status: {new_count}/{total_files}")

        # 마지막 작업자인 경우 _SUCCESS 파일 생성
        if new_count is not None and new_count >= total_files:
            logging.info(f"This is the final job for {job_id}. Creating _SUCCESS file.")
            success_key = f"{prefix}/_SUCCESS"
            s3.put_object(Bucket=bucket, Key=success_key, Body=b'')
            logging.info(f"Created _SUCCESS file at s3://{bucket}/{success_key}")
            
            # (선택) 작업 상태를 'COMPLETED'로 업데이트
            table.update_item(
                Key={'jobId': job_id},
                UpdateExpression="SET #s = :status",
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':status': 'COMPLETED'}
            )
    except Exception as e:
        logging.error(f"Failed to update DynamoDB counter for job {job_id}: {e}")


def _parse_created_at(created):
    """
    twikit created_at은 버전에 따라 str 또는 datetime일 수 있음.
    datetime(naive)이면 UTC로 간주해 tz 부여.
    """
    if created is None:
        return None
    if isinstance(created, str):
        # 예: "Mon Aug 19 02:15:23 +0000 2025"
        try:
            dt = datetime.strptime(created, "%a %b %d %H:%M:%S %z %Y")
            return dt.astimezone(timezone.utc)
        except Exception:
            logging.warning(f"created_at parse failed; raw={created!r}")
            return None
    if isinstance(created, datetime):
        if created.tzinfo is None:
            return created.replace(tzinfo=timezone.utc)
        return created.astimezone(timezone.utc)
    return None


def _save_batch_windowed(path: str, batch, collected_time_iso: str, 
                         keyword: str, window_start: datetime, window_end: datetime):
    """
    - 윈도우 [window_start, window_end] 에 포함되는 트윗만 CSV에 저장
    - 페이지 내 최소/최대 created_at을 반환하여 페이징 중단 판단에 사용
    - 반환값: saved_count, page_has_any_in_window(bool), page_min_dt, page_max_dt
    """
    saved = 0
    page_times = []
    batch_size = len(list(batch))

    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=[
            "username","uploaded_time","collected_time","channel","query","text"
        ])
        for t in batch:
            # 언어가 'en'(영어)가 아니면 건너뛰는 필터 추가
            if getattr(t, "lang", None) != 'en':
                continue

            text = (getattr(t, "text", "") or "").replace("\n", " ").strip()
            # 본문에 키워드가 있을 때만 저장
            if keyword.lower() not in text.lower():
                continue

            created_dt = _parse_created_at(getattr(t, "created_at", None))
            if created_dt is None:
                continue

            page_times.append(created_dt)

            # 윈도우 필터
            if not (window_start <= created_dt <= window_end):
                continue

            w.writerow({
                "username": getattr(getattr(t, "user", None), "name", ""),
                "uploaded_time": created_dt.isoformat(timespec="seconds"),
                "collected_time": collected_time_iso,
                "channel": CHANNEL,
                "query": keyword,
                "text": f"\"{text}\"",
            })
            saved += 1

    if page_times:
        page_min = min(page_times)
        page_max = max(page_times)
        has_any_in_window = any(window_start <= dt <= window_end for dt in page_times)
    else:
        page_min = page_max = None
        has_any_in_window = False
    
    logging.info(f"[{keyword}] Batch processed: {saved} saved out of {batch_size} tweets.")

    return saved, has_any_in_window, page_min, page_max


def _load_keywords_from_s3(tmp_path="/tmp/keywords.txt"):
    """
    S3에서 KEYWORDS_KEY 파일을 받아 줄 단위 키워드 리스트로 리턴.
    - UTF-8(BOM 허용)로 읽음
    - 공백 줄/주석(# ...) 무시
    - 중복 제거, 원본 순서 유지
    """
    try:
        s3.download_file(S3_BUCKET, KEYWORDS_KEY, tmp_path)
        logging.info(f"keywords file downloaded from s3://{S3_BUCKET}/{KEYWORDS_KEY}")
    except botocore.exceptions.ClientError as e:
        logging.warning(f"keywords download failed: {e}; fallback to LEGACY_KEYWORD if provided")
        return []

    seen = set()
    keywords = []
    with open(tmp_path, "r", encoding="utf-8-sig") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            if raw not in seen:
                seen.add(raw)
                keywords.append(raw)
    return keywords


async def _collect_for_keyword(client: Client, keyword: str, collected_iso: str,
                               context, window_start: datetime, window_end: datetime,
                               target_rows_cap: int, local_csv_path: str):
    """
    단일 키워드에 대해 'Latest' 검색을 페이지네이션하며
    created_at이 [window_start, window_end]인 트윗만 local_csv_path에 저장.
    페이지의 모든 트윗이 window_start 이전(=전부 오래됨)이면 중단.
    """
    # ---- 남은 시간 체크 ----
    def time_ms_left():
        try:
            return context.get_remaining_time_in_millis() if context else 300000
        except Exception:
            return 300000

    def has_time_left(buffer_ms=15000):
        return time_ms_left() > buffer_ms

    query = keyword
    logging.info(f"[{keyword}] search start (window {window_start.isoformat()} ~ {window_end.isoformat()})")

    # ---- 첫 페이지 ----
    try:
        page = await client.search_tweet(query, "Latest", count=20)
    except Exception as e:
        logging.error(f"[{keyword}] initial search failed: {e}")
        return {"ok": False, "rows_saved": 0, "keyword": keyword, "error": str(e)}

    saved_count, in_window, page_min, page_max = _save_batch_windowed(
        local_csv_path, page, collected_iso, keyword, window_start, window_end
    )
    cursor = getattr(page, "next_cursor", None)
    total_saved = saved_count

    # 만약 페이지 내 트윗이 전부 window_start 이전이라면(=in_window False 그리고 page_max < window_start),
    # 최신순 기준 이후 페이지는 더 오래된 것뿐 → 중단
    if not in_window and page_max and page_max < window_start:
        cursor = None  # 즉시 중단

    # ---- 페이지네이션 ----
    while cursor and has_time_left(20000):

        # 안전장치(희귀 상황 무한 페이징 방지)
        if total_saved >= target_rows_cap:
            logging.info(f"[{keyword}] reached safety cap {target_rows_cap}; stopping")
            break        

        await asyncio.sleep(3)
        try:
            page = await client.search_tweet(query, "Latest", count=20, cursor=cursor)
        except TooManyRequests:
            logging.warning(f"[{keyword}] 429 rate limit; stop paging")
            break
        except (TimeoutException, ConnectError, ConnectTimeout, ReadTimeout) as e:
            logging.warning(f"[{keyword}] transient error: {e}; stop paging")
            break
        except Exception as e:
            logging.warning(f"[{keyword}] unexpected search error: {e}; stop paging")
            break

        if not page:
            break

        saved, in_window, page_min, page_max = _save_batch_windowed(
            local_csv_path, page, collected_iso, keyword, window_start, window_end
        )
        total_saved += saved
        cursor = getattr(page, "next_cursor", None)

        # 페이지 전체가 윈도우 밖(이전)이라면 더 볼 필요 없음
        if not in_window and page_max and page_max < window_start:
            logging.info(f"[{keyword}] page fully older than window; stopping")
            break

        if not has_time_left(20000):
            logging.warning(f"[{keyword}] Low time budget; break paging")
            break

    result = {
        "ok": True,
        "rows_saved": total_saved,
        "keyoword": keyword,
        "keyword": keyword,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }
    if not has_time_left(0):
        result["near_timeout"] = True
    return result


async def run(event, context=None):
    client = Client(
    'en-US',
    timeout=httpx.Timeout(connect=15, read=45, write=30, pool=30)
    )

    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET env is required")

    # ---- 남은 시간 체크 유틸 ----
    def time_ms_left():
        try:
            return context.get_remaining_time_in_millis() if context else 300000
        except Exception:
            return 300000

    def has_time_left(buffer_ms=15000):
        return time_ms_left() > buffer_ms


    # ---- 쿠키 파일 준비 (/tmp) ----
    # cookies는 람다에서 /tmp에 보관
    
    cookies_path = "/tmp/x_cookies.json"
    
    try:
        s3.download_file(S3_BUCKET, COOKIES_KEY, cookies_path)
        logging.info("cookies.json downloaded from S3")
    except botocore.exceptions.ClientError as e:
        logging.error(f"cookies download failed: {e}")
        raise

    # ---- 로그인 ----
    try:
        # 쿠키 로그인 (동기)
        await client.login( 
            auth_info_1=USERNAME, 
            auth_info_2=EMAIL, 
            password=PASSWORD, 
            cookies_file=cookies_path)
        logging.info("cookie load OK")

    except Exception as e:
        logging.error(f"[FATAL] cookie login failed: {type(e).__name__}: {e}")
        raise

    # ---- 키워드 로딩 ----
    keywords = _load_keywords_from_s3()
    if not keywords and LEGACY_KEYWORD:
        logging.warning("No keywords file or empty; fallback to single KEYWORD env")
        keywords = [LEGACY_KEYWORD]
    if not keywords:
        raise RuntimeError("No keywords to process. Ensure S3 keywords file or KEYWORD env is set.")

    # 윈도우 계산 (UTC 기준)
    now_utc = datetime.now(timezone.utc)
    window_end = now_utc
    window_start = now_utc - timedelta(minutes=WINDOW_MINUTES)
    collected_iso = now_utc.isoformat(timespec="seconds")

    scraper_id = event.get("scraper_id", "x") 
    ts_for_filename = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    local_csv = f"/tmp/scraper_{scraper_id}_{ts_for_filename}.csv"
    _csv_header(local_csv)
    logging.info(f"Created a single CSV file for all keywords: {local_csv}")

    results = []
    for kw in keywords:
        # 키워드 간에도 남은 시간 확인
        if not has_time_left(40000):  # 다음 키워드 수행 여유 (검색/페이지/업로드)
            logging.warning("Insufficient time for next keyword; stopping loop")
            break

        res = await _collect_for_keyword(
            client=client,
            keyword=kw,
            collected_iso=collected_iso,
            context=context,
            window_start=window_start,
            window_end=window_end,
            target_rows_cap=TARGET,  # 안전 상한(무한 페이징 방지용)
            local_csv_path=local_csv
        )
        results.append(res)
        await asyncio.sleep(2)  # rate limit 완화

    prefix = event.get("prefix", S3_PREFIX)
    s3_key = ""

    # 데이터가 있는 경우에만 업로드하도록 수정
    if os.path.exists(local_csv) and os.path.getsize(local_csv) > 65:
        try:
            key = f"{prefix}/scraper_{scraper_id}.csv"
            _s3_upload(local_csv, key)
            s3_key = f"s3://{S3_BUCKET}/{key}"
        finally:
            try:
                os.remove(local_csv)
                logging.info(f"Removed local file: {local_csv}")
            except Exception as e:
                logging.error(f"Failed to remove local file {local_csv}: {e}")

    # 모든 작업이 끝난 후, 최종적으로 DynamoDB 카운터 업데이트
    job_info = event.get("job_info", {})
    _update_and_finalize_job(job_info, S3_BUCKET, prefix)

    # (선택) httpx 커넥션 정리
    try:
        await client.http.aclose()
    except Exception:
        pass

    summary = {
        "ok": any(r.get("ok") for r in results),
        "final_s3_key": s3_key,
        "processed_keywords": [r.get("keyword") for r in results],
        "window_minutes": WINDOW_MINUTES,
        "results_per_keyword": results
    }
    if not has_time_left(0):
        summary["near_timeout"] = True
    return summary


def lambda_handler(event, context):
    """
    AWS Lambda entrypoint.
    키워드별로 '최근 WINDOW_MINUTES분' 트윗만 수집하여 업로드합니다.
    """
    return asyncio.run(run(event, context))

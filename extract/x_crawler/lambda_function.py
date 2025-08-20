# lambda_function.py
import os, asyncio, logging, csv, httpx
from datetime import datetime, timezone
import boto3, botocore
from twikit import Client
from twikit.errors import TooManyRequests, BadRequest
from httpx import ReadTimeout, ConnectTimeout, TimeoutException, ConnectError


logging.basicConfig(level=logging.INFO)

# ===== 환경변수 =====
USERNAME = os.getenv('USERNAME', '')
EMAIL = os.getenv('EMAIL', '')
PASSWORD = os.getenv('PASSWORD', '')

KEYWORD = os.getenv('KEYWORD', '김종국 결혼')  # 바꾸고 싶으면 환경변수로
S3_BUCKET = os.getenv('S3_BUCKET', 'demo-hayeonnoh')        # 예: demo-hayeonnoh
S3_PREFIX = os.getenv('S3_KEY_PREFIX', 'twitter-data')
COOKIES_KEY = os.getenv("COOKIES_KEY", "secret/cookies.json")
TARGET = int(os.getenv("TARGET_ROWS", "200"))  # 수집 목표(옵션)

s3 = boto3.client('s3')


def _s3_upload(local_path: str, key: str):
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET env is empty")
    s3.upload_file(local_path, S3_BUCKET, key)
    logging.info(f"uploaded to s3://{S3_BUCKET}/{key}")


def _csv_header(path: str):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=[
                "username","uploaded_time","collected_time","channel","text"
            ])
            w.writeheader()


def _save_batch(path: str, batch, collected_time_iso: str):
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=[
            "username","uploaded_time","collected_time","channel","text"
        ])
        for t in batch:
            text = (getattr(t, "text", "") or "").replace("\n", " ").strip()
            # 본문에 키워드가 있을 때만 저장
            if KEYWORD.lower() not in text.lower():
                continue

            created = getattr(t, "created_at", None)
            # twikit 버전에 따라 str일 수 있어 방어
            if isinstance(created, str):
                # 예: "Mon Aug 19 02:15:23 +0000 2025"
                try:
                    created = datetime.strptime(created, "%a %b %d %H:%M:%S %z %Y")
                except Exception as e:
                    logging.warning(f"created_at parse failed: {e}; raw={created!r}")
                    created = None

            if created is not None and created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)

            uploaded_iso = created.astimezone(timezone.utc).isoformat(timespec="seconds") if created else None

            w.writerow({
                "username": getattr(getattr(t, "user", None), "name", ""),
                "uploaded_time": uploaded_iso,
                "collected_time": collected_time_iso,
                "channel": "x",
                "text": text,
            })


async def run(context=None):
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
    cookies_path = "/tmp/cookies.json"
    try:
        s3.download_file(S3_BUCKET, COOKIES_KEY, cookies_path)
        #logging.info("cookies.json downloaded from S3")
        print("cookies.json downloaded from S3")
    except botocore.exceptions.ClientError as e:
        #logging.error(f"cookies download failed: {e}")
        print(f"cookies download failed: {e}")
        raise

    # ---- 로그인 ----
    try:
        # 쿠키 로그인 (동기)
        await client.login( 
            auth_info_1=USERNAME, 
            auth_info_2=EMAIL, 
            password=PASSWORD, 
            cookies_file=cookies_path)
        #logging.info("cookie load OK")
        print("cookie load OK")

    except Exception as e:
        #logging.error(f"[FATAL] cookie login failed: {type(e).__name__}: {e}")
        print(f"[FATAL] cookie login failed: {type(e).__name__}: {e}")
        raise


    collected_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    local_csv = f"/tmp/x_outputs_{int(datetime.now().timestamp())}.csv"
    _csv_header(local_csv)

    query = KEYWORD
    logging.info(f"search start: '{query}'")

    # ---- 첫 페이지 ----
    try:
        page = await client.search_tweet(query, "Latest", count=20)
    except Exception as e:
        logging.error(f"[FATAL] initial search failed: {e}")
        raise

    _save_batch(local_csv, page, collected_iso)
    cursor = getattr(page, "next_cursor", None)
    fetched = len(page)

    # ---- 페이지네이션 ----
    while cursor and fetched < TARGET and has_time_left(20000):
        # 여유 시간 부족하면 부분 업로드 후 종료
        if not has_time_left(20000):
            logging.warning("Low time budget detected, stopping pagination")
            break

        await asyncio.sleep(3)

        try:
            page = await client.search_tweet(query, "Latest", count=20, cursor=cursor)
        except TooManyRequests:
            # 남은시간 부족하면 중단 후 부분 업로드
            logging.warning("429 rate limit; breaking to upload partial results")
            break
        except (TimeoutException, ConnectError, ConnectTimeout) as e:
            logging.warning(f"search page transient error: {e}; stop paging")
            break

        if not page:
            break

        _save_batch(local_csv, page, collected_iso)
        fetched += len(page)
        cursor = getattr(page, "next_cursor", None)

    # ---- 업로드 & 정리 ----
    safe_kw = KEYWORD.replace("/", "_").replace(" ", "")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{S3_PREFIX}/keyword={safe_kw}/{ts}.csv"
    
    try:
        _s3_upload(local_csv, key)
    finally:
        # 파일 정리
        try:
            os.remove(local_csv)
        except Exception:
            pass

    # (선택) httpx 커넥션 정리
    try:
        await client.http.aclose()
    except Exception:
        pass

    result = {"ok": True, "rows_estimate": fetched, "s3_key": key}
    if not has_time_left(0):
        # (로그 목적) 거의 소진 상태
        result["near_timeout"] = True
    return result

def lambda_handler(event, context):
    """
    AWS Lambda entrypoint.
    context.get_remaining_time_in_millis()를 run()에 전달하여
    429/지연 시 부분 업로드 후 안전 종료하도록 함.
    """
    return asyncio.run(run(context))

# app/handler.py
import os
import json
import csv
import io
import boto3
from datetime import datetime, timezone
from typing import List
from threads_scraper import scrape_search

# 기본 설정
_BUCKET_NAME = os.getenv("THREADS_BUCKET", "mariahwy-softeer-test")
_COOKIE_KEY = os.getenv("COOKIE_KEY", "cookies/threads_cookies.json")
_KEYWORD_KEY = os.getenv("KEYWORD_KEY", "configs/keywords.txt")

# CSV 기본 스키마 (수집 결과가 비어도 헤더는 기록)
_DEFAULT_FIELDS = ["username", "uploaded_time", "collected_time", "channel", "text"]


def _load_from_s3(bucket: str, key: str) -> str:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def _load_cookies_from_s3(bucket: str) -> list:
    body = _load_from_s3(bucket, _COOKIE_KEY)
    raw = json.loads(body)
    return raw.get("cookies", raw) if isinstance(raw, dict) else raw


def _load_keywords_from_s3(bucket: str) -> List[str]:
    content = _load_from_s3(bucket, _KEYWORD_KEY)
    return [line.strip() for line in content.splitlines() if line.strip()]


def save_to_s3_csv(keyword: str, items: List[dict], bucket: str, prefix: str = "threads") -> str:
    """
    결과를 s3://{bucket}/{prefix}/threads_{keyword}_{UTC_YYYYMMDD_HHMMSS}.csv 로 저장
    결과가 비어도 헤더는 기록
    """
    now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{prefix}/threads_{keyword}_{now_str}.csv"

    buffer = io.StringIO()
    # 결과가 있으면 동적으로 헤더를 구성하되, 없으면 기본 필드 사용
    if items:
        fieldnames = list(items[0].keys())
    else:
        fieldnames = _DEFAULT_FIELDS

    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for item in items:
        writer.writerow(item)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"[S3] Uploaded to s3://{bucket}/{key}")
    return key


def lambda_handler(event, context):
    """
    event 옵션 (모두 선택 사항):
      - within_minutes: int (기본 60)
      - keywords: List[str]  # 주면 S3 대신 이 리스트 사용
      - bucket: str         # 기본 버킷 오버라이드
      - local_cookies_path: str  # 로컬 JSON 파일에서 쿠키 로드
      - local_keywords_path: str # 로컬 txt에서 키워드 로드
      - dry_run: bool       # True면 S3 업로드 생략 (로컬 테스트 편의)
      - prefix: str         # S3 저장 경로 prefix (기본 "threads-data")
    """
    event = event or {}
    bucket = event.get("bucket") or _BUCKET_NAME

    within_minutes = int(
        (event.get("within_minutes") if isinstance(event, dict) else None)
        or os.getenv("WITHIN_MINUTES", "60")
    )

    keywords = _load_keywords_from_s3(bucket)
    print(f"[conf] keywords from s3://{bucket}/{_KEYWORD_KEY}")

    cookies = _load_cookies_from_s3(bucket)
    print(f"[conf] cookies from s3://{bucket}/{_COOKIE_KEY}")

    cookies_override = cookies if cookies else None
    prefix = event.get("prefix", "threads-data")

    all_results = {}
    saved_keys = []
    errors = []

    for kw in keywords:
        try:
            print(f"[handler] Scraping keyword={kw}, within_minutes={within_minutes}")
            result = scrape_search(
                keyword=kw,
                within_minutes=within_minutes,
                cookies_override=cookies_override,
            )
            all_results[kw] = result.get("threads", [])

            key = save_to_s3_csv(kw, all_results[kw], bucket=bucket, prefix=prefix)
            saved_keys.append(key)

        except Exception as e:
            print(f"[ERROR] {kw} 저장 실패: {e}")
            errors.append({"keyword": kw, "error": str(e)})

    body = {
        "message": "Scraping complete" if not errors else "Scraping failed for some keywords",
        "errors": errors,
        "s3_keys": saved_keys,
        "keywords": keywords,
        "total": sum(len(v) for v in all_results.values()),
    }

    return {
        "statusCode": 200 if not errors else 500,
        "headers": {"Content-Type": "application/json; charset=utf-8"},
        "body": json.dumps(body, ensure_ascii=False),
    }


# 로컬 테스트용
if __name__ == "__main__":
    dummy_event = {
        "within_minutes": 10,
    }
    dummy_context = None

    resp = lambda_handler(dummy_event, dummy_context)
    print(json.dumps(resp, indent=2, ensure_ascii=False))

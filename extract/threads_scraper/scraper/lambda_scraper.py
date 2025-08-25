import io
import csv
import json
import boto3
from datetime import datetime, timezone
from typing import List, Dict, Any
from threads_scraper import scrape_search

 # 기본 설정
_BUCKET_NAME = "softeer-de-6th-team1"
_COOKIE_PATH = "configs/threads_cookies.json"

# CSV 기본 스키마 (수집 결과가 비어도 헤더는 기록)
_DEFAULT_FIELDS = ["username", "uploaded_time", "collected_time", "channel", "query", "text"]

s3 = boto3.client("s3")

def _load_cookies_from_s3(bucket: str, key: str) -> list:
    """쿠키 JSON 로드"""
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    raw = json.loads(body)
    return raw.get("cookies", raw) if isinstance(raw, dict) else raw


def _save_to_s3_csv(
    keyword: str,
    items: List[Dict[str, Any]],
    bucket: str,
    prefix: str
) -> str:
    """
    크롤링 결과를 CSV로 저장
    s3://{bucket}/{prefix}/threads_{keyword}.csv
    """
    key = f"{prefix}/threads_{keyword}.csv"

    buffer = io.StringIO()
    if items:
        fieldnames = list(items[0].keys())
    else:
        fieldnames = _DEFAULT_FIELDS

    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for item in items:
        writer.writerow(item)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"[scraper] Uploaded {len(items)} rows → s3://{bucket}/{key}")
    return key


def lambda_handler(event, context):
    """
    Scraper Lambda
    - dispatcher 이벤트를 받아서 threads_scraper 실행
    - CSV를 S3에 저장
    """
    bucket = event.get("bucket", _BUCKET_NAME)
    prefix = event["prefix"]               
    keyword = event["keyword"]
    within_minutes = int(event.get("within_minutes", 60))

    try:
        # 쿠키 로드
        cookies = _load_cookies_from_s3(bucket, _COOKIE_PATH)

        # 크롤링 실행
        print(f"[scraper] keyword={keyword}, within={within_minutes}min")
        result = scrape_search(
            keyword=keyword,
            within_minutes=within_minutes,
            cookies_override=cookies,
        )
        items = result.get("threads", [])

        # CSV 저장
        key = _save_to_s3_csv(keyword, items, bucket, prefix)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Scraping complete",
                "keyword": keyword,
                "s3_key": key,
                "count": len(items),
            }, ensure_ascii=False),
        }

    except Exception as e:
        print(f"[ERROR] keyword={keyword}, error={e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Scraping failed",
                "keyword": keyword,
                "error": str(e),
            }, ensure_ascii=False),
        }


#로컬 테스트용
if __name__ == "__main__":
    dummy_event = {
        "keyword": "hyundai",
        "within_minutes": 10,
        "bucket": _BUCKET_NAME,
        "prefix": "threads-data"
    }
    dummy_context = None

    resp = lambda_handler(dummy_event, dummy_context)
    print(json.dumps(resp, indent=2, ensure_ascii=False))

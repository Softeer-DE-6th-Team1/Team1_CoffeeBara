# app/handler.py
import os
import json
import csv
import io
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError
# from threads_scraper import scrape_search
from threads_scraper_tesla_autopilot import scrape_search


_BUCKET_NAME = "mariahwy-softeer-test"
_COOKIE_KEY = "cookies/threads_cookies.json"
_KEYWORD_KEY = "configs/keywords.txt"


def _load_from_s3(bucket: str, key: str) -> str:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def _load_cookies_from_s3() -> list:
    body = _load_from_s3(_BUCKET_NAME, _COOKIE_KEY)
    raw = json.loads(body)
    return raw.get("cookies", raw) if isinstance(raw, dict) else raw


def _load_keywords_from_s3() -> list[str]:
    content = _load_from_s3(_BUCKET_NAME, _KEYWORD_KEY)
    return [line.strip() for line in content.splitlines() if line.strip()]


def save_to_s3_csv(keyword: str, items: list[dict], bucket: str, prefix: str = "threads") -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{prefix}/threads_{keyword}_{now_str}.csv"

    buffer = io.StringIO()
    if items:
        fieldnames = ["keyword"] + list(items[0].keys())
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            row = {"keyword": keyword, **item}
            writer.writerow(row)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )
    print(f"[S3] Uploaded to s3://{bucket}/{key}")
    return key


def lambda_handler(event, context):
    keywords = _load_keywords_from_s3()
    # max_items = int((event.get("max_items") if isinstance(event, dict) else None) 
    #                 or os.getenv("MAX_ITEMS", "200"))
    start_date = event.get("start_date")
    end_date = event.get("end_date")
    cookies = _load_cookies_from_s3()
    cookies_override = cookies if cookies else None

    all_results = {}
    saved_keys = []
    errors = []

    for kw in keywords:
        try:
            print(f"[handler] Scraping keyword={kw}")
            result = scrape_search(
                keyword=kw,
                # max_items=max_items,
                start_date = start_date,
                end_date = end_date,
                cookies_override=cookies_override
            )
            all_results[kw] = result["threads"]

            # 키워드별 파일 저장
            key = save_to_s3_csv(kw, all_results[kw], bucket=_BUCKET_NAME, prefix="threads-data")
            saved_keys.append(key)
        except Exception as e:
            print(f"[ERROR] {kw} 저장 실패: {e}")
            errors.append({"keyword": kw, "error": str(e)})

    # 최종 결과 반환
    if errors:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json; charset=utf-8"},
            "body": json.dumps({
                "message": "Scraping failed for some keywords",
                "errors": errors,
                "s3_keys": saved_keys,
                "keywords": keywords,
                "total": sum(len(v) for v in all_results.values())
            }, ensure_ascii=False)
        }
    else:
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json; charset=utf-8"},
            "body": json.dumps({
                "message": "Scraping complete",
                "s3_keys": saved_keys,
                "keywords": keywords,
                "total": sum(len(v) for v in all_results.values())
            }, ensure_ascii=False)
        }


if __name__ == "__main__":
    dummy_event = {
        "keyword": "tesla",                  # 검색 키워드
        # "max_items": 200,                    # 최대 수집 개수
        "start_date": "2025-07-20",          # 기간 시작 (YYYY-MM-DD)
        "end_date": "2025-07-31"             # 기간 끝 (YYYY-MM-DD)
    }
    dummy_context = None

    resp = lambda_handler(dummy_event, dummy_context)
    print(json.dumps(resp, indent=2, ensure_ascii=False))

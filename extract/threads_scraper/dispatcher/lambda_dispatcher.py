import os
import json
import boto3
from datetime import datetime, timezone
from typing import List

# 기본 설정
_BUCKET_NAME = "softeer-de-6th-team1"
_KEYWORD_PATH = "configs/keywords.txt"
_THREADS_LAMBDA = os.getenv("THREADS_LAMBDA", "threads-scraper")  # Scraper Lambda 함수명 (환경변수로 덮어쓸 수 있음)
_X_LAMBDA = os.getenv("X_LAMBDA", "x-scraper-lambda")

s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")

def _load_keywords_from_s3(bucket: str, key: str) -> List[str]:
    """S3에서 keywords.txt 읽어서 리스트 반환"""
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    return [line.strip() for line in content.splitlines() if line.strip()]

def lambda_handler(event, context):
    """
    Dispatcher Lambda
    - S3에서 keywords.txt 불러오기
    - 각 키워드별 Scraper Lambda 비동기 실행
    """
    # 파라미터
    bucket = event.get("bucket", _BUCKET_NAME)
    within_minutes = int(event.get("within_minutes", 60))

    # UTC timestamp 폴더
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = event.get("prefix", "raw-data")

    # 키워드 불러오기
    keywords = _load_keywords_from_s3(bucket, _KEYWORD_PATH)
    print(f"[dispatcher] Loaded {len(keywords)} keywords from s3://{bucket}/{_KEYWORD_PATH}")

    # Threads Scraper 비동기 실행
    for idx, kw in enumerate(keywords, start=1):
        payload = {
            "bucket": bucket,
            "prefix": f"{prefix}/{timestamp}",  # raw-data/20250824T090000Z
            "keyword": kw,
            "within_minutes": within_minutes,
        }

        resp = lambda_client.invoke(
            FunctionName=_THREADS_LAMBDA,
            InvocationType="Event",  # 비동기 실행
            Payload=json.dumps(payload).encode("utf-8"),
        )
        print(f"[dispatcher] Invoked threads scraper {idx} for keyword={kw}, resp={resp['StatusCode']}")

    # X Scraper 실행
    resp2 = lambda_client.invoke(
        FunctionName=_X_LAMBDA,
        InvocationType="Event",
        Payload=json.dumps({
            "bucket": bucket,
            "prefix": f"{prefix}/{timestamp}",
            "keyword": "ALL",   # or None
            "within_minutes": within_minutes,
        }).encode("utf-8"),
    )
    print(f"[dispatcher] Invoked x scraper, resp={resp2['StatusCode']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "dispatched": len(keywords),
            "timestamp": timestamp,
            "prefix": f"{prefix}/{timestamp}"
        })
    }
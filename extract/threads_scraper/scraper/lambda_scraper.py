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
dynamodb = boto3.resource("dynamodb")

def _load_cookies_from_s3(bucket: str, key: str) -> list:
    """쿠키 JSON 로드"""
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    raw = json.loads(body)
    return raw.get("cookies", raw) if isinstance(raw, dict) else raw

def _update_and_finalize_job(job_info: dict, bucket: str, prefix: str):
    """DynamoDB 카운터를 업데이트하고, 모든 작업 완료 시 _SUCCESS 파일을 생성합니다."""
    job_id = job_info.get('jobId')
    total_files = job_info.get('totalFiles')
    table_name = job_info.get('tableName')

    if not all([job_id, total_files, table_name]):
        print("job_info is missing. Skipping DynamoDB counter update.")
        return

    table = dynamodb.Table(table_name)
    try:
        response = table.update_item(
            Key={'jobId': job_id},
            UpdateExpression="SET completedFiles = completedFiles + :val",
            ExpressionAttributeValues={':val': 1},
            ReturnValues="UPDATED_NEW"
        )
        new_count = response.get('Attributes', {}).get('completedFiles')
        print(f"Incremented counter for job {job_id}. Status: {new_count}/{total_files}")

        if new_count is not None and new_count >= total_files:
            print(f"This is the final job for {job_id}. Creating _SUCCESS file.")
            success_key = f"{prefix}/_SUCCESS"
            s3.put_object(Bucket=bucket, Key=success_key, Body=b'')
            print(f"Created _SUCCESS file at s3://{bucket}/{success_key}")
            
            table.update_item(
                Key={'jobId': job_id},
                UpdateExpression="SET #s = :status",
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':status': 'COMPLETED'}
            )
    except Exception as e:
        print(f"Failed to update DynamoDB counter for job {job_id}: {e}")



def _save_to_s3_csv(
    scraper_id: Any,
    items: List[Dict[str, Any]],
    bucket: str,
    prefix: str
) -> str:
    """
    크롤링 결과를 CSV로 저장
    s3://{bucket}/{prefix}/threads_{scraper_id}.csv
    """
    key = f"{prefix}/scarper_threads_{scraper_id}.csv"

    buffer = io.StringIO()
    fieldnames = _DEFAULT_FIELDS if not items else list(items[0].keys())
    
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(items)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"[{scraper_id}] Uploaded {len(items)} rows → s3://{bucket}/{key}")
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
    within_minutes = int(event.get("within_minutes", 30))

    # dispatcher로부터 전달받은 정보 추출
    job_info = event.get("job_info", {})
    scraper_id = event.get("scraper_id", keyword) 

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
        key = _save_to_s3_csv(scraper_id, items, bucket, prefix)

        # S3 저장 성공 후, dynamoDB 카운터 업데이트
        _update_and_finalize_job(job_info, bucket, prefix)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Scraping complete and counter updated",
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

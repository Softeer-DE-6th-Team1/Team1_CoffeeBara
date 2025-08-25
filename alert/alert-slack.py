import json
import boto3
import requests

def lambda_handler(event, context):
    # slack, DynamoDB 변수 설정
    slack_webhook_url = "" # INPUT YOUR Webhook URL
    table_name = "" # INPUT YOUR DynamoDB Table Name

    # DynamoDB 연결
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # INSERT 데이터 처리
    for record in event['Records']:
        event_name = record['eventName']
        if event_name == 'INSERT':
            print(f"새로운 데이터를 처리합니다.")  
            new_image = record['dynamodb']['NewImage'] # 요소 가져오기
            
            # Slack 알람 요소 맵핑
            pk = new_image['pk']['S'].split('#')
            channel = pk[0]
            category = pk[2]
            query = pk[1]
            prev_time = new_image['prev_time']['S']
            curr_time = new_image['cur_time']['S']
            growth = new_image['growth']['N']
            keywords = f" {new_image['keyword']['S']} ({new_image['count_keyword']['N']}건)" 
            
            # Slack 메세지 발송
            msg = {"text": f"""
                🚨 [{channel}] {category} 이슈 감지 : 모니터링 필요

                - 검색어: {query}
                - 수집 시각: {curr_time} ~ {curr_time}
                - 급증 카테고리: {category} (+{growth}%)
                - 주요 키워드:{keywords}

                📍 알림 사유: '{category}' 관련 게시물 수치가 평소보다 급증했습니다.
                👉 관련 키워드들을 살펴보고 SNS 모니터링을 강화하세요.
                """}
            requests.post(slack_webhook_url, json = msg)

        # INSERT 명령이 아니면 무시
        else:
            print(f"{event_name} 이벤트이므로 아무 작업도 하지 않고 건너뜁니다.")
            continue


    return {
        'statusCode': 200,
        'body': json.dumps('모든 스트림 처리 완료!')
    }
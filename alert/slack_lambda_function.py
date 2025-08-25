import json
import boto3
import requests
import collections

# slack, DynamoDB 변수 설정
slack_webhook_url = '' # INPUT YOUR Webhook URL
table_name = '' # INPUT YOUR DynamoDB Table name

# DynamoDB 연결
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    """
    DynamoDB Stream 이벤트를 받아서
    - pk 기준으로 INSERT 데이터를 그룹핑하고
    - Slack 웹훅으로 알람 메시지를 전송하는 Lambda 함수
    """

    #그룹핑 데이터 생성
    grouped_data = collections.defaultdict(lambda: {
        'keywords_list': [],
        'common_data': {}
    })   

    # INSERT 데이터 처리
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            new_image = record['dynamodb']['NewImage'] # 요소 가져오기
            
            # 'pk' 기준으로 그룹핑(키워드 모으기)
            pk = new_image['pk']['S']
            keywords = f" {new_image['keyword']['S']} ({new_image['count_keyword']['N']}건)" 
            grouped_data[pk]['keywords_list'].append(keywords)
            grouped_data[pk]['common_data'] = new_image
    
    # 처리할 데이터가 없으면 함수 종료
    if not grouped_data:
        print("처리할 새로운 데이터가 없습니다.")
        return
    
    print(f'{len(grouped_data)}개의 그룹에 대한 알람 발송 시작')

    # 모든 데이터에 대한 Slack 알람 실행
    for pk_key, data in grouped_data.items():
        
        common = data['common_data']
        channel, query, category = pk_key.split('#')
        
        all_keywords = ", ".join(data['keywords_list'])
        
        # Slack 메시지 생성
        msg = {"text": f"""
            🚨 [{channel}] {category} 이슈 감지 : 모니터링 필요

            - 검색어: {query}
            - 수집 시각: {common['prev_time']['S']} ~ {common['cur_time']['S']}
            - 급증 카테고리: {category} (+{common['growth']['N']}%)
            - 주요 키워드:{all_keywords}
            
            👉 관련 키워드들을 살펴보고 SNS 모니터링을 강화하세요.
            """}
        
        # Slack Alert 발송
        requests.post(slack_webhook_url, json=msg)
        print(f"그룹[{pk_key}]에 대한 알람 발송.")


    return {
        'statusCode': 200,
        'body': json.dumps('모든 스트림 처리 완료!')
    }
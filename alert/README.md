# alert : Slack 알림 메세지 람다 함수

- 특정 리스크 카테고리의 언급량이 급증하여 임계값을 넘을 때 실행됩니다.

-  Slack으로 모니터링 권고 메세지를 보내 모니터링 담당자가 해당 카테고리의 SNS를 집중 모니터링 하게 합니다.

## 폴더 구조

- slack_lambda_function.py : Slack으로 모니터링 권고 메세지를 보내는 람다 함수 파일입니다.

## 슬랙 메시지 전송

- Spark Job 내에서 조건에 맞는 데이터가 DynamoDB로 적재되면 DynamoDB Streams가 동작하여 Lambda함수가 실행됩니다.

- 저장된 데이터의 정보를 이용하여 집중 모니터링을 권고하는 Slack Webhook 알림을 전송합니다.

- 스트림 데이터를 순회하며 같은 Partition Key를 가진 리스크 유형들의 Keyword와 그 갯수를 Defaultdict 자료구조에 모은 후 리스크 별로 한 번에 알림을 전송합니다.

## Slack 알림 실행 결과
![Slack 알림 실행 결과](images/alert.png)

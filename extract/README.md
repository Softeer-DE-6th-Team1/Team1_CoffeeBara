# Extract

## 개요

SNS(X, threads)에서 사전에 지정해둔 검색 쿼리를 기반으로 AWS Lambda를 사용해 30분 단위로 데이터를 수집해서 S3에 저장한다.

- 검색 쿼리: [avante, ioniq, niro, ev6, hyundai]
- 데이터 atrributes
  ```json
    { 
    username: str,
    uploaded_time:  timestamp,
    collected_time: timestamp,
    channel: text,
    query: text,
    text: text,
    }
  ```  
- extract 파이프라인
    - dispatch-scrapers lambda에 30분 주기의 EventBridge 트리거 
    - → x_scraper, threads_scraper lambda 실행 
    - → s3:raw-data에 {collected_time}/*.csv 로 저장

## dispatch-scrapers lambda

- dispatch-scrapers Lambda에 30분 주기의 EventBridge를 걸어서 각 scrapers lambda가 30분 마다 SNS에서 데이터를 수집하여 저장할 수 있도록 한다.

## scraper lambda

- 수집시각 기준 30분 이전까지 게시된 게시글의 데이터를 수집한다.
- 게시글 본문에 검색 쿼리가 있을 때만 저장한다.
- x_scraper lambda
    - X scraper의 lambda는 검색 쿼리를 순회하며 수집하여 하나의 csv 파일로 저장한다.
- threads_scraper lambda
    - threads는 키워드 당 수집시간이 약 5분 정도로 오래걸려 하나의 lambda에서 실행하면 타임아웃이 발생한다. 따라서 각 검색 쿼리마다 개별적인 lambda를 실행해서 타임아웃을 방지하고, 결과는 각각의 csv 파일로 저장된다.

## S3 Event Notification

- 실행되는 각 scrapers lambda에 scrapers-status DynamoDB를 연결시켜 오류 없이 실행되면 해당 수집시각의 count를 증가시켜 마지막으로 실행되는 lambda가 csv가 저장되는 디렉토리에 _SUCCESS 파일을 추가하도록한다.
- S3:raw-data에 _SUCCESS suffix를 가진 파일이 추가되면 Event Notification을 발생시켜 get-s3-data-to-ec2-spark lambda를 트리거한다.
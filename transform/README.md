# Transformer

## 개요
EC2 환경에서 Spark job 실행과 배포를 용이하게 하기 위해 Docker 기반 Spark Standalone 클러스터를 구성하였습니다.

- 파이프라인
    
    텍스트 데이터 수집 → 전처리 → word bag 매핑 → 집계 → 지표 계산 → 알림 전송
    

## Docker 세팅

### Entrypoints

- `entrypoint-master.sh` : Spark master 컨테이너 실행
- `entrypoint-worker.sh` : Spark worker 컨테이너 실행
- `entrypoint-history.sh` : Spark history 서버 실행

### Config

- `spark-defaults.conf` : Spark 실행 환경 변수 (S3, DB 연결, 드라이버 옵션)
- `workers` : 클러스터 worker 목록 정의

### Dockerfile

- 베이스 이미지: Amazon Linux 2023
- Apache Spark 3.5.1
- JAR
    - Hadoop S3A Connector
    - PostgreSQL JDBC Driver
- Spark UI 접근 포트
    - `7077` (master)
    - `8080` (master UI)
    - `8081`, 8082 (worker UI)
    - `4040` (job UI)
    - `18080` (history server)

### docker-compose.yml

- 구성: 1 Master + 2 Workers
- Spark 실행 모드: Standalone, client mode
- 볼륨 공유: `/home/softeer/jobs` → Spark job 스크립트 실행 경로
- Spark UI 접근:
    - Master UI → `http://<EC2_PUBLIC_IP>:8080`
    - Worker UI → `http://<EC2_PUBLIC_IP>:8081`

## EC2 세팅
- EC2 arm t4g.medium
- vCPU: 2개
- RAM: 4GB
- Storage: 32GB

## Spark Job 파이프라인

### 1. 텍스트 전처리

- S3 `raw-data` 버킷에서 수집된 데이터를 로드
- 불필요한 줄바꿈, 특수문자, 이모지 제거
- 단어 단위 분리를 수월하게 하기 위해 영어 데이터만 필터링

### 2. 텍스트 토큰화

- 공백/구두점을 기준으로 단어 단위 분리
- 최소 글자수 2 이상 단어만 허용
- 불용어(stopwords) 제거

### 3. Word Bag 매핑

- S3에서 미리 정의된 리스크 유형별 키워드 Word Bag 로드
- Spark broadcast join으로 키워드-리스크 매핑
- 중간 결과 checkpoint 및 EDA를 위해 결과를 mapped S3에 적재

### 4. 리스크/키워드별 집계

- GroupBy 집계
    - (수집시각, 채널, 검색 쿼리, 리스크 유형) 단위로 집계한 데이터프레임 반환
    - (수집시각, 채널, 검색 쿼리, 리스크 유형, 키워드) 단위로 집계한 데이터프레임 반환

### 5. 지표 계산 → RDS

Spark window를 사용하여 집계하고, 그 결과를 바탕으로 다양한 모니터링 지표 계산 후 RDS(PostgreSQL) 에 저장.

- 단기 급등 (30분 단위)
- 장기 추세지표
    
    `cur_count / moving_avg(1h)`
    
- 변동성 지표
    
    `stddev(count_category, 최근 1시간 데이터)`
    
- 누적 성장률 (최근 3회 연속 증가했으면 1, 아니면 0)
- 성장 지속시간 (threshold 초과 상태 3회 이상이면 1, 아니면 0)
- 상대적 중요도
    
    `category_count / total_count`
    
- 속도 (증가율의 증가율)
    
    `growth_t - growth_(t-1)`
    
- 추후 시계열 분석 및 대시보드에 활용하기 위하여 RDS에 적재

```sql
CREATE TABLE metrics (
    id SERIAL PRIMARY KEY,
    channel TEXT NOT NULL,
    query TEXT NOT NULL,
    category TEXT NOT NULL,
    cur_time TIMESTAMP NOT NULL,
    prev_time TIMESTAMP,
    cur_count INT,
    prev_count INT,
    short_term_growth DOUBLE PRECISION,
    long_term_ratio DOUBLE PRECISION,
    volatility DOUBLE PRECISION,
    ratio_to_total DOUBLE PRECISION,
    acceleration DOUBLE PRECISION,
    score DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT now()
);

-- 조회 속도를 위해 인덱스 추가
CREATE INDEX idx_metrics_time
    ON metrics (channel, query, category, cur_time);
```

### 6. Alert

- Risk score가 threshold를 넘는 데이터만 필터링하여 Alert DynamoDB에 적재
- 추후 알림을 보내는 목적으로 사용


## Spark Job 실행방법
```{bash}
# Spark job 실행
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /home/softeer/jobs/spark-job.py
```
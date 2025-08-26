from pyspark.sql import SparkSession, Row
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer
from pyspark.sql.functions import (
    col, expr, explode, array_intersect, array,
    lit, size, lower, trim, broadcast, regexp_replace,
)
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import boto3
import os
import sys
import logging


def preprocess_text(df, input_col="text", output_col="clean_text"):
    """
    텍스트 컬럼을 전처리:
      - 줄바꿈 제거
      - 영어 알파벳과 숫자만 남기고 나머지 문자(한글, 아랍어, 이모지 등) 제거
      - 다중 공백 → 단일 공백
    """
    df_clean = (
        df.withColumn(
            output_col,
            regexp_replace(col(input_col), r"[\r\n]+", " ")  # 줄바꿈 → 공백
        )
        .withColumn(
            output_col,
            regexp_replace(col(output_col), r"[^A-Za-z0-9\s]", " ")  # 영어/숫자 외 제거
        )
        .withColumn(
            output_col,
            regexp_replace(col(output_col), r"\s+", " ")  # 다중 공백 → 단일 공백
        )
        .withColumn(output_col, trim(col(output_col)))  # 앞뒤 공백 제거
    )
    return df_clean


def text_to_words(spark, bucket_name:str, file_path:str):
    """
    S3 CSV 파일을 읽고 text 컬럼에 대해:
      1. 토큰화
      2. 불용어 제거
    수행한 DataFrame을 반환
    """
    # S3에서 csv 파일 읽기 (버킷/경로 맞게 수정)
    raw_df = spark.read.csv(
        f"s3a://{bucket_name}/{file_path}",
        header=True,
        multiLine=True,   # 줄바꿈 포함 텍스트 처리
        quote='"',        # 따옴표 안의 콤마는 무시
        escape='"'        # 따옴표 이스케이프 처리
    )

    # 이모지, 줄바꿈 전처리
    clean_df = preprocess_text(raw_df, input_col="text", output_col="clean_text")
    
    # 문장을 단어 단위로 분리
    tokenizer = RegexTokenizer(
        inputCol="clean_text",
        outputCol="words",
        pattern="\\W",   # 단어 아닌 것(공백, 구두점 등) 기준 split
        minTokenLength=2 # 최소 글자수
    )
    df_words = tokenizer.transform(clean_df)

    # 불용어 제거
    remover = StopWordsRemover(
        inputCol="words",
        outputCol="filtered_words"
    )

    df_filtered = remover.transform(df_words)
    return df_filtered


def collect_keywords(wordbag_df):
    """
    wordbag_df → keyword array를 collect해서 Spark literal array 반환
    """
    keywords = [row["keyword"] for row in wordbag_df.collect()]
    return array([lit(k) for k in keywords])


def map_with_wordbag(spark, df_filtered, bucket_name:str, wordbag_path:str):
    """
    S3의 wordbag(category, keyword)과 broadcast join하여
    text에 포함된 keyword를 매핑 → multirow 데이터프레임으로 반환
    매칭 안된 경우 category/keyword = 'no'
    """
    # wordbag 로드
    wordbag_df = spark.read.csv(
        f"s3a://{bucket_name}/{wordbag_path}",
        header=True,
    )

    # 소문자 정규화
    wordbag_df = wordbag_df.withColumn("category", trim(lower(col("category"))))
    wordbag_df = wordbag_df.withColumn("keyword", trim(lower(col("keyword"))))

    df_filtered = df_filtered.withColumn(
        "filtered_words",
        expr("transform(filtered_words, x -> lower(x))")
    )

    # 매칭되는 키워드 추출
    df_matched = df_filtered.withColumn(
        "matched",
        array_intersect(col("filtered_words"), collect_keywords(wordbag_df))
    )

    df_hit = df_matched.filter(size(col("matched")) > 0) \
        .withColumn("keyword", explode(col("matched"))) \
        .join(broadcast(wordbag_df), on="keyword", how="left")
    
    df_miss = df_matched.filter(size(col("matched")) == 0) \
        .withColumn("category", lit("no")) \
        .withColumn("keyword", lit("no"))
    
    result = df_hit.select("username","uploaded_time","collected_time",
                           "channel","query","text","category","keyword") \
        .unionByName(
            df_miss.select("username","uploaded_time","collected_time",
                            "channel","query","text","category","keyword")
        )
    
    return result


def count_category_and_keywords(df_mapped):
    """
    category 단위 집계(cat_counts)와
    category+keyword 단위 집계(cat_kw_result)를 모두 반환
    """
    # 카테고리별 전체 건수
    df_cat = (
        df_mapped
        .filter(col("category") != "no")
        .groupBy("collected_time", "channel", "query", "category")
        .count()
        .withColumnRenamed("count", "count_category")
    )

    # 카테고리 + 키워드별 건수
    df_cat_kw_counts = (
        df_mapped
        .filter(col("category") != "no")
        .groupBy("collected_time", "channel", "query", "category", "keyword")
        .count()
        .withColumnRenamed("count", "count_keyword")
    )

    # keyword 집계에 category 집계 붙이기
    df_cat_kw= (
        df_cat_kw_counts
        .join(
            df_cat,
            on=["collected_time", "channel", "query", "category"],
            how="left"
        )
        .select(
            "collected_time", "channel", "query", "category",
            "keyword", "count_category", "count_keyword"
        )
        .orderBy("count_category", "count_keyword", ascending=[False, False])
    )

    return df_cat, df_cat_kw


def save_to_category_ddb(df_cat, table_name: str, region="ap-northeast-2"):
    """
    df_count에서 category 단위만 추출해 DDB_COUNT 테이블에 저장
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    # category 단위로 중복 제거. 수집 주기가 짧으므로 collect로 수집해도 OOM 확률 적음.
    rows = (
        df_cat
        .select("collected_time", "channel", "query", "category", "count_category")
        .distinct()
        .collect()
    )

    for row in rows:
        pk = f"{row['channel']}#{row['query']}#{row['category']}"
        sk = f"{row['collected_time']}"

        item = {
            "pk": pk,
            "sk": sk,
            "channel": row["channel"],
            "query": row["query"],
            "collected_time": row['collected_time'],
            "category": row["category"],
            "count_category": int(row["count_category"])
        }
        table.put_item(Item=item)


def get_prev_item(table, pk, prev_time):
    """
    이전 시점의 데이터를 dynamodb로부터 가져오기
    """
    try:
        response = table.get_item(
            Key={
                "pk": pk,
                "sk": prev_time
            }
        )
        return response.get("Item")
    except Exception as e:
        print(f"[WARN] DynamoDB get_item error: {e}")
        return None


def calc_short_term_growth(cur_count, prev_count):
    """
    30분 단위 단기 증가율
    """
    if prev_count is None or prev_count <= 0:
        prev_count = 1
    if cur_count is None:
        cur_count = 0
    return (cur_count - prev_count) / prev_count


def calc_long_term_ratio(cur_count, history):
    """
    최근 3회(1시간반) 평균 대비 현재 30분 count 비율
    """
    vals = [int(x) for x in history if x is not None and x > 0]
    if not vals:
        return 0.0
    moving_avg = sum(vals) / len(vals)
    if moving_avg <= 0:
        return 0.0
    cur_count = 0 if cur_count is None else cur_count
    return cur_count / moving_avg


def calc_volatility(history):
    """
    최근 6회(3시간) stddev/mean
    """
    vals = [int(x) for x in history if x is not None and x >= 0]
    if len(vals) < 2:
        return 0.0
    mu = sum(vals) / len(vals)
    if mu <= 0:
        return 0.0
    sigma = (sum([(x - mu) ** 2 for x in vals]) / (len(vals) - 1)) ** 0.5
    return sigma / mu


def calc_duration_above_threshold(history_growth, threshold=2.0):
    """
    최근 3회 growth가 threshold 초과했는지
    """
    vals = [g for g in history_growth if g is not None]
    if len(vals) < 3:
        return 0
    return 1 if all(g > threshold for g in vals[-3:]) else 0


def calc_ratio_to_total(cur_count, total_count):
    """
    전체 대비 점유율
    """
    if cur_count is None:
        cur_count = 0
    if total_count is None or total_count <= 0:
        return 0.0
    return cur_count / total_count


def calc_acceleration(growth, prev_growth):
    """
    성장 속도 변화량
    """
    if growth is None:
        growth = 0.0
    if prev_growth is None:
        return 0.0
    return growth - prev_growth


def calculate_score(cur_count: int, prev_count: int, growth: float) -> float:
    """
    최종 score 계산 (현재는 growth 그대로 사용)
    향후 다른 지표를 조합 가능
    """
    return growth


def calculate_metrics(spark, df_count, table_name, weights, region="ap-northeast-2"):
    """
    현재 DataFrame과 DynamoDB에 있는 직전 시점 데이터를 비교해서
    단기증가율, 장기추세, 변동성, 연속성, 지속시간, 점유율, 가속도, 최종 score 등을 계산
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    rows = df_count.collect()
    result_rows = []

    for row in rows:
        pk = f"{row['channel']}#{row['query']}#{row['category']}"
        cur_time = datetime.fromisoformat(row['collected_time'].replace("Z", "+00:00"))

        # 현재 count
        cur_count = row['count_category'] if row['count_category'] is not None else 0

        # 직전 시점 (30분 전)
        prev_time = (cur_time - timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
        prev_item = get_prev_item(table, pk, prev_time)
        prev_count = int(prev_item['count_category']) if prev_item and prev_item.get("count_category") else 1

        # 직전 growth (가속도 계산용)
        prev_growth = None
        prev_prev_time = (cur_time - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
        prev_prev_item = get_prev_item(table, pk, prev_prev_time)
        if prev_prev_item and prev_prev_item.get("count_category") is not None:
            count_pp = int(prev_prev_item['count_category'])
            prev_growth = calc_short_term_growth(prev_count, count_pp)

        # 최근 history (6회=3시간)
        hist_counts = []
        for i in range(1, 7):
            past_time = (cur_time - timedelta(minutes=30 * i)).isoformat().replace("+00:00", "Z")
            past_item = get_prev_item(table, pk, past_time)
            hist_counts.append(int(past_item['count_category']) if past_item and past_item.get("count_category") else None)

        # history로부터 growth 시계열
        hist_growth = []
        for i in range(1, len(hist_counts)):
            if hist_counts[i] is not None and hist_counts[i-1] is not None:
                hist_growth.append(calc_short_term_growth(hist_counts[i], hist_counts[i-1]))
            else:
                hist_growth.append(None)

        # ---- 지표 계산 ----
        short_term_growth = calc_short_term_growth(cur_count, prev_count)
        long_term_ratio   = calc_long_term_ratio(cur_count, hist_counts[:3])   # 최근 3회
        volatility        = calc_volatility(hist_counts)
        duration          = calc_duration_above_threshold(hist_growth, threshold=2.0)

        # 전체 대비 점유율
        total_count = df_count.filter(
            (col("collected_time") == row['collected_time']) &
            (col("channel") == row['channel']) &
            (col("query") == row['query'])
        ).agg({"count_category":"sum"}).collect()[0][0]
        ratio_to_total = calc_ratio_to_total(cur_count, total_count)

        # 가속도
        acceleration = calc_acceleration(short_term_growth, prev_growth)

        # 최종 score
        score = (
            weights[0] * short_term_growth +
            weights[1] * long_term_ratio +
            weights[2] * ratio_to_total +
            weights[3] * volatility +
            weights[4] * acceleration
        )

        result_rows.append(Row(
            pk=pk,
            channel=row["channel"],
            query=row["query"],
            category=row["category"],
            cur_time=row['collected_time'],
            prev_time=prev_time,
            cur_count=cur_count,
            prev_count=prev_count,
            short_term_growth=short_term_growth,
            long_term_ratio=long_term_ratio,
            volatility=volatility,
            duration=duration,
            ratio_to_total=ratio_to_total,
            acceleration=acceleration,
            score=score
        ))

    return spark.createDataFrame(result_rows)


def save_to_rds(df_metrics, db_name):
    """
    df_metrics DataFrame을 RDS(Postgres) metrics 테이블에 저장
    """
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")

    jdbc_url = f"jdbc:postgresql://softeer-risk-metrics.cnec0os4gsir.ap-northeast-2.rds.amazonaws.com:5432/{db_name}"
    
    # 시간 컬럼 캐스팅
    df_casted = (
        df_metrics
        .withColumn("cur_time", col("cur_time").cast(TimestampType()))
        .withColumn("prev_time", col("prev_time").cast(TimestampType()))
    )

    (
        df_casted.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metrics")        # 방금 만든 테이블 이름
        .option("user", db_user)          # RDS 유저명
        .option("password",db_pass)      # RDS 접속 비밀번호
        .option("driver", "org.postgresql.Driver")
        .mode("append")                      # 데이터 누적 저장
        .save()
    )


def extract_alert(df_metrics, df_count, threshold):
    """
    score가 threshold 이상인 category에 대해 keyword join 결과 반환
    """
    # score가 threshold를 넘는 category 필터
    # df_risk = df_metrics.filter((col("score") > threshold) & (col("duration") == 1)).select(
    df_risk = df_metrics.filter((col("score") > threshold)).select(
        "pk",
        "channel",
        "query",
        "category",
        "cur_time",
        "prev_time",
        "cur_count",
        "prev_count",
        "short_term_growth",
        "long_term_ratio",
        "volatility",
        "duration",
        "ratio_to_total",
        "acceleration",
        "score"
    )

    # df_count (count_category)와 join하여 keyword 단위 연결
    df_alert = (
        df_risk.alias("r")
        .join(
            df_count.alias("c"),
            (
                (col("r.channel") == col("c.channel")) &
                (col("r.query") == col("c.query")) &
                (col("r.category") == col("c.category")) &
                (col("r.cur_time") == col("c.collected_time"))
            ),
            how="inner"
        )
        .select(
            col("r.pk"),
            col("r.channel"),
            col("r.query"),
            col("r.category"),
            col("r.cur_time"),
            col("r.prev_time"),
            col("r.cur_count"),
            col("r.prev_count"),
            col("c.keyword"),
            col("c.count_keyword"),
            col("r.short_term_growth"),
            col("r.long_term_ratio"),
            col("r.ratio_to_total"),
            col("r.score")
        )
        .orderBy(col("r.score").desc(), col("c.count_keyword").desc())
    )

    return df_alert


def save_to_alert_ddb(df_alert, table_name: str, region="ap-northeast-2"):
    """
    df_alert (threshold 넘은 category + keyword join 결과)를
    Alert DynamoDB 테이블에 저장
    - 알림 메시지 생성에 필요한 모든 컬럼 저장
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    rows = df_alert.collect()

    for row in rows:
        pk = f"{row['channel']}#{row['query']}#{row['category']}"
        sk = f"{row['cur_time']}#{row['keyword']}"  # keyword 단위로 고유하게 저장

        item = {
            "pk": pk,
            "sk": sk,
            "channel": row["channel"],
            "query": row["query"],
            "category": row["category"],
            "cur_time": row["cur_time"],
            "prev_time": row["prev_time"],      # 이전 시각
            "cur_count": int(row["cur_count"]), # 현재 카테고리 건수
            "prev_count": int(row["prev_count"]),
            "keyword": row["keyword"],
            "count_keyword": int(row["count_keyword"]),
            "short_term_growth": Decimal(str(row["short_term_growth"])),
            "long_term_ratio": Decimal(str(row["long_term_ratio"])),
            "ratio_to_total": Decimal(str(row["ratio_to_total"])),
            "score": Decimal(str(row["score"]))
        }
        table.put_item(Item=item)


if __name__ == "__main__":
    # 외부에서 전달받은 S3 파일 경로 인자 확인
    if len(sys.argv) < 2:
        print("에러: 처리할 S3 파일 경로를 인자로 전달해야 합니다.")
        # 예: spark-submit spark-job.py s3a://my-bucket/x-data/new-file.csv
        sys.exit(1)

    s3_raw_path_from_arg = sys.argv[1] # 첫 번째 인자를 파일 경로로 사용
    print(f"전달받은 파일 경로: {s3_raw_path_from_arg}")

    # team
    _BUCKET_NAME="softeer-de-6th-team1"
    # _S3_RAW_PATH="out/20250801T000000Z.csv" # local test
    _S3_WORDBAG_PATH="configs/wordbag.csv"
    _S3_MAPPED_PATH="mapped"
    _DDB_COUNT_TABLE = "softeer-count"
    _DDB_ALERT_TABLE = "softeer-alert"
    _RDS_NAME = "postgres"

    _WEIGHTS = [0.4, 0.2, 0.2, 0.1, 0.1]
    threshold = 2.0

    # 로그 기본 설정
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(__name__)

    logger.info("=== Spark Job 시작 ===")

    # spark session 생성
    spark = SparkSession.builder.appName("SparkJobTest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # 또는 "ERROR"

    # text를 word로 분리
    logger.info("1. 텍스트 → 단어 분리")
    df_words = text_to_words(spark, _BUCKET_NAME, s3_raw_path_from_arg)

    # wordbag과 매핑
    logger.info("2. Wordbag 매핑")
    df_mapped = map_with_wordbag(spark, df_words, _BUCKET_NAME, _S3_WORDBAG_PATH)

    # 중간 결과 s3에 저장
    logger.info("3. 매핑 결과 S3 저장")
    mapped_save_path = f"s3a://{_BUCKET_NAME}/{_S3_MAPPED_PATH}"
    df_mapped.write.mode("overwrite") \
        .option("header", "true") \
        .csv(mapped_save_path)
    
    # channel, query, category 집계
    logger.info("4. 카테고리/키워드 집계")
    df_cat, df_cat_kw = count_category_and_keywords(df_mapped)

    # category별 집계 결과를 ddb_count에 저장
    logger.info("5. DynamoDB(count) 저장")
    save_to_category_ddb(df_cat, _DDB_COUNT_TABLE)

    # 순간증가율, 이동평균, 최종 score 계산
    logger.info("6. 지표 계산")
    df_metrics = calculate_metrics(spark, df_cat, _DDB_COUNT_TABLE, _WEIGHTS)

    # rds에 지표 계산 결과 저장 -> 추후 대시보드에 연결
    logger.info("7. RDS 저장")
    save_to_rds(df_metrics, _RDS_NAME)

    # risk score이 특정 threshold 이상인 category 추출
    logger.info("8. Alert 추출")
    df_alert = extract_alert(df_metrics, df_cat_kw, threshold=threshold)

    # 필터링 결과를 ddb_alert에 저장
    logger.info("9. DynamoDB(alert) 저장")
    save_to_alert_ddb(df_alert, _DDB_ALERT_TABLE)

    logger.info("=== Spark Job 완료 ===")
    spark.stop()

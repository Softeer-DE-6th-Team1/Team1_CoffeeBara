from pyspark.sql import SparkSession, Row
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer
from pyspark.sql.functions import (
    col, expr, explode, array_intersect, array,
    lit, size, lower, trim, broadcast
)
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import boto3

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

    # 문장을 단어 단위로 분리
    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="words",
        pattern="\\W",   # 단어 아닌 것(공백, 구두점 등) 기준 split
        minTokenLength=2 # 최소 글자수
    )
    df_words = tokenizer.transform(raw_df)

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


def calculate_growth(cur_count: int, prev_count: int) -> float:
    """
    증가율 계산
    """
    if prev_count == 0:
        return float("inf")  # 0으로 나눔 방지
    return (cur_count - prev_count) / prev_count


def calculate_score(cur_count: int, prev_count: int, growth: float) -> float:
    """
    최종 score 계산 (현재는 growth 그대로 사용)
    향후 다른 지표를 조합 가능
    """
    return growth


def calculate_metrics(spark, df_count, table_name, region="ap-northeast-2"):
    """
    현재 DataFrame과 DynamoDB에 있는 직전 시점 데이터를 비교해서
    growth, score 등을 계산한 최종 DataFrame 반환
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    rows = df_count.collect()
    result_rows = []

    for row in rows:
        pk = f"{row['channel']}#{row['query']}#{row['category']}"

        # date 형식 통일
        cur_time = datetime.fromisoformat(row['collected_time'].replace("Z", "+00:00"))
        prev_time = (cur_time - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")

        # DynamoDB에서 이전 데이터 조회
        prev_item = get_prev_item(table, pk, prev_time)

        cur_count = int(row['count_category'])
        prev_count = int(prev_item['count_category']) if prev_item else 1

        # metrics 계산
        growth = calculate_growth(cur_count, prev_count)
        score = calculate_score(cur_count, prev_count, growth)

        result_rows.append(Row(
            pk=pk,
            channel=row["channel"],
            query=row["query"],
            category=row["category"],
            cur_time=row['collected_time'],
            prev_time=prev_time,
            cur_count=cur_count,
            prev_count=prev_count,
            growth=growth,
            score=score
        ))

    return spark.createDataFrame(result_rows)


def extract_alert(df_metrics, df_count, threshold):
    """
    score가 threshold 이상인 category에 대해 keyword join 결과 반환
    """
    # score가 threshold를 넘는 category 필터
    df_risk = df_metrics.filter(col("score") > threshold).select(
        "pk",
        "channel",
        "query",
        "category",
        "cur_time",
        "prev_time",
        "cur_count",
        "prev_count",
        "growth",
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
            col("r.growth"),
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
            "growth": Decimal(row["growth"]),
            "score": Decimal(row["score"]),
            "keyword": row["keyword"],
            "count_keyword": int(row["count_keyword"])
        }

        table.put_item(Item=item)


if __name__ == "__main__":
    # team
    _BUCKET_NAME="softeer-de-6th-team1"
    _S3_RAW_PATH="out/20250801T000000Z.csv"
    _S3_WORDBAG_PATH="configs/wordbag.csv"
    _S3_MAPPED_PATH="mapped"
    _DDB_COUNT_TABLE = "softeer-count"
    _DDB_SCORE_TABLE = "softeer-score"
    _DDB_ALERT_TABLE = "softeer-alert"

    # 개인
    # _BUCKET_NAME="mariahwy-softeer-test"
    # _S3_RAW_PATH="out/20250801T000000Z.csv"
    # _S3_WORDBAG_PATH="configs/wordbag.csv"
    # _S3_MAPPED_PATH="mapped"
    # _DDB_COUNT_TABLE = "softeer-count"
    # _DDB_SCORE_TABLE = "softeer-score"
    # _DDB_ALERT_TABLE = "softeer-alert"

    # spark session 생성
    spark = SparkSession.builder.appName("SparkJobTest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # 또는 "ERROR"

    # text를 word로 분리
    df_words = text_to_words(spark, _BUCKET_NAME, _S3_RAW_PATH)

    # wordbag과 매핑
    df_mapped = map_with_wordbag(spark, df_words, _BUCKET_NAME, _S3_WORDBAG_PATH)

    # 중간 결과 s3에 저장
    mapped_save_path = f"s3a://{_BUCKET_NAME}/{_S3_MAPPED_PATH}"
    df_mapped.write.mode("overwrite") \
        .option("header", "true") \
        .csv(mapped_save_path)
    
    # channel, query, category 집계
    df_cat, df_cat_kw = count_category_and_keywords(df_mapped)

    # category별 집계 결과를 ddb_count에 저장
    save_to_category_ddb(df_cat, _DDB_COUNT_TABLE)

    # 순간증가율, 이동평균, 최종 score 계산
    df_metrics = calculate_metrics(spark, df_cat, _DDB_COUNT_TABLE)

    # risk score이 특정 threshold 이상인 category 추출
    df_alert = extract_alert(df_metrics, df_cat_kw, threshold=4.0)

    # 필터링 결과를 ddb_alert에 저장
    save_to_alert_ddb(df_alert, _DDB_ALERT_TABLE)

    # 예시 출력
    df_alert.show(truncate=False)

    spark.stop()

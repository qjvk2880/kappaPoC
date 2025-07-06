from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, concat_ws
from pyspark.sql.types import *

# SparkSession 생성
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka에서 읽어올 schema 정의
schema = StructType([
    StructField("우편번호", StringType()),
    StructField("시도", StringType()),
    StructField("시군구", StringType()),
    StructField("법정동명", StringType()),
    StructField("지번본번", StringType()),
    StructField("지번부번", StringType()),
    StructField("도로명", StringType()),
    StructField("건물번호본번", StringType()),
    StructField("건물번호부번", StringType())
])

# Kafka에서 stream read
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "real-estate-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# value를 string으로 변환 후 json parse
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# 컬럼명 변경
final_df = parsed_df.withColumnRenamed("우편번호", "zip_code")

# 도로명 주소 컬럼 추가
final_df = final_df.withColumn(
    "road_address",
    concat_ws(
        " ",
        col("시도"),
        col("시군구"),
        col("도로명"),
        col("건물번호본번"),
        when(
            (col("건물번호부번").isNotNull()) & (col("건물번호부번") != "0"),
            lit("-") + col("건물번호부번")
        ).otherwise(lit(""))
    )
)

# 지번 주소 컬럼 추가
final_df = final_df.withColumn(
    "lot_address",
    concat_ws(
        " ",
        col("시도"),
        col("시군구"),
        col("법정동명"),
        col("지번본번"),
        when(
            (col("지번부번").isNotNull()) & (col("지번부번") != "0"),
            lit("-") + col("지번부번")
        ).otherwise(lit(""))
    )
)

# PostgreSQL JDBC sink
final_df.select("zip_code", "road_address", "lot_address").writeStream \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .foreachBatch(lambda batch_df, _: batch_df.write \
                  .format("jdbc") \
                  .option("url", "jdbc:postgresql://postgres:5432/real_estate") \
                  .option("dbtable", "property") \
                  .option("user", "user") \
                  .option("password", "password") \
                  .option("driver", "org.postgresql.Driver") \
                  .mode("append") \
                  .save()
                  ) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
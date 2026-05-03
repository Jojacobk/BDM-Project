import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages "
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_PREFIX = os.environ.get("CDC_TOPIC_PREFIX", "dbserver1")
CDC_TOPICS = f"{TOPIC_PREFIX}.public.customers,{TOPIC_PREFIX}.public.drivers"
BRONZE_TABLE = "lakehouse.cdc.bronze"


def spark_session():
    return (
        SparkSession.builder
        .appName("bronze_cdc")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def ensure_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            kafka_topic     STRING,
            kafka_partition INT,
            kafka_offset    LONG,
            kafka_timestamp TIMESTAMP,
            op              STRING,
            ts_ms           LONG,
            source_table    STRING,
            source_lsn      LONG,
            before_json     STRING,
            after_json      STRING,
            raw_value       STRING,
            is_tombstone    BOOLEAN,
            ingested_at     TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('write.target-file-size-bytes' = '67108864')
    """)

    existing = {field.name for field in spark.table(BRONZE_TABLE).schema.fields}
    for column_name, column_type in [
        ("source_lsn", "LONG"),
        ("raw_value", "STRING"),
        ("is_tombstone", "BOOLEAN"),
    ]:
        if column_name not in existing:
            spark.sql(f"ALTER TABLE {BRONZE_TABLE} ADD COLUMN {column_name} {column_type}")


def run(spark):
    raw_stream = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", CDC_TOPICS)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    value_str = F.col("value").cast("string")
    bronze_df = raw_stream.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.get_json_object(value_str, "$.payload.op").alias("op"),
        F.get_json_object(value_str, "$.payload.ts_ms").cast("long").alias("ts_ms"),
        F.get_json_object(value_str, "$.payload.source.table").alias("source_table"),
        F.get_json_object(value_str, "$.payload.source.lsn").cast("long").alias("source_lsn"),
        F.get_json_object(value_str, "$.payload.before").alias("before_json"),
        F.get_json_object(value_str, "$.payload.after").alias("after_json"),
        value_str.alias("raw_value"),
        F.col("value").isNull().alias("is_tombstone"),
        F.current_timestamp().alias("ingested_at"),
    )
    bronze_df = bronze_df.select(*spark.table(BRONZE_TABLE).columns)

    existing_offsets = spark.table(BRONZE_TABLE).select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    )
    new_rows = bronze_df.join(
        existing_offsets,
        ["kafka_topic", "kafka_partition", "kafka_offset"],
        "left_anti",
    )

    new_count = new_rows.count()
    if new_count == 0:
        print("[bronze_cdc] No new Kafka offsets to append.")
        return

    new_rows.writeTo(BRONZE_TABLE).append()
    print(f"[bronze_cdc] Appended {new_count} new CDC event(s).")


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark)
    print(f"[bronze_cdc] Reading topics: {CDC_TOPICS}")
    run(spark)
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}").collect()[0]["cnt"]
    print(f"[bronze_cdc] {BRONZE_TABLE} rows: {count}")
    spark.stop()


if __name__ == "__main__":
    main()

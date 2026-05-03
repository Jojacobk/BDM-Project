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
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TAXI_TOPIC = os.environ.get("TAXI_TOPIC", "taxi-trips")
BRONZE_TABLE = "lakehouse.taxi.bronze_trips"

TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
])


def spark_session():
    return (
        SparkSession.builder
        .appName("bronze_taxi")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def ensure_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            VendorID                INT,
            tpep_pickup_datetime    TIMESTAMP_NTZ,
            tpep_dropoff_datetime   TIMESTAMP_NTZ,
            passenger_count         LONG,
            trip_distance           DOUBLE,
            RatecodeID              LONG,
            store_and_fwd_flag      STRING,
            PULocationID            INT,
            DOLocationID            INT,
            payment_type            LONG,
            fare_amount             DOUBLE,
            extra                   DOUBLE,
            mta_tax                 DOUBLE,
            tip_amount              DOUBLE,
            tolls_amount            DOUBLE,
            improvement_surcharge   DOUBLE,
            total_amount            DOUBLE,
            congestion_surcharge    DOUBLE,
            Airport_fee             DOUBLE,
            cbd_congestion_fee      DOUBLE,
            trip_id                 LONG,
            source_file             STRING,
            kafka_topic             STRING,
            kafka_partition         INT,
            kafka_offset            LONG,
            kafka_timestamp         TIMESTAMP,
            raw_json                STRING,
            ingested_at             TIMESTAMP
        )
        USING iceberg
    """)

    existing = {field.name for field in spark.table(BRONZE_TABLE).schema.fields}
    for column_name, column_type in [
        ("kafka_topic", "STRING"),
        ("kafka_partition", "INT"),
        ("kafka_offset", "LONG"),
        ("kafka_timestamp", "TIMESTAMP"),
        ("raw_json", "STRING"),
    ]:
        if column_name not in existing:
            spark.sql(f"ALTER TABLE {BRONZE_TABLE} ADD COLUMN {column_name} {column_type}")


def run(spark):
    raw_stream = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TAXI_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    raw_json = F.col("value").cast("string")
    parsed = F.from_json(raw_json, TAXI_SCHEMA)

    bronze_df = raw_stream.select(
        parsed.alias("trip"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        raw_json.alias("raw_json"),
    ).filter(F.col("raw_json").isNotNull())

    for field in TAXI_SCHEMA.fields:
        bronze_df = bronze_df.withColumn(field.name, F.col(f"trip.{field.name}"))

    bronze_df = (
        bronze_df
        .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime").cast("timestamp_ntz"))
        .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime").cast("timestamp_ntz"))
        .withColumn("trip_id", F.xxhash64("kafka_topic", "kafka_partition", "kafka_offset"))
        .withColumn("source_file", F.lit(f"kafka:{TAXI_TOPIC}"))
        .withColumn("ingested_at", F.current_timestamp())
        .drop("trip")
        .select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "RatecodeID",
            "store_and_fwd_flag",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "Airport_fee",
            "cbd_congestion_fee",
            "trip_id",
            "source_file",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "raw_json",
            "ingested_at",
        )
    )
    bronze_df = bronze_df.select(*spark.table(BRONZE_TABLE).columns)

    existing_offsets = (
        spark.table(BRONZE_TABLE)
        .filter(F.col("kafka_topic").isNotNull())
        .select("kafka_topic", "kafka_partition", "kafka_offset")
    )
    new_rows = bronze_df.join(
        existing_offsets,
        ["kafka_topic", "kafka_partition", "kafka_offset"],
        "left_anti",
    )

    new_count = new_rows.count()
    if new_count == 0:
        print("[bronze_taxi] No new Kafka offsets to append.")
        return

    new_rows.writeTo(BRONZE_TABLE).append()
    print(f"[bronze_taxi] Appended {new_count} new taxi event(s).")


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark)
    print(f"[bronze_taxi] Reading topic: {TAXI_TOPIC}")
    run(spark)
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}").collect()[0]["cnt"]
    print(f"[bronze_taxi] {BRONZE_TABLE} rows: {count}")
    spark.stop()


if __name__ == "__main__":
    main()

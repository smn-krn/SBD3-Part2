from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lower,
    count,
    window,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

# ---------------------------------------
# 1. Spark Session & Configuration
# ---------------------------------------
CHECKPOINT_PATH = "/tmp/spark-checkpoints/activity-3"

spark = (
    SparkSession.builder
    .appName("Activity3-CrashMonitoring")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------
# 2. Define Kafka message schema
# ---------------------------------------
schema = StructType([
    StructField("timestamp", LongType()),     # epoch seconds
    StructField("status", StringType()),
    StructField("severity", StringType()),
    StructField("source_ip", StringType()),
    StructField("user_id", StringType()),
    StructField("content", StringType())
])

# ---------------------------------------
# 3. Read stream from Kafka
# ---------------------------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "logs")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# ---------------------------------------
# 4. Parse JSON & extract fields
# ---------------------------------------
parsed_df = (
    raw_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# ---------------------------------------
# 5. Event-time processing
# ---------------------------------------
# Convert epoch seconds to Spark Timestamp
events_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

# ---------------------------------------
# 6. Filtering logic (Activity 3 rules)
# ---------------------------------------
filtered_df = (
    events_df
    .filter(lower(col("content")).contains("crash"))
    .filter(col("severity").isin("High", "Critical"))
)

# ---------------------------------------
# 7. Windowed aggregation
# ---------------------------------------
aggregated_df = (
    filtered_df
    .withWatermark("event_time", "20 seconds")
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("user_id")
    )
    .agg(count("*").alias("crash_count"))
    .filter(col("crash_count") > 2)
)

# ---------------------------------------
# 8. Output to console
# ---------------------------------------
query = (
    aggregated_df
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()

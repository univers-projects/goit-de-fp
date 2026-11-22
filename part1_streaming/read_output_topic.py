"""Print aggregated records from the output Kafka topic (for the screenshot)."""

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from config import build_spark, load_settings

OUTPUT_SCHEMA = StructType(
    [
        StructField("sport", StringType()),
        StructField("medal", StringType()),
        StructField("sex", StringType()),
        StructField("country_noc", StringType()),
        StructField("avg_height", DoubleType()),
        StructField("avg_weight", DoubleType()),
        StructField("timestamp", StringType()),
    ]
)


def main() -> None:
    spark = build_spark("ReadOutputTopic")
    settings = load_settings()
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_output_topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(from_json(col("value").cast("string"), OUTPUT_SCHEMA).alias("data"))
        .select("data.*")
    )
    print(f"Records in topic {settings.kafka_output_topic}: {df.count()}")
    df.show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

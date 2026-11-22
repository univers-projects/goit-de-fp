"""Stage 3 (part 1): read athlete_event_results from MySQL and publish it to Kafka as JSON."""

import logging

from pyspark.sql.functions import col, struct, to_json

from config import build_spark, load_settings

logger = logging.getLogger(__name__)

SOURCE_TABLE = "athlete_event_results"


def main() -> None:
    spark = build_spark("MySQLToKafka")
    settings = load_settings()

    # Етап 3: зчитування athlete_event_results з MySQL
    events_df = spark.read.format("jdbc").options(**settings.jdbc_options(SOURCE_TABLE)).load()

    # Етап 3: запис у Kafka-топік athlete_event_results (кожен рядок -> JSON)
    kafka_df = events_df.select(
        col("athlete_id").cast("string").alias("key"),
        to_json(struct(*events_df.columns)).alias("value"),
    )
    (
        kafka_df.write.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("topic", settings.kafka_input_topic)
        .save()
    )
    logger.info(
        "Published %s rows to topic %s", events_df.count(), settings.kafka_input_topic
    )
    spark.stop()


if __name__ == "__main__":
    main()

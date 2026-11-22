"""End-to-end streaming pipeline: Kafka events + MySQL bio -> aggregates -> Kafka + MySQL.

Run produce_event_results.py first so the input topic has data.

    python streaming_pipeline.py                 # runs until stopped (Ctrl+C)
    python streaming_pipeline.py --available-now # drains the topic in micro-batches, then exits
"""

import argparse
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct, to_json

from config import Settings, build_spark, load_settings
from mysql_schema import ensure_output_table
from transformations import aggregate_stats, enrich_events, filter_valid_bio, parse_kafka_events

logger = logging.getLogger(__name__)

BIO_TABLE = "athlete_bio"
EVENTS_TABLE = "athlete_event_results"
MAX_OFFSETS_PER_TRIGGER = 50_000


def make_batch_writer(settings: Settings):
    """Build the forEachBatch sink that fans each micro-batch out to Kafka and MySQL."""

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        batch_df.persist()
        try:
            # Етап 6.а): запис у вихідний Kafka-топік
            (
                batch_df.select(
                    col("sport").alias("key"),
                    to_json(struct(*batch_df.columns)).alias("value"),
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
                .option("topic", settings.kafka_output_topic)
                .save()
            )
            # Етап 6.b): запис у базу даних MySQL
            (
                batch_df.write.format("jdbc")
                .options(**settings.jdbc_options(settings.mysql_output_table))
                .mode("append")
                .save()
            )
            logger.info("Batch %s: wrote %s aggregated rows", batch_id, batch_df.count())
        finally:
            batch_df.unpersist()

    return write_batch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()

    spark = build_spark("AthleteStreamingPipeline")
    settings = load_settings()

    # Етап 1: зчитування біологічних даних атлетів з MySQL (olympic_dataset.athlete_bio)
    bio_raw = spark.read.format("jdbc").options(**settings.jdbc_options(BIO_TABLE)).load()

    # Етап 2: фільтрація рядків, де зріст або вага порожні чи не є числами
    bio_df = filter_valid_bio(bio_raw).cache()
    logger.info("Valid bio rows: %s of %s", bio_df.count(), bio_raw.count())

    # Етап 3: зчитування результатів змагань з Kafka-топіку і розбір JSON у колонки.
    # Схема JSON береться з MySQL-таблиці, з якої дані були опубліковані в топік.
    events_schema = (
        spark.read.format("jdbc").options(**settings.jdbc_options(EVENTS_TABLE)).load().schema
    )
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_input_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )
    events_df = parse_kafka_events(kafka_stream, events_schema)

    # Етап 4: об'єднання результатів змагань з біологічними даними за athlete_id
    enriched_df = enrich_events(events_df, bio_df)

    # Етап 5: середній зріст і вага за sport, medal, sex, country_noc + timestamp розрахунку
    stats_df = aggregate_stats(enriched_df)

    # Етап 6: стрим даних через forEachBatch у Kafka-топік і MySQL
    ensure_output_table(settings)
    writer = (
        stats_df.writeStream.foreachBatch(make_batch_writer(settings))
        .outputMode("update")
        .option("checkpointLocation", settings.checkpoint_dir)
    )
    writer = writer.trigger(availableNow=True) if args.available_now else writer
    writer.start().awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()

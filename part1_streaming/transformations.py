"""Pure DataFrame transformations for the streaming pipeline (work on batch and stream)."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, coalesce, col, current_timestamp, from_json, isnan, lit
from pyspark.sql.types import StructType

NO_MEDAL = "No medal"
GROUP_COLUMNS = ("sport", "medal", "sex", "country_noc")


def filter_valid_bio(bio_df: DataFrame) -> DataFrame:
    """Keep athletes whose height and weight are present and numeric.

    try_cast returns NULL for non-numeric strings instead of failing under ANSI mode
    (default in Spark 4); the string "NaN" casts to a real NaN, so it is excluded explicitly.
    """
    return (
        bio_df.withColumn("height", col("height").try_cast("double"))
        .withColumn("weight", col("weight").try_cast("double"))
        .filter(col("height").isNotNull() & col("weight").isNotNull())
        .filter(~isnan("height") & ~isnan("weight"))
        .select("athlete_id", "sex", "height", "weight")
    )


def parse_kafka_events(kafka_df: DataFrame, schema: StructType) -> DataFrame:
    """Turn Kafka JSON values into a DataFrame with one column per JSON field."""
    return (
        kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )


def enrich_events(events_df: DataFrame, bio_df: DataFrame) -> DataFrame:
    """Join competition results with athlete bio; country_noc comes from the event."""
    return events_df.join(bio_df, on="athlete_id", how="inner")


def aggregate_stats(enriched_df: DataFrame) -> DataFrame:
    """Average height and weight per sport, medal (or none), sex and country_noc."""
    return (
        enriched_df.withColumn("medal", coalesce(col("medal"), lit(NO_MEDAL)))
        .groupBy(*GROUP_COLUMNS)
        .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
        .withColumn("timestamp", current_timestamp())
    )

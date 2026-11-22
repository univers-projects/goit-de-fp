"""Silver -> gold: join bio with event results and compute average height/weight."""

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, coalesce, col, current_timestamp, isnan, lit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("silver_to_gold")

DATALAKE_DIR = Path(os.getenv("DATALAKE_DIR", Path(__file__).resolve().parent.parent / "datalake"))
NO_MEDAL = "No medal"
GROUP_COLUMNS = ("sport", "medal", "sex", "country_noc")


def prepare_bio(bio_df: DataFrame) -> DataFrame:
    """Cast height/weight to double and drop rows where either is missing or not a number."""
    return (
        bio_df.withColumn("height", col("height").try_cast("double"))
        .withColumn("weight", col("weight").try_cast("double"))
        .filter(col("height").isNotNull() & col("weight").isNotNull())
        .filter(~isnan("height") & ~isnan("weight"))
        .select("athlete_id", "sex", "height", "weight")
    )


def build_avg_stats(bio_df: DataFrame, events_df: DataFrame) -> DataFrame:
    """country_noc comes from the event (the team the athlete competed for)."""
    return (
        events_df.join(prepare_bio(bio_df), on="athlete_id", how="inner")
        .withColumn("medal", coalesce(col("medal"), lit(NO_MEDAL)))
        .groupBy(*GROUP_COLUMNS)
        .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
        .withColumn("timestamp", current_timestamp())
    )


def main() -> None:
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
    bio_df = spark.read.parquet(str(DATALAKE_DIR / "silver" / "athlete_bio"))
    events_df = spark.read.parquet(str(DATALAKE_DIR / "silver" / "athlete_event_results"))

    gold_df = build_avg_stats(bio_df, events_df)
    output_path = DATALAKE_DIR / "gold" / "avg_stats"
    gold_df.write.mode("overwrite").parquet(str(output_path))
    logger.info("Saved %s rows to %s", gold_df.count(), output_path)
    gold_df.show()
    spark.stop()


if __name__ == "__main__":
    main()

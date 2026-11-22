"""Load athlete_bio and athlete_event_results CSVs from the course FTP into local MySQL.

Stands in for the course MySQL server (olympic_dataset). Overwrites both tables
with the full datasets on every run.
"""

import logging
from pathlib import Path

import requests
from pyspark.sql import DataFrame, SparkSession

from config import PROJECT_ROOT, build_spark, load_settings

logger = logging.getLogger(__name__)

FTP_BASE_URL = "https://ftp.goit.study/neoversity/"
TABLES = ("athlete_bio", "athlete_event_results")
DOWNLOAD_DIR = PROJECT_ROOT / "datalake" / "seed"
# Existing olympic_dataset.athlete_event_results (from hw-07) uses snake_case names
COLUMN_RENAMES = {"event": "event_name", "isTeamSport": "is_team_sport"}


def download_csv(table: str, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{table}.csv"
    url = f"{FTP_BASE_URL}{table}.csv"
    logger.info("Downloading %s", url)
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    target.write_bytes(response.content)
    return target


def read_csv(spark: SparkSession, path: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(path))
    )


def normalize_columns(df: DataFrame) -> DataFrame:
    """Strip the UTF-8 BOM from the first header and align names with the MySQL schema."""
    renamed = [
        COLUMN_RENAMES.get(name.lstrip("\ufeff"), name.lstrip("\ufeff")) for name in df.columns
    ]
    return df.toDF(*renamed)


def main() -> None:
    spark = build_spark("SeedMySQL")
    settings = load_settings()
    for table in TABLES:
        df = normalize_columns(read_csv(spark, download_csv(table, DOWNLOAD_DIR)))
        logger.info("Writing %s rows to MySQL table %s", df.count(), table)
        # truncate keeps the existing table definition instead of recreating it
        (
            df.write.format("jdbc")
            .options(**settings.jdbc_options(table))
            .option("truncate", True)
            .option("batchsize", 10000)
            .mode("overwrite")
            .save()
        )
    spark.stop()


if __name__ == "__main__":
    main()

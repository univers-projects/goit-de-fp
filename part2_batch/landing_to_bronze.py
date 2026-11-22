"""Landing -> bronze: download CSVs from the course FTP and store them as parquet."""

import logging
import os
from pathlib import Path

import requests
from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("landing_to_bronze")

FTP_BASE_URL = "https://ftp.goit.study/neoversity/"
TABLES = ("athlete_bio", "athlete_event_results")
DATALAKE_DIR = Path(os.getenv("DATALAKE_DIR", Path(__file__).resolve().parent.parent / "datalake"))


def download_data(table: str, landing_dir: Path) -> Path:
    landing_dir.mkdir(parents=True, exist_ok=True)
    url = f"{FTP_BASE_URL}{table}.csv"
    logger.info("Downloading from %s", url)
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    target = landing_dir / f"{table}.csv"
    target.write_bytes(response.content)
    logger.info("File downloaded successfully and saved as %s", target)
    return target


def read_csv(spark: SparkSession, path: Path) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(path))
    )
    # athlete_event_results.csv starts with a UTF-8 BOM that leaks into the first header
    return df.toDF(*[name.lstrip("﻿") for name in df.columns])


def main() -> None:
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    for table in TABLES:
        csv_path = download_data(table, DATALAKE_DIR / "landing")
        df = read_csv(spark, csv_path)
        output_path = DATALAKE_DIR / "bronze" / table
        df.write.mode("overwrite").parquet(str(output_path))
        logger.info("Saved %s rows to %s", df.count(), output_path)
        df.show()
    spark.stop()


if __name__ == "__main__":
    main()

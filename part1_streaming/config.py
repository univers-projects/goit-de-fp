"""Shared settings and Spark session factory for the streaming pipeline."""

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SPARK_VERSION = "4.2.0"
SPARK_PACKAGES = ",".join(
    [
        f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}",
        "com.mysql:mysql-connector-j:8.4.0",
    ]
)
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    mysql_host: str
    mysql_port: str
    mysql_db: str
    mysql_user: str
    mysql_password: str
    kafka_bootstrap_servers: str
    kafka_input_topic: str
    kafka_output_topic: str
    mysql_output_table: str
    checkpoint_dir: str

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_db}"
            "?rewriteBatchedStatements=true"
        )

    def jdbc_options(self, table: str) -> dict[str, str]:
        return {
            "url": self.jdbc_url,
            "driver": MYSQL_DRIVER,
            "dbtable": table,
            "user": self.mysql_user,
            "password": self.mysql_password,
        }


def load_settings() -> Settings:
    return Settings(
        mysql_host=os.getenv("MYSQL_HOST", "localhost"),
        mysql_port=os.getenv("MYSQL_PORT", "3307"),
        mysql_db=os.getenv("MYSQL_DB", "olympic_dataset"),
        mysql_user=_require("MYSQL_USER"),
        mysql_password=_require("MYSQL_PASSWORD"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_input_topic=os.getenv("KAFKA_INPUT_TOPIC", "athlete_event_results"),
        kafka_output_topic=os.getenv("KAFKA_OUTPUT_TOPIC", "athlete_enriched_agg"),
        mysql_output_table=os.getenv("MYSQL_OUTPUT_TABLE", "athlete_enriched_agg"),
        # relative paths resolve against the project root, not the current working directory
        checkpoint_dir=str(
            PROJECT_ROOT / os.getenv("CHECKPOINT_DIR", "checkpoints/athlete_enriched_agg")
        ),
    )


def build_spark(app_name: str) -> SparkSession:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

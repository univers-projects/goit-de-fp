"""Bronze -> silver: clean every text column and drop duplicate rows."""

import logging
import os
import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze_to_silver")

TABLES = ("athlete_bio", "athlete_event_results")
# Measurements must be numbers before text cleaning: clean_text would turn a range
# such as "80-82" into the bogus number "8082". Non-numeric values become NULL.
NUMERIC_COLUMNS = ("height", "weight")
DATALAKE_DIR = Path(os.getenv("DATALAKE_DIR", Path(__file__).resolve().parent.parent / "datalake"))


def clean_text(text: str | None) -> str | None:
    """Keep only latin letters, digits and , . \\ " ' characters. NULL stays NULL."""
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', "", str(text))


clean_text_udf = udf(clean_text, StringType())


def cast_numeric_columns(df: DataFrame) -> DataFrame:
    present = [name for name in NUMERIC_COLUMNS if name in df.columns]
    return df.withColumns({name: col(name).try_cast("double") for name in present})


def clean_text_columns(df: DataFrame) -> DataFrame:
    text_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    return df.select(
        *[clean_text_udf(df[name]).alias(name) if name in text_columns else df[name] for name in df.columns]
    )


def main() -> None:
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    for table in TABLES:
        df = spark.read.parquet(str(DATALAKE_DIR / "bronze" / table))
        silver_df = clean_text_columns(cast_numeric_columns(df)).dropDuplicates()
        output_path = DATALAKE_DIR / "silver" / table
        silver_df.write.mode("overwrite").parquet(str(output_path))
        logger.info("Saved %s rows (bronze had %s) to %s", silver_df.count(), df.count(), output_path)
        silver_df.show()
    spark.stop()


if __name__ == "__main__":
    main()

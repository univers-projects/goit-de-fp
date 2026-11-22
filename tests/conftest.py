import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    # UDF modules must be importable by Spark's Python workers, not only by pytest
    root = Path(__file__).resolve().parent.parent
    os.environ["PYTHONPATH"] = os.pathsep.join(
        [str(root / "part1_streaming"), str(root / "part2_batch"), os.getenv("PYTHONPATH", "")]
    )
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()

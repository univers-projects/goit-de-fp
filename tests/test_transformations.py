from pyspark.sql import SparkSession

from bronze_to_silver import cast_numeric_columns, clean_text, clean_text_columns
from silver_to_gold import build_avg_stats
from transformations import NO_MEDAL, aggregate_stats, enrich_events, filter_valid_bio

BIO_COLUMNS = ["athlete_id", "sex", "height", "weight", "country_noc"]


def test_filter_drops_empty_and_non_numeric_height_or_weight(spark: SparkSession) -> None:
    bio = spark.createDataFrame(
        [
            (1, "Male", "180", "80", "UKR"),
            (2, "Male", "", "80", "UKR"),
            (3, "Male", "180", "abc", "UKR"),
            (4, "Male", None, "80", "UKR"),
            (5, "Male", "NaN", "80", "UKR"),
        ],
        BIO_COLUMNS,
    )
    assert [r.athlete_id for r in filter_valid_bio(bio).collect()] == [1]


def test_aggregate_averages_per_group_and_labels_missing_medal(spark: SparkSession) -> None:
    bio = filter_valid_bio(
        spark.createDataFrame(
            [(1, "Male", "180", "80", "X"), (2, "Male", "190", "90", "X")], BIO_COLUMNS
        )
    )
    events = spark.createDataFrame(
        [(1, "Rowing", None, "UKR"), (2, "Rowing", None, "UKR"), (3, "Rowing", "Gold", "UKR")],
        ["athlete_id", "sport", "medal", "country_noc"],
    )
    rows = aggregate_stats(enrich_events(events, bio)).collect()
    assert len(rows) == 1
    row = rows[0]
    assert (row.medal, row.country_noc, row.avg_height, row.avg_weight) == (
        NO_MEDAL,
        "UKR",
        185.0,
        85.0,
    )
    assert row.timestamp is not None


def test_clean_text_removes_disallowed_characters_and_keeps_null() -> None:
    assert clean_text("Cycling Track!") == "CyclingTrack"
    assert clean_text("O'Neil, \"Jr.\"") == "O'Neil,\"Jr.\""
    assert clean_text(None) is None


def test_clean_text_columns_touches_only_strings(spark: SparkSession) -> None:
    df = spark.createDataFrame([(7, "a b-c")], ["athlete_id", "name"])
    row = clean_text_columns(df).collect()[0]
    assert (row.athlete_id, row.name) == (7, "abc")


def test_gold_casts_string_measures_and_uses_event_country(spark: SparkSession) -> None:
    bio = spark.createDataFrame([(1, "Female", "170", "60", "BIO")], BIO_COLUMNS)
    events = spark.createDataFrame(
        [(1, "Judo", "Gold", "EVT")], ["athlete_id", "sport", "medal", "country_noc"]
    )
    row = build_avg_stats(bio, events).collect()[0]
    assert (row.country_noc, row.avg_height, row.avg_weight) == ("EVT", 170.0, 60.0)


def test_silver_turns_weight_range_into_null_not_concatenated_number(spark: SparkSession) -> None:
    bio = spark.createDataFrame(
        [(1, "180", "80-82"), (2, "175", "70")], ["athlete_id", "height", "weight"]
    )
    rows = {r.athlete_id: r for r in clean_text_columns(cast_numeric_columns(bio)).collect()}
    assert rows[1].weight is None
    assert (rows[2].height, rows[2].weight) == (175.0, 70.0)

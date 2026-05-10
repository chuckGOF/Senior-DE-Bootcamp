from pyspark.sql import SparkSession
from week4_spark.src.bronze_to_silver import transform_orders


def spark():

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("tests")
        .getOrCreate()
    )


def test_negative_amount_removed():

    s = spark()

    data = [
        (1, 100.0, "2026-05-01 10:00:00"),
        (2, -50.0, "2026-05-01 10:00:00"),
    ]

    df = s.createDataFrame(
        data,
        ["order_id", "amount", 'updated_at']
    )

    result = transform_orders(df, 'order_id', 'updated_at')

    assert result.count() == 1



def test_latest_duplicate_kept():

    s = spark()

    data = [
        (1, 100.0, "2026-05-01 10:00:00"),
        (1, 200.0, "2026-05-01 11:00:00"),
    ]

    df = s.createDataFrame(
        data,
        ["order_id", "amount", "updated_at"]
    )

    result = transform_orders(df, 'order_id', 'updated_at')

    rows = result.collect()

    assert rows[0]["amount"] == 200.0
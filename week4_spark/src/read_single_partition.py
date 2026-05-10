from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = (
        SparkSession.builder
        .appName('read_single_partition')
        .master('local[*]')
        .getOrCreate()
    )

    df = spark.read.parquet('week4_spark/data/silver/orders')
    filtered = df.filter(col('order_date') == '2026-05-01')
    filtered.explain(mode='formatted')
    filtered.show(truncate=False)

    spark.stop()


if __name__ == '__main__':
    main()
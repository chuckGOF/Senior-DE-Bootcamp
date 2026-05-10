from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col


def main():
    spark = (
        SparkSession.builder
        .appName('create_bronze_sample')
        .master('local[*]')
        .getOrCreate()
    )

    data = [
        (1, 101, 100.0, "completed", "2026-05-01 10:00:00"),
        (2, 102, 250.0, "completed", "2026-05-01 11:00:00"),
        (2, 102, 260.0, "completed", "2026-05-01 12:00:00"),  # duplicate update
        (3, 103, -50.0, "failed", "2026-05-02 09:00:00"),      # bad amount
        (4, 104, 400.0, "completed", "2026-05-02 13:00:00"),
    ]

    df = spark.createDataFrame(data, schema=['order_id', 'customer_id', 'amount', 'status', 'updated_at'])
    df = (
        df.withColumn('updated_at', col('Updated_at').cast('timestamp'))
        .withColumn('order_date', to_date('updated_at'))
    )
    df.write.mode('overwrite').partitionBy('order_date').parquet(
        'week4_spark/data/bronze/orders'
    )

    df.show(truncate=False)

    spark.stop()

if __name__ == '__main__':
    main()
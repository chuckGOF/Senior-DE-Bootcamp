from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col


def main():
    spark = (
        SparkSession.builder
        .appName('create_skew_sample')
        .master('local[*]')
        .getOrCreate()
    )

    rows = []

    # Skewed customer_id = 999 gets most records
    for i in range(1, 100001):
        customer_id = 999 if i <= 8000 else i
        rows.append(
            (
                i,
                customer_id,
                float(i % 500 + 1),
                'completed',
                '2026-05-01 10:00:00'
            )
        )

    df = spark.createDataFrame(rows, schema=['order_id', 'customer_id', 'amount', 'status', 'updated_at'])

    df = (
        df.withColumn('updated_at', col('updated_at').cast('timestamp'))
        .withColumn('order_date', to_date('updated_at'))
    )

    df.write.mode('overwrite').partitionBy('order_date').parquet(
        'week4_spark/data/bronze/skew_orders'
    )

    df.show(truncate=False)

    spark.stop()

if __name__ == '__main__':
    main()
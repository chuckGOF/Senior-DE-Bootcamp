from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName('bronze_to_silver_orders')
        .master('local[*]')
        .config('spark.sql.adaptive.enabled', 'true')
        .config('spark.sql.shuffle.partitions', '8')
        .getOrCreate()
    )


def transform_orders(df, partition_column, sort_column):
    window = Window.partitionBy(partition_column).orderBy(col(sort_column).desc())

    return (
        df.filter(col('amount') > 0)
        .withColumn('rn', row_number().over(window))
        .filter(col('rn') == 1)
        .drop('rn')
    )


def main():
    spark = build_spark()

    bronze_path = 'week4_spark/data/bronze/orders'
    silver_path = 'week4_spark/data/silver/orders'

    df = spark.read.parquet(bronze_path)
    silver_df = transform_orders(df, 'order_id', 'updated_at')

    print('=== Spark execution plan ===')
    silver_df.explain(mode='formatted')
    
    silver_df.write.mode('overwrite').partitionBy('order_date').parquet(silver_path)
    silver_df.show(truncate=False)

    spark.stop()


if __name__ == '__main__':
    main()
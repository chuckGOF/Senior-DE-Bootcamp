from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, concat, sum as spark_sum

def build_spark(aqe_enable: bool) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f'aqe_skew_optimization_{aqe_enable}')
        .master('local[*]')
        .config('spark.sql.adaptive.enabled', str(aqe_enable).lower())
        .config('spark.sql.adaptive.skewJoin.enabled', 'true')
        .config('spark.sql.shuffle.partition', '16')
        .getOrCreate()
    )


def aggregate_by_customer(df):
    return (
        df.groupBy('customer_id')
        .agg(count('*').alias('order_count'))
        .orderBy(col('order_count').desc())
    )

def salted_aggregate(df):
    salted = df.withColumn(
        'salted_customer_id',
        concat(col('customer_id').cast('string'), lit('_'), (col('order_id') % 8).cast('string'))
    )

    partial = (
        salted.groupBy('salted_customer_id', 'customer_id')
        .agg(count('*').alias('partial_count'))
    )

    return (
        partial.groupBy('customer_id')
        .agg(spark_sum('partial_count').alias('order_count'))
        .orderBy(col('order_count').desc())
    )

def main():
    spark = build_spark(aqe_enable=True)
    df = spark.read.parquet('week4_spark/data/bronze/skew_orders')

    print('====== Baseline aggregation with AQE ======')
    result = aggregate_by_customer(df)
    result.explain(mode='formatted')
    result.show(10, truncate=False)


    print('==== Salted aggregation pattern ====')
    salted_result = salted_aggregate(df)
    salted_result.explain(mode='formatted')
    salted_result.show(10, truncate=False)

    spark.stop()


if __name__ == '__main__':
    main()
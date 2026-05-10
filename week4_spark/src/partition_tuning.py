from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName('partition_pruning')
        .master('local[*]')
        .getOrCreate()
    )

    df = spark.read.parquet('week4_spark/data/silver/orders')
    print('Current partitions:' , df.rdd.getNumPartitions())

    repartitioned = df.repartition(4, 'order_date')
    print('After partition: ', repartitioned.rdd.getNumPartitions())

    coalesced = df.coalesce(1)
    print('After coalesce: ', coalesced.rdd.getNumPartitions())


    spark.stop()

if __name__ == '__main__':
    main()
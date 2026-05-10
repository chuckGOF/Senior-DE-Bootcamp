.PHONY: test run build docker-run spark-create-bronze spark-bronze-to-silver spark-read-partition spark-partition-tuning spark-create-skew-sample spark-aqe-optimization

test:
	pytest

run:
	python -m week1_basics.src.main
	python -m week2_cdc.src.main

build:
	docker build -t senior-de-week1 .
	docker build -t senior-de-week2 .


docker-run:
	docker run -p 8000:8000 senior-de-week1
	docker run -p 8000:8000 senior-de-week2


spark-create-bronze:
	python -m week4_spark.src.create_bronze_sample


spark-bronze-to-silver:
	python -m week4_spark.src.bronze_to_silver


spark-read-partition:
	python -m week4_spark.src.read_single_partition


spark-partition-tuning:
	python -m week4_spark.src.partition_tuning

spark-create-skew-sample:
	python -m week4_spark.src.create_skew_sample


spark-aqe-optimization:
	python -m week4_spark.src.aqe_skew_optimization
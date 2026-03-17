from prometheus_client import Counter, Histogram


# Prometheus metrics
cdc_runs = Counter("cdc_runs_total", "Total number of CDC runs")
cdc_failures = Counter("cdc_failures_total", "Total number of CDC failures")
cdc_duration = Histogram("cdc_duration_seconds", "Time spent running CDC")
validation_failures = Counter(
    "cdc_validation_failures_total", "Total number of data validation failures"
)
cdc_rows_processed_total = Counter(
    "cdc_rows_processed_total", "Total number of rows processed in CDC"
)
cdc_partitions_written_total = Counter(
    "cdc_partitions_written_total", "Total number of partitions written in CDC"
)
cdc_watermark_log_seconds = Histogram(
    "cdc_watermark_log_seconds", "Time taken to log watermark in CDC"
)

import time
from datetime import datetime
from prometheus_client import Counter, Gauge, Histogram

class PrometheusMetric:
    def __init__(self):
        self.pipeline_runs_total = Counter(
            "bronze_pipeline_runs_total",
            "Total bronze pipeline runs",
            ['table', 'status']
        )

        self.rows_processed_total = Counter(
            "bronze_rows_processed_total",
            "Total rows processed in bronze",
            ['table']
        )

        self.files_written_total = Counter(
            "bronze_files_written_total",
            "Total files written in bronze",
            ['table']
        )

        self.partitions_written_total = Counter(
            "bronze_partitions_written_total",
            "Total partitions written in bronze",
            ['table']
        )

        self.watermark_latest = Gauge(
            "bronze_watermark_latest",
            "Lastest watermark seen by bronze",
            ['table']
        )

        self.run_duration_seconds = Histogram(
            "bronze_run_duration_seconds",
            "Bronze run duration in seconds",
            ['table']
        )

        self._start_times = {}

    def start_table(self, table):
        self._start_times[table] = time.perf_counter()

    def _observe_duration(self, table):
        start = self._start_times.get(table)

        if start is None:
            return
        self.run_duration_seconds.labels(table=table).observe(
            time.perf_counter() - start
        )

    def add_chunk(self, table, rows, files, partitions):
        self.rows_processed_total.labels(table=table).inc(int(rows))
        self.files_written_total.labels(table=table).inc(int(files))
        self.partitions_written_total.labels(table=table).inc(int(partitions))

    def _watermark_to_unix(self, watermark_value):
        if watermark_value is None:
            return None

        if hasattr(watermark_value, "to_pydatetime"):
            watermark_value = watermark_value.to_pydatetime()

        if isinstance(watermark_value, datetime):
            return float(watermark_value.timestamp())

        return float(watermark_value)

    def mark_success(self, table, watermark_value):
        self.pipeline_runs_total.labels(table=table, status='success').inc()

        wm = self._watermark_to_unix(watermark_value)
        if wm is not None:
            self.watermark_latest.labels(table=table).set(wm)

        self._observe_duration(table)


    def mark_failure(self, table):
        self.pipeline_runs_total.labels(table=table, status='failed').inc()
        self._observe_duration(table)



class Metric:
    def __init__(self):
        self.rows_processed = 0
        self.files_written = 0
        self.partitions_written = set()

    
    def add_rows(self, rows):
        self.rows_processed += rows

    
    def add_files(self, files=1):
        self.files_written += int(files)


    def add_partition(self, partition_value):
        if partition_value is not None:
            self.partitions_written.add(str(partition_value))


    def add_partitions(self, partition_values):
        for value in partition_values:
            self.partitions_written.add(value)


    def summary(self):
        return {
            "rows_processed": self.rows_processed,
            "files_written": self.files_written,
            "partitions_written_count": len(self.partitions_written),
            "partitions_written": sorted(self.partitions_written),
        }
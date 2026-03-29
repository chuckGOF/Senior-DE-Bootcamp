from datetime import datetime, timezone
from prometheus_client import start_http_server

from week2_ingestion_production_framework.core.run_manager import RunManager
from week2_ingestion_production_framework.core.client import ADLSClient

from week2_ingestion_production_framework.bronze.metadata.metadata_manager import (
    MetadataManager,
)
from week2_ingestion_production_framework.bronze.ingestion.extractor import Extractor
from week2_ingestion_production_framework.bronze.ingestion.writer import Writer
from week2_ingestion_production_framework.bronze.ingestion.promoter import Promoter
from week2_ingestion_production_framework.bronze.quality.validations import (
    validate_no_duplicates,
    validate_no_null_partition,
    validate_watermark_progression,
    validate_row_count,
)
from week2_ingestion_production_framework.core.connection import get_engine
from week2_ingestion_production_framework.bronze.metrics.metrics import PrometheusMetric


def run():
    start_http_server(8001)

    run_manager = RunManager()
    run_id = run_manager.get_run_id()
    engine = get_engine()

    storage = ADLSClient()
    metadata_mgr = MetadataManager(engine)
    extractor = Extractor(engine)
    writer = Writer(storage, "bronze")
    promoter = Promoter(storage)
    metrics = PrometheusMetric()

    tables = metadata_mgr.get_tables()

    for t in tables:
        table = t.table_name
        schema = t.source_schema
        partition = t.partition_column
        watermark_col = t.watermark_column
        primary_key = t.primary_key

        start_time = datetime.now(timezone.utc)
        written_rows = 0
        metrics.start_table(table)

        try:
            watermark = metadata_mgr.get_watermark(table)
            source_rows, chunks = extractor.extract_incremental(
                schema, table, watermark_col, watermark, chunksize=5
            )
            max_watermark = watermark

            if source_rows == 0:
                metadata_mgr.log_pipeline_run(
                    run_id,
                    table,
                    start_time,
                    datetime.now(timezone.utc),
                    written_rows,
                    "SUCCESS",
                )
                metrics.mark_success(
                    table, watermark
                )  # watermark may be None; handled safely
                print(f"No new data for table {table}. Skipping.")
                continue

            for chunk in chunks:
                # chunk-level checks
                validate_no_null_partition(chunk, partition)
                validate_no_duplicates(chunk, primary_key)

                # one parquet file written per partition group in this chunk
                partition_values = (
                    chunk[partition].dropna().astype("string").unique().tolist()
                )
                # metrics.add_rows(len(chunk))
                # metrics.add_files(len(partition_values))
                # metrics.add_partitions(partition_values)

                writer.write_partition(chunk, table, partition, run_id)
                written_rows += len(chunk)

                metrics.add_chunk(
                    table=table,
                    rows=len(chunk),
                    files=len(partition_values),
                    partitions=len(partition_values),
                )

                chunk_max = chunk[watermark_col].max()
                if chunk_max > max_watermark:
                    max_watermark = chunk_max

            # table-level checks after all chunks are written
            validate_row_count(source_rows, written_rows)

            # only enforce watermark progression if there was data to process
            if source_rows > 0:
                validate_watermark_progression(watermark, max_watermark)

            promoter.promote(f"bronze/_staging/{table}/{run_id}")

            metrics.mark_success(table, max_watermark)

            metadata_mgr.update_watermark(table, max_watermark)
            metadata_mgr.log_pipeline_run(
                run_id,
                table,
                start_time,
                datetime.now(timezone.utc),
                written_rows,
                "SUCCESS",
            )

            # print(
            #     {
            #         "table": table,
            #         "run_id": run_id,
            #         "metrics": metrics.summary(),
            #         "Watermark Updated": max_watermark,
            #         "Status": "SUCCESS"
            #     }
            # )

        except Exception as e:
            metadata_mgr.log_pipeline_run(
                run_id,
                table,
                start_time,
                datetime.now(timezone.utc),
                written_rows,
                "FAILED",
                str(e),
            )

            metrics.mark_failure(table)

            raise


if __name__ == "__main__":
    run()

from datetime import datetime, timezone

from prometheus_client import start_http_server

from week3_ingestion_orchestration.core.client import ADLSClient
from week3_ingestion_orchestration.core.connection import get_engine
from week3_ingestion_orchestration.core.run_manager import RunManager
from week3_ingestion_orchestration.ingestion.extractor import Extractor
from week3_ingestion_orchestration.ingestion.promoter import Promoter
from week3_ingestion_orchestration.ingestion.writer import Writer
from week3_ingestion_orchestration.metadata.metadata_manager import MetadataManager
from week3_ingestion_orchestration.metrics.metrics import PrometheusMetric
from week3_ingestion_orchestration.pipeline.extract import run_extract
from week3_ingestion_orchestration.pipeline.load import run_load
from week3_ingestion_orchestration.pipeline.promote import run_promote
from week3_ingestion_orchestration.pipeline.update_watermark import run_update_metadata


def process_single_table(
    table_metadata,
    run_id,
    metadata,
    extractor,
    writer,
    promoter,
    metrics,
):
    table = table_metadata.table_name
    start_time = datetime.now(timezone.utc)
    progress = {"written_rows": 0}

    metrics.start_table(table)

    try:
        extract_result = run_extract(metadata, extractor, table_metadata)
        source_rows = extract_result["source_rows"]
        watermark = extract_result["watermark"]

        if source_rows == 0:
            metadata.log_pipeline_run(
                run_id,
                table,
                start_time,
                datetime.now(timezone.utc),
                progress["written_rows"],
                "SUCCESS",
            )
            metrics.mark_success(table, watermark)
            print(f"No new data for table {table}. Skipping.")
            return

        written_rows, max_watermark = run_load(
            writer=writer,
            metrics=metrics,
            run_id=run_id,
            table=table,
            source_rows=source_rows,
            partition=extract_result["partition_column"],
            primary_key=extract_result["primary_key"],
            watermark_column=extract_result["watermark_column"],
            watermark=watermark,
            chunks=extract_result["chunks"],
            progress=progress,
        )

        run_promote(promoter, run_id, table)
        run_update_metadata(metadata, table, max_watermark)

        metrics.mark_success(table, max_watermark)

        metadata.log_pipeline_run(
            run_id,
            table,
            start_time,
            datetime.now(timezone.utc),
            written_rows,
            "SUCCESS",
        )

    except Exception as e:
        metadata.log_pipeline_run(
            run_id,
            table,
            start_time,
            datetime.now(timezone.utc),
            progress["written_rows"],
            "FAILED",
            str(e),
        )
        metrics.mark_failure(table)
        raise


def run_all_tables(start_metrics_server=True):
    if start_metrics_server:
        start_http_server(8001)

    run_id = RunManager().get_run_id()
    engine = get_engine()

    storage = ADLSClient()
    metadata = MetadataManager(engine)
    extractor = Extractor(engine)
    writer = Writer(storage, "bronze")
    promoter = Promoter(storage)
    metrics = PrometheusMetric()

    for table_metadata in metadata.get_tables():
        process_single_table(
            table_metadata=table_metadata,
            run_id=run_id,
            metadata=metadata,
            extractor=extractor,
            writer=writer,
            promoter=promoter,
            metrics=metrics,
        )

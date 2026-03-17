from datetime import datetime, timezone

from week2_ingestion_production_framework.core.run_manager import RunManager
from week2_ingestion_production_framework.core.client import ADLSClient

from week2_ingestion_production_framework.metadata.metadata_manager import MetadataManager
from week2_ingestion_production_framework.ingestion.extractor import Extractor
from week2_ingestion_production_framework.ingestion.writer import Writer
from week2_ingestion_production_framework.ingestion.promoter import Promoter
from week2_ingestion_production_framework.quality.validations import validate_no_duplicates, validate_no_null_partition
from week2_ingestion_production_framework.core.connection import get_connection


def run():
    run_manager = RunManager()
    run_id = run_manager.get_run_id()
    conn = get_connection()

    storage = ADLSClient()

    metadata_mgr = MetadataManager(conn)
    extractor = Extractor(conn)


    writer = Writer(storage, 'bronze')
    promoter = Promoter(storage)

    tables = metadata_mgr.get_tables()

    for t in tables:
        table = t.table_name
        schema = t.source_schema
        partition = t.partition_column
        watermark_col = t.watermark_column
        primary_key = t.primary_key

        start_time = datetime.now(timezone.utc)

        rows = 0

        try:
            watermark = metadata_mgr.get_watermark(table)
            chunks = extractor.extract_incremental(schema, table, watermark_col, watermark)
            max_watermark = watermark

            for chunk in chunks:
                validate_no_null_partition(chunk, partition)
                validate_no_duplicates(chunk, primary_key)

                writer.write_partition(chunk, table, partition, run_id)

                rows += len(chunk)

                chunk_max = chunk[watermark_col].max()

                if chunk_max > max_watermark:
                    max_watermark = chunk_max

            promoter.promote(
                f'bronze/_staging/{table}/{run_id}'
            )

            metadata_mgr.update_watermark(table, max_watermark)
            metadata_mgr.log_pipeline_run(run_id, table, start_time, datetime.now(timezone.utc), rows, 'SUCCESS')

        except Exception as e:
            metadata_mgr.log_pipeline_run(
                run_id,
                table,
                start_time,
                datetime.now(timezone.utc),
                rows,
                'FAILED',
                str(e)
            )

            raise

if __name__ == '__main__':
    run()
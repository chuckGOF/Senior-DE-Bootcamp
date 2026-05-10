from week3_ingestion_orchestration.ingestion.extractor import Extractor
from week3_ingestion_orchestration.metadata.metadata_manager import MetadataManager


def run_extract(metadata: MetadataManager, extractor: Extractor, table_metadata):
    table = table_metadata.table_name
    schema = table_metadata.source_schema
    watermark_column = table_metadata.watermark_column
    watermark = metadata.get_watermark(table)

    source_rows, chunks = extractor.extract_incremental(
        schema,
        table,
        watermark_column,
        watermark,
        chunksize=5,
    )

    return {
        "table": table,
        "schema": schema,
        "primary_key": table_metadata.primary_key,
        "partition_column": table_metadata.partition_column,
        "watermark_column": watermark_column,
        "watermark": watermark,
        "source_rows": source_rows,
        "chunks": chunks,
    }

from week3_airflow_orchestration.ingestion.extractor import Extractor
from week3_airflow_orchestration.ingestion.writer import df_to_parquet_bytes
from week3_airflow_orchestration.metadata.metadata_manager import MetadataManager
from week3_airflow_orchestration.pipeline.models import StagePayload, TableConfig


def extract_table(
        metadata: MetadataManager,
        extractor: Extractor,
        storage,
        table_cfg: TableConfig,
        run_id: str,
        chunk_size: int = 5000,
) -> StagePayload:
    table = table_cfg['table_name']
    schema = table_cfg['source_schema']
    watermark_column = table_cfg['watermark_column']
    watermark = metadata.get_watermark(table=table)


    source_rows, chunks = extractor.extract_incremental(
        schema,
        table,
        watermark_column,
        watermark,
        chunk_size
    )

    extract_prefix = f'bronze/_airflow_extract/{run_id}/{table}'
    chunk_paths = []

    for idx, chunk in enumerate(chunks):
        chunk_path = f'{extract_prefix}/chunk_{idx:06d}.parquet'
        storage.upload_bytes(chunk_path, df_to_parquet_bytes(chunk))
        chunk_paths.append(chunk_path)
    
    return {
        'run_id': run_id,
        'table': table,
        'schema': schema,
        'primary_key': table_cfg['primary_key'],
        'partition_column': table_cfg['partition_column'],
        'watermark_column': watermark_column,
        'watermark': watermark,
        'source_rows': source_rows,
        'extract_prefix': extract_prefix,
        'chunk_paths': chunk_paths,
        'started_at': metadata.utcnow_iso()
    }

from week3_airflow_orchestration.core.client import ADLSClient
from week3_airflow_orchestration.core.connection import get_engine
from week3_airflow_orchestration.ingestion.extractor import Extractor
from week3_airflow_orchestration.metadata.metadata_manager import MetadataManager
from week3_airflow_orchestration.pipeline.extract import extract_table
from week3_airflow_orchestration.pipeline.load import load_to_staging
from week3_airflow_orchestration.pipeline.promote import promote_table
from week3_airflow_orchestration.pipeline.validate import validate_extract
from week3_airflow_orchestration.pipeline.update_watermark import update_watermark


def build_run_id(dag_run_id: str) -> str:
    return dag_run_id.replace(':', '_')


def discover_tables() -> list[dict]:
    engine = get_engine()
    metadata = MetadataManager(engine)

    tables = []
    for row in metadata.get_tables():
        tables.append({
            'table_name': row.table_name,
            'source_schema': row.source_schema,
            'primary_key': row.primary_key,
            'partition_column': row.partition_column,
            'watermark_column': row.watermark_column
        })
    return tables


def extract_stage(table_cfg: dict, pipeline_run_id: str) -> dict:
    engine = get_engine()
    metadata = MetadataManager(engine)
    extractor = Extractor(engine)
    storage = ADLSClient()
    return extract_table(
        metadata,
        extractor,
        storage,
        table_cfg,
        pipeline_run_id,
        chunk_size=5000
    )


def validate_stage(payload: dict) -> dict:
    storage = ADLSClient()
    return validate_extract(storage, payload)


def load_stage(payload: dict) -> dict:
    storage = ADLSClient()
    return load_to_staging(storage, payload)


def promote_stage(payload: dict) -> dict:
    storage = ADLSClient()
    return promote_table(storage, payload)


def update_watermark_stage(payload: dict) -> dict:
    storage = ADLSClient()
    return update_watermark(storage, payload)
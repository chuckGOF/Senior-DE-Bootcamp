import io
import pandas as pd

from week3_airflow_orchestration.ingestion.writer import Writer
from week3_airflow_orchestration.pipeline.models import StagePayload
from week3_airflow_orchestration.quality.validations import validate_row_count


def load_to_staging(storage, payload: StagePayload) -> StagePayload:
    source_rows = payload['source_rows']
    if source_rows == 0:
        payload['written_rows'] = 0
        payload['staging_prefix'] = f"bronze/_staging/{payload['table']}/{payload['run_id']}"
        return payload
    
    writer = Writer(storage, 'bronze')
    written_rows = 0
    files_written = 0

    for path in payload['chunk_paths']:
        raw = storage.read_file(path)
        chunk = pd.read_parquet(io.BytesIO(raw))
        files_written += writer.write_partition(
            chunk,
            payload['table'],
            payload['partition_column'],
            payload['run_id']
        )
        written_rows += len(chunk)
    validate_row_count(source_rows, written_rows)
    payload['written_rows'] = written_rows
    payload['files_written'] = files_written
    payload['staging_prefix'] = f"bronze/_staging/{payload['table']}/{payload['run_id']}"
    return payload
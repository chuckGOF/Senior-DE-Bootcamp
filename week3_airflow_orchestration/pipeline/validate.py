import io

import pandas as pd

from week3_airflow_orchestration.pipeline.models import StagePayload
from week3_airflow_orchestration.quality.validations import (
    validate_is_not_empty,
    validate_no_duplicates,
    validate_no_null_partition,
    validate_row_count,
    validate_watermark_progression,
)


def validate_extract(storage, payload: StagePayload) -> StagePayload:
    source_rows = payload["source_rows"]
    chunk_paths = payload["chunk_paths"]

    if source_rows == 0:
        payload["validated_rows"] = 0
        payload["max_watermark"] = payload["watermark"]
        return payload

    validated_rows = 0
    max_watermark = payload["watermark"]

    for path in chunk_paths:
        raw = storage.read_file(path)
        chunk = pd.read_parquet(io.BytesIO(raw))

        validate_is_not_empty(chunk)
        validate_no_duplicates(chunk, payload["primary_key"])
        validate_no_null_partition(chunk, payload["partition_column"])

        validated_rows += len(chunk)
        chunk_max = chunk[payload["watermark_column"]].max()

        if max_watermark is None or chunk_max > max_watermark:
            max_watermark = chunk_max

        validate_row_count(source_rows, validated_rows)
        validate_watermark_progression(payload["watermark"], max_watermark)

        payload["validated_rows"] = validated_rows
        payload["max_watermark"] = max_watermark
    return payload

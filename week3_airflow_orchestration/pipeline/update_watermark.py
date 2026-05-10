from datetime import datetime, timezone

from week3_airflow_orchestration.metadata.metadata_manager import MetadataManager
from week3_airflow_orchestration.pipeline.models import StagePayload


def update_watermark(metadata: MetadataManager, payload: StagePayload) -> StagePayload:
    if payload["source_rows"] > 0:
        metadata.update_watermark(payload["table"], payload["max_watermark"])

    metadata.log_pipeline_run(
        run_id=payload["run_id"],
        table=payload["table"],
        start_time=datetime.fromisoformat(payload["started_at"]),
        end_time=datetime.now(timezone.utc),
        rows=payload.get("written_rows", 0),
        status="SUCCESS",
    )

    return payload

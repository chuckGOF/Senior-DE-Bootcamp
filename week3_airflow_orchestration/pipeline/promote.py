from week3_airflow_orchestration.ingestion.promoter import Promoter
from week3_airflow_orchestration.pipeline.models import StagePayload


def promote_table(storage, payload: StagePayload) -> StagePayload:
    if payload["source_rows"] == 0:
        return payload
    promoter = Promoter(storage)
    promoter.promote(payload["staging_prefix"])
    return payload

from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import text


class MetadataManager:
    def __init__(self, engine):
        self.engine = engine

    def get_tables(self):
        query = text(
            """
            SELECT
                table_name,
                source_schema,
                primary_key,
                partition_column,
                watermark_column
            FROM metadata.ingestion_tables
            WHERE active = 1
        """
        )

        with self.engine.connect() as conn:
            rows = conn.execute(query).mappings().all()

        return [SimpleNamespace(**row) for row in rows]

    def get_watermark(self, table):
        query = text(
            """
            SELECT watermark_value
            FROM metadata.ingestion_watermarks
            WHERE table_name = :table
        """
        )

        with self.engine.connect() as conn:
            row = conn.execute(query, {"table": table}).first()

        return row[0] if row else None

    def update_watermark(self, table, watermark):
        query = text(
            """
            UPDATE metadata.ingestion_watermarks
            SET watermark_value = :watermark, updated_at = CURRENT_TIMESTAMP
            WHERE table_name = :table
        """
        )

        with self.engine.begin() as conn:
            conn.execute(query, {"watermark": watermark, "table": table})

    def log_pipeline_run(
        self, run_id, table, start_time, end_time, rows, status, error=None
    ):
        query = text(
            """
            INSERT INTO metadata.pipeline_runs (
                run_id,
                table_name,
                start_time,
                end_time,
                rows_ingested,
                status,
                error_message
            )
            VALUES (
                :run_id,
                :table,
                :start_time,
                :end_time,
                :rows,
                :status,
                :error
            )
        """
        )

        with self.engine.begin() as conn:
            conn.execute(
                query,
                {
                    "run_id": run_id,
                    "table": table,
                    "start_time": start_time,
                    "end_time": end_time,
                    "rows": rows,
                    "status": status,
                    "error": error,
                },
            )

    @staticmethod
    def utcnow_iso():
        return datetime.now(timezone.utc).isoformat()

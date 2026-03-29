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
            SELECT
                watermark_value
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


class MetadataManagerOLD:
    def __init__(self, conn):
        self.conn = conn

    def get_tables(self):
        query = """
            SELECT 
                table_name,
                source_schema,
                primary_key,
                partition_column,
                watermark_column
            FROM metadata.ingestion_tables
            WHERE active = 1
        """

        cursor = self.conn.cursor()
        return cursor.execute(query).fetchall()

    def get_watermark(self, table):
        query = """
            SELECT
                watermark_value
            FROM metadata.ingestion_watermarks
            WHERE table_name = ?
        """

        cursor = self.conn.cursor()
        result = cursor.execute(query, table).fetchone()
        return result[0]

    def update_watermark(self, table, watermark):
        query = """
            UPDATE metadata.ingestion_watermarks
            SET watermark_value = ?, updated_at = CURRENT_TIMESTAMP
            WHERE table_name = ?
        """

        cursor = self.conn.cursor()
        cursor.execute(query, watermark, table)

        self.conn.commit()

    def log_pipeline_run(
        self, run_id, table, start_time, end_time, rows, status, error=None
    ):
        query = """
            INSERT INTO metadata.pipeline_runs
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        cursor = self.conn.cursor()
        cursor.execute(query, run_id, table, start_time, end_time, rows, status, error)

        self.conn.commit()

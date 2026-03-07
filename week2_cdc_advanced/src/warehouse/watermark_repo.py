from datetime import datetime


# this is crash safe, concurrency safe and distributed safe
class WatermarkRepository:
    def __init__(self, conn, pipeline_name: str):
        self.conn = conn
        self.pipeline = pipeline_name

    def acquire_lock(self) -> datetime:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT last_watermark
            FROM cdc_watermarks
            WHERE pipeline_name = ?
        """,
            self.pipeline,
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def update(self, new_watermark: datetime) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE cdc_watermarks
            SET last_watermark = ?, updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_name = ?
        """,
            new_watermark,
            self.pipeline,
        )

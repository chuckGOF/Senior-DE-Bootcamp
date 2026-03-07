from week2_cdc_advanced.src.archive.db import get_connection
from week2_cdc_advanced.src.archive.config import settings


def get_watermark():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_updated FROM pipeline_watermark WHERE pipeline_name = %s",
                (settings.PIPELINE_NAME,),
            )
            return cur.fetchone()[0]


def update_watermark(new_ts):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_watermark 
                SET last_updated = %s
                WHERE pipeline_name = %s
                """,
                (new_ts, settings.PIPELINE_NAME),
            )
            conn.commit()

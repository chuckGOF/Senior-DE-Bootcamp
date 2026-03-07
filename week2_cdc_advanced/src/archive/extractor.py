import pandas as pd
from week2_cdc_advanced.src.archive.db import get_connection
from week2_cdc_advanced.src.archive.watermark_repository import get_watermark


def extract():
    watermark = get_watermark()

    query = """
        SELECT *
        FROM sales
        WHERE updated_at > %s
        ORDER BY updated_at
    """

    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=(watermark,))

    return df


def extract_incremental(conn, last_watermark):
    query = """
        SELECT *
        FROM sales
        WHERE updated_at > %s
        ORDER BY updated_at
    """

    df = pd.read_sql(query, conn, params=(last_watermark,))
    return df

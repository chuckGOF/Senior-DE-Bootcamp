import pandas as pd
from datetime import timedelta


LOOKBACK_DAYS = 2


# late arriving data handled, reprocessing safe and idempotent when combined with overwrite strategy
def extract_incremental(conn, last_watermark) -> pd.DataFrame:
    effective_from = last_watermark - timedelta(days=LOOKBACK_DAYS)

    query = """
        SELECT *
        FROM sales_orders
        WHERE updated_at > ?
    """

    return pd.read_sql(query, conn, params=[effective_from])

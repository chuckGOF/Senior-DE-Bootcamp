from typing import Generator

import pandas as pd
from datetime import timedelta

LOOKBACK_DAYS = 2


# late arriving data handled, reprocessing safe and idempotent when combined with overwrite strategy
# implement chunking if data is too large to fit in memory
def extract_incremental(
    conn, last_watermark, chunk_size=500000
) -> Generator[pd.DataFrame, None, None]:
    effective_from = last_watermark - timedelta(days=LOOKBACK_DAYS)

    query = """
        SELECT *
        FROM sales_orders
        WHERE updated_at > ?
        ORDER BY updated_at
    """

    for chunk in pd.read_sql(
        query, conn, params=[effective_from], chunksize=chunk_size
    ):
        yield chunk

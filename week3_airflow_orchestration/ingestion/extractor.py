import pandas as pd
from sqlalchemy import text


class Extractor:
    def __init__(self, engine):
        self.engine = engine

    def extract_incremental(self, schema, table, watermark_col, watermark, chunksize=5000):
        count_query = text(
            f"""
            SELECT COUNT(*) AS total_rows
            FROM {schema}.{table}
            WHERE {watermark_col} > :watermark
        """
        )
        source_rows = int(
            pd.read_sql(
                count_query,
                self.engine,
                params={"watermark": watermark},
            ).iloc[0]["total_rows"]
        )

        data_query = text(
            f"""
            SELECT *
            FROM {schema}.{table}
            WHERE {watermark_col} > :watermark
            ORDER BY {watermark_col}
        """
        )

        chunks = pd.read_sql(
            data_query,
            self.engine,
            params={"watermark": watermark},
            chunksize=chunksize,
        )

        return source_rows, chunks

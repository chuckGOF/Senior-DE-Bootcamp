import pandas as pd


class Extractor:
    def __init__(self, conn):
        self.conn = conn

    def extract_incremental(
        self,
        schema,
        table,
        watermark_col,
        watermark
    ):
        query = f"""
            SELECT *
            FROM {schema}.{table}
            WHERE {watermark_col} > ?
            ORDER BY {watermark_col}
        """

        return pd.read_sql(
            query,
            self.conn,
            params=[watermark],
            chunksize=5
        )
import pandas as pd
from sqlalchemy import text


class Extractor:
    def __init__(self, engine):
        self.engine = engine

    def extract_incremental(
        self, schema, table, watermark_col, watermark, chunksize=5000
    ):
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


class ExtractorOLD:
    def __init__(self, conn):
        self.conn = conn

    def extract_incremental(
        self, schema, table, watermark_col, watermark, chunksize=5000
    ):
        count_query = f"""
            SELECT COUNT(*) AS total_rows
            FROM {schema}.{table}
            WHERE {watermark_col} > ?
        """
        source_rows = int(
            pd.read_sql(count_query, self.conn, params=[watermark]).iloc[0][
                "total_rows"
            ]
        )

        data_query = f"""
            SELECT *
            FROM {schema}.{table}
            WHERE {watermark_col} > ?
            ORDER BY {watermark_col}
        """

        chunks = pd.read_sql(
            data_query, self.conn, params=[watermark], chunksize=chunksize
        )

        return source_rows, chunks


class ExtractorV3:
    def __init__(self, conn):
        self.conn = conn

    def extract_incremental(
        self, schema, table, watermark_col, watermark, chunksize=5000
    ):
        count_query = f"""
            SELECT COUNT(*) AS total_rows
            FROM {schema}.{table}
            WHERE {watermark_col} > ?
        """
        data_query = f"""
            SELECT *
            FROM {schema}.{table}
            WHERE {watermark_col} > ?
            ORDER BY {watermark_col}
        """

        # Count rows
        count_cursor = self.conn.cursor()
        count_cursor.execute(count_query, (watermark,))
        source_rows = int(count_cursor.fetchone()[0])
        count_cursor.close()

        # Stream data in chunks without pandas.read_sql warning
        data_cursor = self.conn.cursor()
        data_cursor.execute(data_query, (watermark,))
        columns = [col[0] for col in data_cursor.description]

        def chunk_generator():
            while True:
                rows = data_cursor.fetchmany(chunksize)
                if not rows:
                    break
                yield pd.DataFrame.from_records(rows, columns=columns)
            data_cursor.close()

        return source_rows, chunk_generator()

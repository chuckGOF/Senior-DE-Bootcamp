import io
import uuid

import pyarrow as pa
import pyarrow.parquet as pq


class Writer:
    def __init__(self, storage, base_path):
        self.storage = storage
        self.base_path = base_path

    def write_partition(
        self, df, table: str, partition_column: str, run_id: str
    ) -> int:
        files_written = 0
        groups = df.groupby(partition_column)

        for value, partition_df in groups:
            if hasattr(value, "date"):
                value = value.date().isoformat()
            path = (
                f"{self.base_path}/_staging/{table}/{run_id}/"
                f"{partition_column}={value}/part_{uuid.uuid4().hex}.parquet"
            )
            buffer = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(partition_df), buffer)
            self.storage.upload_bytes(path, buffer.getvalue())
            files_written += 1

        return files_written


def df_to_parquet_bytes(df) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df), buffer)
    return buffer.getvalue()

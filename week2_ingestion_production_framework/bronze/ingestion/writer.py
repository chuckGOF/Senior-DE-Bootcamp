import uuid
import io
import pyarrow as pa
import pyarrow.parquet as pq
from week2_ingestion_production_framework.core.storage import StorageClient


class Writer:
    def __init__(self, storage: StorageClient, base_path):
        self.storage = storage
        self.base_path = base_path

    def write_partition(self, df, table: str, partition_column: str, run_id: str) -> None:
        groups = df.groupby(partition_column)

        for value, partition_df in groups:
            value = value.date().isoformat()
            path = f"{self.base_path}/_staging/{table}/{run_id}/{partition_column}={value}/part_{uuid.uuid4().hex}.parquet"
            buffer = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(partition_df), buffer)
            self.storage.upload_bytes(path, buffer.getvalue())


class WriterOLD:
    def __init__(self, adls):
        self.adls = adls

    def write_partition(self, df, table, partition_column, run_id):
        groups = df.groupby(partition_column)

        for value, partition_df in groups:
            folder = f"_staging/{table}/{run_id}/{partition_column}={value}"
            filename = f"part_{uuid.uuid4().hex}.parquet"
            path = f"{folder}/{filename}"
            buffer = io.BytesIO()
            arrow_table = pa.Table.from_pandas(partition_df)
            pq.write_table(arrow_table, buffer)
            self.adls.upload_bytes(path, buffer.getvalue())
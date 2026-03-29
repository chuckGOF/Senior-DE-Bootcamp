# import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd

from week2_ingestion_production_framework.core.storage import StorageClient


class BronzeReader:
    def __init__(self, storage: StorageClient, path: str):
        self.storage = storage
        self.path = path

    def read_table_in_chunks(self, chunk_size=5, columns=None, filter_expr=None):
        dataset = ds.dataset(
            self.storage._full_path(self.path),
            format="parquet",
            filesystem=self.storage.fs,
        )

        scanner = dataset.scanner(
            columns=columns, filter=filter_expr, batch_size=chunk_size
        )

        for batch in scanner.to_batches():
            yield batch.to_pandas()

    def read_table(self):
        return pd.concat(self.read_table_in_chunks(), ignore_index=True)

import uuid

import pandas as pd
import s3fs
from week2_cdc_advanced.src.config import cloud_settings


class S3Writer:
    def __init__(self):
        self.fs = s3fs.S3FileSystem()
        self.base_path = f"{cloud_settings.S3_BUCKET}/sales_orders"

    def write(self, df: pd.DataFrame) -> None:
        if df.empty:
            return

        df["updated_date"] = df["updated_at"].dt.date
        partitions = df["updated_date"].unique()

        for partition in partitions:
            partition_df = df[df["updated_date"] == partition]

            path = f"{self.base_path}/updated_date={partition}/data.parquet"

            with self.fs.open(path, "wb") as f:
                partition_df.to_parquet(f, engine="pyarrow", index=False)

    def write_partition(self, df: pd.DataFrame, partition) -> None:
        if df.empty:
            return

        staging_path = (
            f"{self.base_path}/_staging/{uuid.uuid4()}/updated_date={partition}.parquet"
        )
        final_path = f"{self.base_path}/updated_date={partition}/data.parquet"

        with self.fs.open(staging_path, "wb") as f:
            df.to_parquet(f, engine="pyarrow", index=False)

        return staging_path, final_path

    def promote(self, staging_path, final_path) -> None:
        if self.fs.exists(final_path):
            self.fs.rm(final_path, recursive=True)
            self.fs.mv(staging_path, final_path)
        else:
            self.fs.mv(staging_path, final_path)

import uuid
import pandas as pd
from adlfs import AzureBlobFileSystem
from azure.identity import DefaultAzureCredential
from week2_cdc_advanced.src.config import cloud_settings


credential = DefaultAzureCredential()


class ADLSWriter:
    def __init__(self):
        self.fs = AzureBlobFileSystem(
            account_name=cloud_settings.ADLS_ACCOUNT_NAME,
            account_key=cloud_settings.ADLS_ACCESS_KEY,
        )

        self.base_path = f"{cloud_settings.ADLS_CONTAINER}/sales_orders"

    def write(self, df: pd.DataFrame) -> None:
        if df.empty:
            return

        df["updated_date"] = df["updated_at"].dt.date
        partitions = df["updated_date"].unique()

        for partition in partitions:
            partition_df = df[df["updated_date"] == partition]

            path = f"{self.base_path}/updated_date={partition}/data.parquet"

            # Overwrite partition safely
            with self.fs.open(path, "wb") as f:
                partition_df.to_parquet(f, engine="pyarrow", index=False)

    def write_chunk(self, df: pd.DataFrame, run_id: str):
        df["updated_date"] = df["updated_at"].dt.date
        partitions = df["updated_date"].unique()

        files = []

        for partition in partitions:
            partition_df = df[df["updated_date"] == partition]

            file_id = uuid.uuid4()

            path = (
                f"{self.base_path}/_staging/run_id={run_id}/"
                f"updated_date={partition}/part-{file_id}.parquet"
            )

            with self.fs.open(path, "wb") as f:
                partition_df.to_parquet(f, engine="pyarrow", index=False)

            files.append(path)

        return files

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

    def promote_run(self, run_id):
        staging_path = f"{self.base_path}/_staging/run_id={run_id}"

        partitions = self.fs.ls(staging_path)

        for partition_path in partitions:
            partition_name = partition_path.split("/")[-1]
            print("staging_path", partition_name)
            final_partition = f"{self.base_path}/{partition_name}"

            files = self.fs.ls(partition_path)
            print("final_partiton", final_partition)

            for file in files:
                print(file)
                filename = file.split("/")[-1]

                target = f"{final_partition}/{filename}"

                self.fs.mv(file, target)

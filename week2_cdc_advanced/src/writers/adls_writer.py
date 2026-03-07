import pandas as pd
from adlfs import AzureBlobFileSystem
from azure.identity import DefaultAzureCredential
from week2_cdc_advanced.src.config import cloud_settings

credential = DefaultAzureCredential()


class ADLSWriter:
    def __init__(self):
        self.fs = AzureBlobFileSystem(
            account_name=cloud_settings.ADLS_ACCOUNT_NAME, credential=credential
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

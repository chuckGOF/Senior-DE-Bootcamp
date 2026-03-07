import os
import pandas as pd
from azure.storage.blob import BlobServiceClient


def write_to_adls(df: pd.DataFrame) -> None:
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    service = BlobServiceClient.from_connection_string(connection_string)

    container = service.get_container_client("sales-data")
    parquet_bytes = df.to_parquet(index=False)

    blob = container.get_blob_client("sales/latest.parquet")
    blob.upload_blob(parquet_bytes, overwrite=True)

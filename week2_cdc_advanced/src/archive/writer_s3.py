import os
import io
import boto3
import pandas as pd


def write_to_s3(df: pd.DataFrame) -> None:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)

    s3.put_object(
        Bucket=os.getenv("S3_BUCKET"),
        Key="sales/latest.parquet",
        Body=buffer.getvalue(),
    )

import pandas as pd
from pathlib import Path
from week2_cdc.src.config import settings
from week2_cdc.src.schema import align_schema
from week2_cdc.src.watermark import write_watermark_atomic


# Idempotent Upsert + Partitioned Parquet
def upsert_parquet(df: pd.DataFrame) -> None:
    output_path = Path(settings.OUTPUT_PATH)

    if output_path.exists():
        existing = pd.read_parquet(output_path)
        df = align_schema(existing, df)

        combined = pd.concat([existing, df])
        combined = combined.sort_values(settings.TIMESTAMP_COLUMNS)
        combined = combined.drop_duplicates(subset=[settings.PRIMARY_KEY], keep="last")
    else:
        combined = df

    combined.to_parquet(
        output_path, partition_cols=["updated_date"], engine="pyarrow", index=False
    )


def load(df: pd.DataFrame):
    if df.empty:
        return

    # Upsert to Parquet
    upsert_parquet(df)

    # Update watermark
    max_ts = df[settings.TIMESTAMP_COLUMNS].max()
    write_watermark_atomic(max_ts)

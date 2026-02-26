import shutil
import tempfile
import pandas as pd
from pathlib import Path
from week2_cdc.src.config import settings
from week2_cdc.src.schema import align_schema
from week2_cdc.src.watermark import write_watermark_atomic


BASE_DIR = Path(__file__).parent.parent.parent


# Idempotent Upsert + Partitioned Parquet
def upsert_parquet(df: pd.DataFrame) -> None:
    output_path = Path(BASE_DIR / settings.OUTPUT_PATH)

    if output_path.exists():
        existing = pd.read_parquet(output_path)
        df = align_schema(existing, df)
        combined = (
            pd.concat([existing, df])
            .sort_values(settings.TIMESTAMP_COLUMNS, ignore_index=True)
            .drop_duplicates(subset=[settings.PRIMARY_KEY], keep="last")
        )
    else:
        combined = df

    # Write to temp and move for atomicity
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir) / "temp_output"
        combined.to_parquet(
            temp_path, partition_cols=["updated_date"], engine="pyarrow", index=False
        )
        if output_path.exists():
            shutil.rmtree(output_path)
        shutil.move(temp_path, output_path)


def load(df: pd.DataFrame):
    if df.empty:
        return

    # Upsert to Parquet
    upsert_parquet(df)

    # Update watermark
    max_ts = df[settings.TIMESTAMP_COLUMNS].max()
    write_watermark_atomic(max_ts)

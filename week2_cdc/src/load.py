import pandas as pd
from pathlib import Path
from week2_cdc.src.extract import write_watermark


OUTPUT_FILE = Path("output.csv")


def load(df: pd.DataFrame, output_path: str = OUTPUT_FILE):
    if df.empty:
        return
    # Append to output
    df.to_csv(output_path, mode="a", index=False, header=not output_path.exists())
    # Update watermark
    max_ts = df["updated_at"].max()
    write_watermark(max_ts)

import json
import pandas as pd
from pathlib import Path
from typing import Union


WATERMARK_FILE = Path("watermark.json")


def read_watermark() -> pd.Timestamp:
    if WATERMARK_FILE.exists():
        with open(WATERMARK_FILE, "r") as file:
            data = json.load(file)
            return pd.Timestamp(data["last_updated"])
    return pd.Timestamp("1970-01-01 00:00:00")


def write_watermark(timestamp: pd.Timestamp) -> None:
    with open(WATERMARK_FILE, "w") as file:
        json.dump({"last_updated": timestamp.isoformat()}, file)


def extract_new_rows(df: pd.DataFrame, last_watermark: pd.Timestamp) -> pd.DataFrame:
    """Return only rows updated after the last watermark."""
    return df[df["updated_at"] > last_watermark]


def extract(path: Union[Path, str]) -> pd.DataFrame:
    """Full extract function reading CSV and applying watermark."""
    df = pd.read_csv(path, parse_dates=["updated_at"])
    new_rows = extract_new_rows(df, read_watermark())
    return new_rows

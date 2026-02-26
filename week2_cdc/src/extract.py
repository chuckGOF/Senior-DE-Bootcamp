import pandas as pd
from datetime import timedelta
from week2_cdc.src.config import settings
from week2_cdc.src.watermark import read_watermark


# def extract_new_rows(df: pd.DataFrame, last_watermark: pd.Timestamp) -> pd.DataFrame:
#     """Return only rows updated after the last watermark."""
#     return df[df["updated_at"] > last_watermark]


# Late Data Safe
def extract() -> pd.DataFrame:
    """Full extract function reading CSV and applying watermark."""
    df = pd.read_csv(settings.SOURCE_PATH, parse_dates=[settings.TIMESTAMP_COLUMN])
    last_watermark = read_watermark()
    safe_watermark = last_watermark - timedelta(days=settings.WATERMARK_LAG_DAYS)
    # new_rows = extract_new_rows(df, read_watermark())
    return df[df[settings.TIMESTAMP_COLUMNS] > safe_watermark]

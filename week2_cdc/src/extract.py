import pandas as pd
from pathlib import Path
from datetime import timedelta
from week2_cdc.src.config import settings
from week2_cdc.src.watermark import read_watermark


BASE_DIR = Path(__file__).parent.parent.parent


# Late Data Safe
def extract() -> pd.DataFrame:
    """Full extract function reading CSV and applying watermark."""
    df = pd.read_csv(
        Path(BASE_DIR / settings.SOURCE_PATH), parse_dates=[settings.TIMESTAMP_COLUMNS]
    )
    last_watermark = read_watermark()
    safe_watermark = last_watermark - timedelta(days=settings.WATERMARK_LAG_DAYS)
    return df[df[settings.TIMESTAMP_COLUMNS] > safe_watermark]

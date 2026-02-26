import pandas as pd
from week2_cdc.src.config import settings


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Sample transformation: add a processed timestamp and calculated total_price"""
    if df.empty:
        return df
    return df.assign(
        total_price=df["amount"] * df["unit_price"],
        processed_at=pd.Timestamp.now(),
        updated_date=df[settings.TIMESTAMP_COLUMNS].dt.date,
    )

import pandas as pd


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Sample transformation: add a processed timestamp and calculated total_price"""
    return df.assign(
        total_price=df["amount"] * df["unit_price"], processed_at=pd.Timestamp.now()
    )

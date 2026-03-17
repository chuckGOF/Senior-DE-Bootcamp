import pandas as pd


class DataValidationError(Exception):
    pass


def validate(df: pd.DataFrame) -> None:
    if df.empty:
        return

    if df["order_id"].isnull().any():
        raise DataValidationError("Null order_id detected")

    if df.duplicated(["order_id"]).any():
        raise DataValidationError("Duplicate order_id detected")

    if (df["amount"] < 0).any():
        raise DataValidationError("Negative amount detected")

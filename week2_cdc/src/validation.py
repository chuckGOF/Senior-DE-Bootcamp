import pandas as pd
from week2_cdc.src.config import settings


class DataValidationError(Exception):
    pass


def validate(df: pd.DataFrame) -> None:
    if df.empty:
        return  # No data is not failure

    # Null check on primary key
    if df[settings.PRIMARY_KEY].isnull().any():
        raise DataValidationError("Primary key contains NULL values.")

    # Duplicate primary keys within batch
    if df.duplicated(subset=[settings.PRIMARY_KEY]).any():
        raise DataValidationError("Duplicate primary keys detected.")

    # Future timestamps
    if (df[settings.TIMESTAMP_COLUMNS] > pd.Timestamp.now()).any():
        raise DataValidationError("Future timestamp detected.")

    # Timestamp nulls
    if df[settings.TIMESTAMP_COLUMNS].isnull().any():
        raise DataValidationError("Timestamp column contains NULL.")



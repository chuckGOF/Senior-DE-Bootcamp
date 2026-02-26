import pytest
import pandas as pd
from week2_cdc.src.validation import validate, DataValidationError


def test_duplicate_primary_key():
    df = pd.DataFrame(
        {"id": [1, 1], "updated_at": pd.to_datetime(["2026-01-01", "2026-01-02"])}
    )

    with pytest.raises(DataValidationError):
        validate(df)


def test_future_timestamp():
    df = pd.DataFrame(
        {"id": [1], "updated_at": [pd.Timestamp.now() + pd.Timedelta(days=1)]}
    )

    with pytest.raises(DataValidationError):
        validate(df)


def test_null_timestamp():
    df = pd.DataFrame({"id": [1], "updated_at": pd.NA})

    with pytest.raises(DataValidationError):
        validate(df)


def test_null_primary_key():
    df = pd.DataFrame({"id": [pd.NA], "updated_at": pd.Timestamp.now()})

    with pytest.raises(DataValidationError):
        validate(df)

import pandas as pd


def test_schema(df: pd.DataFrame, expected_columns):
    actual = set(df.columns)
    expected = set(expected_columns)

    assert actual == expected, f"Schema mismatch: expected {expected}, got {actual}"

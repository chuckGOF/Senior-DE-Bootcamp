import pandas as pd


def test_partition_integrity(df: pd.DataFrame, partition_column) -> None:
    assert (
        df[partition_column].isnull().sum() == 0
    ), f"Null values found in partition column '{partition_column}'"

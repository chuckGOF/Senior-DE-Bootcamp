import pandas as pd


def test_no_duplicate_keys(df: pd.DataFrame, primary_key) -> None:
    duplicates = df[df.duplicated(subset=primary_key)]

    assert (
        len(duplicates) == 0
    ), f"Duplicate keys found: {duplicates[primary_key].tolist()}"

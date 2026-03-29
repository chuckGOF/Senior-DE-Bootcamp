import pandas as pd
from typing import List


class Transformations:
    def enforce_schema(self, df: pd.DataFrame, schema: dict) -> pd.DataFrame:
        for col, dtype in schema.items():
            df[col] = df[col].astype(dtype)
        return df

    def remove_duplicates(self, df: pd.DataFrame, pk: List[str] | str) -> pd.DataFrame:
        return df.drop_duplicates(subset=[pk])

    def handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.fillna(0)

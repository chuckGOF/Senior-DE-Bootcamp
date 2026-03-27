import pandas as pd


def align_schema(existing_schema, df) -> pd.DataFrame:
    for col in existing_schema:
        if col not in df.columns:
            df[col] = None
    return df[existing_schema]

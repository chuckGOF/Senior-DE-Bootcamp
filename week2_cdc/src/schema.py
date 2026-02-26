import pandas as pd


# Schema Alignment
def align_schema(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """Ensure new_df has the same columns as existing_df, filling missing ones with NaN."""
    for col in existing_df.columns:
        if col not in new_df.columns:
            new_df[col] = pd.NA

    for col in new_df.columns:
        if col not in existing_df.columns:
            existing_df[col] = pd.NA

    return new_df[existing_df.columns.tolist()]

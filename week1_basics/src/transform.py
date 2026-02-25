import polars as pl


def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("amount") > 0).with_columns(
        (pl.col("amount") * pl.col("unit_price")).alias("total_price")
    )

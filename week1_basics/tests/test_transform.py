import polars as pl
from week1_basics.src.transform import transform


def test_transform_filters_negative_amounts():
    df = pl.DataFrame({"amount": [10, -5], "unit_price": [2, 3]})

    result = transform(df)

    assert result.shape[0] == 1
    assert "total_price" in result.columns

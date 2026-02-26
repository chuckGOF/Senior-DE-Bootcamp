import pandas as pd
from week2_cdc.src.extract import extract_new_rows


def test_extract_new_rows():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "updated_at": pd.to_datetime(
                [
                    "2026-01-01 10:00",
                    "2026-01-02 10:00",
                    "2026-01-03 10:00",
                    "2026-01-04 10:00",
                ]
            ),
        }
    )
    last_watermark = pd.Timestamp("2026-01-02 00:00")
    new_rows = extract_new_rows(df, last_watermark)
    assert new_rows.shape[0] == 3
    assert new_rows["id"].tolist() == [2, 3, 4]

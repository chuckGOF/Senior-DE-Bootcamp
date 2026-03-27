from datetime import datetime


def test_watermark(new_watermark: datetime, last_watermark: datetime) -> None:
    assert (
        new_watermark > last_watermark
    ), f"Watermark did not progress: last={last_watermark}, new={new_watermark}"

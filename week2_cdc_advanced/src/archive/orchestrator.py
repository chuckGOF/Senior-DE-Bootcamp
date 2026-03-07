from week2_cdc_advanced.src.archive.extractor import extract
from week2_cdc_advanced.src.archive.watermark_repository import update_watermark
from week2_cdc_advanced.src.archive.writer_s3 import write_to_s3


def run():
    df = extract()

    if df.empty:
        print("No new data to process.")
        return

    write_to_s3(df)
    update_watermark(df["updated_at"].max())

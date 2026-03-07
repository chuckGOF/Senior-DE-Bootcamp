from week2_cdc_advanced.src.warehouse.watermark_repo import WatermarkRepository
from week2_cdc_advanced.src.warehouse.extractor import extract_incremental
from week2_cdc_advanced.src.writers.adls_writer import ADLSWriter
from week2_cdc_advanced.src.writers.s3_writer import S3Writer
from week2_cdc_advanced.src.cdc.locks import acquire_file_lock
from week2_cdc_advanced.src.warehouse.connection import get_connection


def run():
    conn = get_connection()
    watermark_repo = WatermarkRepository(conn, "sales_orders_cdc")

    lock = acquire_file_lock()

    try:
        last_wm = watermark_repo.acquire_lock()
        df = extract_incremental(conn, last_wm)

        if df.empty:
            return

        adls_writer = ADLSWriter()
        s3_writer = S3Writer()

        adls_writer.write(df)
        s3_writer.write(df)

        watermark_repo.update(df["updated_at"].max())
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        lock.release()


if __name__ == "__main__":
    run()

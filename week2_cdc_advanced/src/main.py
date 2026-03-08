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

        df["updated_date"] = df["updated_at"].dt.date
        partitions = df["updated_date"].unique()

        adls_writer = ADLSWriter()
        s3_writer = S3Writer()
        adls_staging, s3_staging = [], []
        for partition in partitions:
            s1, f1 = adls_writer.write_partition(
                df[df["updated_date"] == partition], partition
            )
            s2, f2 = s3_writer.write_partition(
                df[df["updated_date"] == partition], partition
            )

            adls_staging.append((s1, f1))
            s3_staging.append((s2, f2))

        # Promote partitions after all writes succeed
        for s1, f1 in adls_staging:
            adls_writer.promote(s1, f1)

        for s2, f2 in s3_staging:
            s3_writer.promote(s2, f2)

        watermark_repo.update(df["updated_at"].max())
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        lock.release()


if __name__ == "__main__":
    run()

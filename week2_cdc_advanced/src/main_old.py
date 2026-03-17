import time
import sys

from week2_cdc_advanced.src.warehouse.watermark_repo import WatermarkRepository
from week2_cdc_advanced.src.warehouse.extractor import extract_incremental
from week2_cdc_advanced.src.writers.adls_writer import ADLSWriter
from week2_cdc_advanced.src.writers.s3_writer import S3Writer
from week2_cdc_advanced.src.cdc.locks import acquire_file_lock
from week2_cdc_advanced.src.cdc.validator import validate, DataValidationError
# from week2_cdc_advanced.src.cdc.schme_manager import align_schema
from week2_cdc_advanced.src.warehouse.connection import get_connection
from week2_cdc_advanced.src.observability.metrics import (
    cdc_runs, 
    cdc_failures, 
    cdc_duration, 
    validation_failures, 
    cdc_rows_processed_total, 
    cdc_partitions_written_total, 
    cdc_watermark_log_seconds
)

def run():
    start_time = time.time()
    cdc_runs.inc()

    conn = get_connection()
    watermark_repo = WatermarkRepository(conn, "sales_orders_cdc")
    cdc_watermark_log_seconds.observe(0)  # Log initial watermark read time


    lock = acquire_file_lock()

    try:
        last_wm = watermark_repo.acquire_lock()
        cdc_watermark_log_seconds.observe(time.time() - start_time)  # Log watermark read time

        for chunk in extract_incremental(conn, last_wm):
            if chunk.empty:
                return
            
            validate(chunk)

            chunk["updated_date"] = chunk["updated_at"].dt.date
            partitions = chunk["updated_date"].unique()

            adls_writer = ADLSWriter()
            s3_writer = S3Writer()
            adls_staging, s3_staging = [], []
            for partition in partitions:
                s1, f1 = adls_writer.write_partition(
                    chunk[chunk["updated_date"] == partition], partition
                )
                s2, f2 = s3_writer.write_partition(
                    chunk[chunk["updated_date"] == partition], partition
                )

                adls_staging.append((s1, f1))
                s3_staging.append((s2, f2))

            cdc_rows_processed_total.inc(len(chunk))
            cdc_partitions_written_total.inc(len(partitions))
            
            # Promote partitions after all writes succeed
            for s1, f1 in adls_staging:
                adls_writer.promote(s1, f1)

            for s2, f2 in s3_staging:
                s3_writer.promote(s2, f2)

        duration = time.time() - start_time
        cdc_duration.observe(duration)

        watermark_repo.update(df["updated_at"].max())
        cdc_watermark_log_seconds.observe(time.time() - start_time)  # Log total time to update watermark
        conn.commit()

    except DataValidationError:
        validation_failures.inc()
        conn.rollback()
        sys.exit(1)

    except Exception as _:
        cdc_failures.inc()
        conn.rollback()
        sys.exit(1)

    finally:
        lock.release()


if __name__ == "__main__":
    run()

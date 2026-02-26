import sys
import time
from pathlib import Path
from prometheus_client import start_http_server, Counter, Histogram
from week1_basics.logger import get_logger
from week2_cdc.src.extract import extract
from week2_cdc.src.transform import transform
from week2_cdc.src.load import load

# Base Directory
BASE_DIR = Path(__file__).parent.parent.parent

# Add Logger
logger = get_logger("cdc_pipeline_prod")


# Prometheus metrics
cdc_runs = Counter("cdc_runs_total", "Total number of CDC runs")
cdc_failures = Counter("cdc_failures_total", "Total number of CDC failures")
cdc_duration = Histogram("cdc_duration_seconds", "Time spent running CDC")


def run():
    start_time = time.time()
    cdc_runs.inc()
    try:
        logger.info("Starting production CDC pipeline")

        df = extract()

        if df.empty:
            logger.info("No new data to process")
            return

        transformed_df = transform(df)
        load(transformed_df)

        duration = time.time() - start_time
        cdc_duration.observe(duration)

        logger.info(
            f"CDC pipeline completed successfully."
            f"Rows processed: {len(transformed_df)}."
            f"Duration: {duration:.2f}s"
        )

    except Exception as _:
        cdc_failures.inc()
        logger.exception("CDC pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    start_http_server(8000)
    run()

    # # # Keep alive for Prometheus scraping
    # while True:
    #     time.sleep(5)

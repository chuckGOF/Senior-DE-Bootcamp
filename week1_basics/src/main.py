# import logging
import time
import sys

from pathlib import Path
from prometheus_client import start_http_server, Counter, Histogram

from week1_basics.config import settings
from week1_basics.logger import get_logger
from week1_basics.src.extract import extract
from week1_basics.src.transform import transform
from week1_basics.src.load import load

# Base directory
BASE_DIR = Path(__file__).parent.parent


# logging.basicConfig(level=settings.log_level)
# Logger
logger = get_logger(__name__)

# Prometheus metrics

etl_runs = Counter("etl_runs_total", "Total number of ETL runs")
etl_failutes = Counter("etl_failures_total", "Total number of ETL failures")
etl_duration = Histogram("etl_duration_seconds", "Time spent running ETL")


def run():
    start_time = time.time()
    etl_runs.inc()

    try:
        # logging.info("Starting ETL job")
        logger.info("Starting ETL job")

        input_path = Path(BASE_DIR / settings.input_path)
        output_path = Path(BASE_DIR / settings.output_path)

        df = extract(input_path)
        df = transform(df)
        load(df, output_path)

        duration = time.time() - start_time
        etl_duration.observe(duration)

        # logging.info("ETL completed successfully")
        logger.info(f"ETL completed successfully in {duration:.2} seconds")

    except Exception as _:
        etl_failutes.inc()
        logger.exception("ETL job failed")
        sys.exit(1)


if __name__ == "__main__":
    # Start Prometheus metrics server
    start_http_server(8000)
    run()

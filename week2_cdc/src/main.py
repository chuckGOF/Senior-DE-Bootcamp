import sys
import time
from pathlib import Path
from prometheus_client import start_http_server, Counter, Histogram
from week1_basics.logger import get_logger
from week1_basics.config import settings
from week2_cdc.src.extract import extract
from week2_cdc.src.transform import transform
from week2_cdc.src.load import load

# Base Directory
BASE_DIR = Path(__file__).parent.parent.parent
print(BASE_DIR)

# Add Logger
logger = get_logger("cdc_pipeline")


# Prometheus metrics
cdc_runs = Counter("cdc_runs_total", "Total number of CDC runs")
cdc_failures = Counter("cdc_failures_total", "Total number of CDC failures")
cdc_duration = Histogram("cdc_duration_seconds", "Time spent running CDC")


def run():
    start_time = time.time()
    cdc_runs.inc()
    try:
        logger.info("Starting CDC ETL job")
        df = extract(Path(BASE_DIR / "week1_basics" / settings.input_path))
        transformed_df = transform(df)
        load(transformed_df, Path(BASE_DIR / "week2_cdc" / settings.output_path))
        duration = time.time() - start_time
        cdc_duration.observe(duration)
        logger.info(
            f"CDC ETL completed successfully, {len(transformed_df)} rows processed in {duration:.2f}s"
        )
    except Exception as _:
        cdc_failures.inc()
        logger.exception("CDC ETL failed")
        sys.exit(1)


if __name__ == "__main__":
    start_http_server(8000)
    run()

    # # Keep alive for Prometheus scraping
    # import time
    # while True:
    #     time.sleep(5)

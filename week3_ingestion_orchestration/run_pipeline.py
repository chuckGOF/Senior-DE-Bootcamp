from week3_ingestion_orchestration.pipeline.orchestrate import run_all_tables


def run():
    run_all_tables(start_metrics_server=True)


if __name__ == "__main__":
    run()

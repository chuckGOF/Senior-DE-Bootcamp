import time

from week2_ingestion_production_framework.core.run_manager import RunManager
from week2_ingestion_production_framework.core.client import ADLSClient

from week2_ingestion_production_framework.bronze.metadata.metadata_manager import (
    MetadataManager,
)
from week2_ingestion_production_framework.silver.bronze_reader import BronzeReader
from week2_ingestion_production_framework.silver.silver_promoter import SilverPromoter
from week2_ingestion_production_framework.silver.silver_writer import SilverWriter
from week2_ingestion_production_framework.silver.transformations import Transformations
from week2_ingestion_production_framework.silver.models.schema_registry import schemas
from week2_ingestion_production_framework.core.connection import get_connection


def run():
    run_manager = RunManager()
    run_id = run_manager.get_run_id()
    storage = ADLSClient()
    conn = get_connection()

    metadata_mgr = MetadataManager(conn)
    tables = metadata_mgr.get_tables()

    tranform = Transformations()
    writer = SilverWriter(storage, "silver")
    promoter = SilverPromoter(storage)

    for t in tables:
        table = t.table_name
        partition = t.partition_column
        primary_key = t.primary_key
        bronze_path = f"bronze/{table}"

        reader = BronzeReader(storage, bronze_path)

        start_time = time.time()

        try:
            df = reader.read_table()
            df = tranform.enforce_schema(df, schemas[table])
            df = tranform.remove_duplicates(df, primary_key)
            df = tranform.handle_nulls(df)

            writer.write_partition(df, table, partition, run_id)

            promoter.promote(f"silver/_staging/{table}/{run_id}")

            print(f"Bronze ==> Silve done in: {time.time() - start_time}")

        except Exception as _:
            raise


if __name__ == "__main__":
    run()

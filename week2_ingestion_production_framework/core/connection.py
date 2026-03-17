import pyodbc
from week2_ingestion_production_framework.config.settings import settings


def get_connection():
    return pyodbc.connect(
        settings.fabric_conn_str,
        autocommit=False,  # watermark + data must be transactional # warehouse is source of truth not local state
    )
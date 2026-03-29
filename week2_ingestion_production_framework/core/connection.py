import pyodbc
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from week2_ingestion_production_framework.config.settings import settings


def get_connection():
    return pyodbc.connect(
        settings.fabric_conn_str,
        autocommit=False,  # watermark + data must be transactional # warehouse is source of truth not local state
    )


def get_engine():
    # SQLAlchemy engine for pandas read_sql_query in Extractor
    odbc_connect = quote_plus(settings.fabric_conn_str)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={odbc_connect}",
        pool_pre_ping=True,
        future=True,
    )

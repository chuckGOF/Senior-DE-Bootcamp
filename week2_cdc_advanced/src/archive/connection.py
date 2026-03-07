import pyodbc
from src.config.settings import settings


def get_connection():
    return pyodbc.connect(
        settings.FABRIC_CONN_STR,
        autocommit=False,  # watermark + data must be transactional # warehouse is source of truth not local state
    )

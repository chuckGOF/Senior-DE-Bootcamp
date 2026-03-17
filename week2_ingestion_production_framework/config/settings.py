from pydantic_settings import BaseSettings
from typing import Optional
from dotenv import load_dotenv
import os


load_dotenv()  # Load environment variables from .env file


class Settings(BaseSettings):
    sql_endpoint: str = (
        "tbgkgxebvgdexodp5l66tysnv4-wytthnplb6pefemsjaiymsqqji.datawarehouse.fabric.microsoft.com"
    )
    database: str = os.getenv(
        "FABRIC_DATABASE"
    )  # Default to 'sales_orders_db' if not set
    driver: str = "ODBC Driver 18 for SQL Server"
    port: int = 1433
    uid: Optional[str] = os.getenv(
        "FABRIC_UID"
    )  # It's a good practice to load sensitive info from env variables
    password: Optional[str] = os.getenv(
        "FABRIC_PASSWORD"
    )  # It's a good practice to load sensitive info from env variables
    authentication: str = "ActiveDirectoryPassword"
    encrypt: bool = True
    trust_server_certificate: bool = False

    @property
    def fabric_conn_str(self):
        return (
            f"Driver={{{self.driver}}};"
            f"Server={self.sql_endpoint},{self.port};"
            f"Database={self.database};"
            f"UID={self.uid};"
            f"PWD={self.password};"
            f"Authentication={self.authentication};"
            f"Encrypt={'Yes' if self.encrypt else 'No'};"
            f"TrustServerCertificate={'Yes' if self.trust_server_certificate else 'No'};"
        )


settings = Settings()


class CloudSettings(BaseSettings):
    S3_BUCKET: Optional[str] = None
    ADLS_ACCOUNT_NAME: Optional[str] = None
    ADLS_CONTAINER: Optional[str] = None
    ADLS_ACCESS_KEY: Optional[str] = os.getenv(
        "ADLS_ACCESS_KEY"
    )  # It's a good practice to load sensitive info from env variables


cloud_settings = CloudSettings()

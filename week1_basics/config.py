from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    input_path: Path = Path("data/sales.csv")
    output_path: Path = Path("output/cleaned.parquet")
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

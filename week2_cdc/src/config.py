from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SOURCE_PATH: str = "data/sales.csv"
    OUTPUT_PATH: str = "output/output_wk2"
    WATERMARK_PATH: str = "watermark.json"
    WATERMARK_LAG_DAYS: int = 2
    PRIMARY_KEY: str = "id"
    TIMESTAMP_COLUMNS: str = "updated_at"


settings = Settings()

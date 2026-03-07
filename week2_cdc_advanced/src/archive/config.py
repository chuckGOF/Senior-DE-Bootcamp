from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PIPELINE_NAME: str = "sales_cdc_pipeline"


settings = Settings()

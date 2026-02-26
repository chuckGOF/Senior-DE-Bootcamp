import polars as pl
from pathlib import Path
from week1_basics.config import settings

# Base directory
BASE_DIR = Path(__file__).parent.parent.parent


def extract() -> pl.DataFrame:
    return pl.read_csv(Path(BASE_DIR / settings.input_path))

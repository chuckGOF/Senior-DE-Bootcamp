import polars as pl
from pathlib import Path
from week1_basics.config import settings

BASE_DIR = Path(__file__).parent.parent.parent


def load(df: pl.DataFrame) -> None:
    Path(BASE_DIR / settings.output_path).parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(Path(BASE_DIR / settings.output_path))

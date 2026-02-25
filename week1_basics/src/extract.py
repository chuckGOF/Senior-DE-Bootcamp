from pathlib import Path
import polars as pl
from typing import Union


def extract(path: Union[str, Path]) -> pl.DataFrame:
    return pl.read_csv(path)

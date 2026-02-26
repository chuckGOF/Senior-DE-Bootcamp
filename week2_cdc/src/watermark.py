import os
import json
import tempfile
import pandas as pd
from pathlib import Path
from week2_cdc.src.config import settings


def read_watermark() -> pd.Timestamp:
    path = Path(settings.WATERMARK_PATH)
    if not path.exists():
        return pd.Timestamp("1970-01-01 00:00:00")

    with open(path, "r") as f:
        data = json.load(f)
    return pd.Timestamp(data["last_updated"])


# Atomic Write
def write_watermark_atomic(timestamp: pd.Timestamp) -> None:
    temp = tempfile.NamedTemporaryFile(delete=False)
    json.dump({"last_updated": timestamp.isoformat()}, temp)
    temp.close()
    os.replace(temp.name, settings.WATERMARK_PATH)

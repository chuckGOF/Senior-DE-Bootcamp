import uuid
from datetime import datetime, timezone


def generate_run_id():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    uid = str(uuid.uuid4())[:8]
    return f"{ts}_{uid}"

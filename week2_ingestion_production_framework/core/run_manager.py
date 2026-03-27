import uuid
from datetime import datetime, timezone


class RunManager:
    def __init__(self):
        self.run_id = str(uuid.uuid4())
        self.start_time = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    def get_run_id(self):
        return self.run_id

import os
from pathlib import Path


class FileLock:
    def __init__(self, lock_file: str = "pipeline.lock"):
        self.lock_path = Path(lock_file)

    def acquire(self):
        if self.lock_path.exists():
            raise RuntimeError("Another instance of the pipeline is already running.")

        self.lock_path.write_text(str(os.getpid()))

    def release(self):
        if self.lock_path.exists():
            self.lock_path.unlink()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

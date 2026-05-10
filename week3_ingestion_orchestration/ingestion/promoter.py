# ingestion/promoter.py
class Promoter:
    def __init__(self, storage):
        self.storage = storage

    def promote(self, staging_prefix: str):
        # e.g. "_staging/orders/20260317"
        for rel_path in self.storage.list_files(staging_prefix):
            target_path = rel_path.replace("_staging/", "", 1)
            data = self.storage.read_file(rel_path)
            self.storage.upload_bytes(target_path, data)
            self.storage.delete_file(rel_path)

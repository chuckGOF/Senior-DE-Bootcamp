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


class PromoterOLD:
    def __init__(self, adls):
        self.adls = adls

    def promote(self, staging_prefix: str):
        # staging_prefic should be relative e.g. "_staging/orders/20260317"
        full_staging_prefix = f"{self.adls.base_path}/{staging_prefix}"
        full_paths = self.adls.fs.find(full_staging_prefix)

        for full_path in full_paths:
            rel_path = full_path.replace(f"{self.adls.base_path}/", "", 1)
            new_rel_path = rel_path.replace("_staging/", "", 1)

            data = self.adls.read_file(rel_path)
            self.adls.upload_bytes(new_rel_path, data)
            self.adls.delete_file(rel_path)

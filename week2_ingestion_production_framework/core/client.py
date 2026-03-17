from adlfs import AzureBlobFileSystem
from week2_ingestion_production_framework.config.settings import cloud_settings



class ADLSClient:
    def __init__(self):
        self.fs = AzureBlobFileSystem(
            account_name=cloud_settings.ADLS_ACCOUNT_NAME,
            account_key=cloud_settings.ADLS_ACCESS_KEY,
        )
        self.base_path = f"{cloud_settings.ADLS_CONTAINER}/cdc-production-framework"

    def _full_path(self, path: str) -> str:
        return f"{self.base_path}/{path}"

    def upload_bytes(self, path: str, data: bytes) -> None:
        full_path = self._full_path(path)
        with self.fs.open(full_path, "wb") as f:
            f.write(data)
    
    def read_file(self, path: str) -> bytes:
        full_path = self._full_path(path)
        with self.fs.open(full_path, "rb") as f:
            return f.read()
        
    def delete_file(self, path: str) -> None:
        full_path = self._full_path(path)
        self.fs.rm(full_path)

    def file_exists(self, path: str) -> bool:
        return self.fs.exists(self._full_path(path))
    
    def list_files(self, prefix: str):
        full_prefix = self._full_path(prefix)
        for full_path in self.fs.find(full_prefix):
            yield full_path.replace(f"{self.base_path}/", "", 1)


class S3Client:
    def __init__(self):
        pass
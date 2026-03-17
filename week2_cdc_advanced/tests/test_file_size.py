import os

def test_file_size(path: str) -> None:
    files = os.listdir(path)

    for f in files:
        size = os.path.getsize(os.path.join(path, f))
        assert size > 1_000_000, f"Small file detected: {f} with size {size} bytes"
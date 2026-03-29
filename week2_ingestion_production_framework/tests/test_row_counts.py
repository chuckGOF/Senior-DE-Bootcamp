# Ensuew ingestion doesn't silently drop rows.
def validate_row_count(source_rows, written_rows):
    if written_rows < source_rows * 0.99:
        raise Exception(
            f'Row loss detected. Source={source_rows} Written={written_rows}'
        )
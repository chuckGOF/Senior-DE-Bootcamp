from week3_ingestion_orchestration.quality.validations import (
    validate_is_not_empty,
    validate_no_duplicates,
    validate_no_null_partition
)

def run_validate(chunks, primary_key, partition):
    for chunk in chunks:
        validate_is_not_empty(chunk)
        validate_no_duplicates(chunk, primary_key)
        validate_no_null_partition(chunk, partition)
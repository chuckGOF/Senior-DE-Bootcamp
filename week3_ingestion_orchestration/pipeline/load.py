from week3_ingestion_orchestration.quality.validations import (
    validate_no_duplicates,
    validate_no_null_partition,
    validate_watermark_progression,
    validate_row_count,
)


def run_load(
    writer,
    metrics,
    run_id,
    table,
    source_rows,
    partition,
    primary_key,
    watermark_column,
    watermark,
    chunks,
    progress=None,
):
    written_rows = 0
    max_watermark = watermark

    if progress is not None:
        progress["written_rows"] = 0

    for chunk in chunks:
        validate_no_duplicates(chunk, primary_key)
        validate_no_null_partition(chunk, partition)

        partition_values = chunk[partition].dropna().astype("string").unique().tolist()
        writer.write_partition(chunk, table, partition, run_id)
        written_rows += len(chunk)
        if progress is not None:
            progress["written_rows"] = written_rows

        metrics.add_chunk(
            table=table,
            rows=len(chunk),
            files=len(partition_values),
            partitions=len(partition_values),
        )

        chunk_max = chunk[watermark_column].max()
        if chunk_max > max_watermark:
            max_watermark = chunk_max

    validate_row_count(source_rows, written_rows)

    if source_rows > 0:
        validate_watermark_progression(watermark, max_watermark)

    return written_rows, max_watermark

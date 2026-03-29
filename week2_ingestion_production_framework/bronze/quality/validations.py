class DataQualityValidation(Exception):
    pass


def validate_no_null_partition(df, partition_column):
    if partition_column not in df.columns:
        raise DataQualityValidation(
            f"Partition column '{partition_column}' missing from dataframe"
        )

    if df[partition_column].isnull().sum() > 0:
        raise DataQualityValidation(
            f"Null values found in partition column '{partition_column}'"
        )


def validate_no_duplicates(df, primary_key):
    if df.duplicated(subset=primary_key).sum() > 0:
        raise DataQualityValidation(
            f"Duplicate values found in primary key '{primary_key}'"
        )


def validate_watermark_progression(old_wm, new_wm):
    if new_wm <= old_wm:
        raise DataQualityValidation(f"Watermark did not advance: {old_wm} -> {new_wm}")


def validate_row_count(source_rows, written_rows):
    if written_rows < source_rows * 0.99:
        raise DataQualityValidation(
            f"Row loss detected. Source={source_rows} Written={written_rows}"
        )

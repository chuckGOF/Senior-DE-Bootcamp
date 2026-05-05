# Ensures partition column exists and is not null
def validate_partition_integrity(df, partition_column):
    if partition_column not in df.columns:
        raise Exception("Partition column missing")

    if df[partition_column].isnull().sum() > 0:
        raise Exception("Null partition column detected")

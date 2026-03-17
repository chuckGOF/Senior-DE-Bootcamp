class DataQualityValidation(Exception):
    pass


def validate_no_null_partition(df, partition_column):
    if df[partition_column].isnull().sum() > 0:
        raise DataQualityValidation(f"Null values found in partition column '{partition_column}'")
    

def validate_no_duplicates(df, primary_key):
    if df.duplicated(subset=primary_key).sum() > 0:
        raise DataQualityValidation(f"Duplicate values found in primary key '{primary_key}'")
# Protect against CDC duplication
def validate_no_duplicate_keys(df, primary_key):
    duplicates =df[df.duplicated(primary_key)]

    if len(duplicates) > 0:
        raise Exception(
            f'Duplicate primary keys detected: {len(duplicates)}'
        )
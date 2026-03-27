def test_row_count(source_count, ingested_count) -> None:
    assert (
        ingested_count >= source_count * 0.99
    ), f"Row count mismatch: source={source_count}, ingested={ingested_count}"

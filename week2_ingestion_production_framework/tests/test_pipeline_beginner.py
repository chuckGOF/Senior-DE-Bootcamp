import pandas as pd
import pytest
from types import SimpleNamespace

from week2_ingestion_production_framework.bronze.ingestion.extractor import Extractor
import week2_ingestion_production_framework.bronze.pipeline.run_pipeline as rp


def test_extractor_contract_returns_count_and_chunks(monkeypatch):
    calls = []

    def fake_read_sql(query, conn, params=None, chunksize=None):
        calls.append((query, params, chunksize))
        if "COUNT(*)" in query:
            return pd.DataFrame({"total_rows": [3]})
        return iter(
            [
                pd.DataFrame({"id": [1, 2], "updated_at": [11, 12]}),
                pd.DataFrame({"id": [3], "updated_at": [13]}),
            ]
        )

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    source_rows, chunks = Extractor("fake-conn").extract_incremental(
        "dbo", "sales", "updated_at", 10, chunksize=2
    )

    chunk_list = list(chunks)
    assert source_rows == 3
    assert [len(c) for c in chunk_list] == [2, 1]
    assert calls[0][1] == [10]
    assert calls[1][1] == [10]
    assert calls[1][2] == 2


@pytest.fixture
def patch_pipeline_base(monkeypatch):
    table_cfg = SimpleNamespace(
        table_name="sales",
        source_schema="dbo",
        partition_column="dt",
        watermark_column="updated_at",
        primary_key="id",
    )

    class FakeRunManager:
        def get_run_id(self):
            return "run-123"

    class FakeMetadataManager:
        def __init__(self, conn):
            self.updated = None
            self.logs = []

        def get_tables(self):
            return [table_cfg]

        def get_watermark(self, table):
            return 100

        def update_watermark(self, table, wm):
            self.updated = (table, wm)

        def log_pipeline_run(self, *args):
            self.logs.append(args)

    class FakeWriter:
        def __init__(self, storage, zone):
            self.rows = 0

        def write_partition(self, chunk, table, partition, run_id):
            self.rows += len(chunk)

    class FakePromoter:
        def __init__(self, storage):
            self.paths = []

        def promote(self, path):
            self.paths.append(path)

    monkeypatch.setattr(rp, "RunManager", FakeRunManager)
    monkeypatch.setattr(rp, "get_connection", lambda: "fake-conn")
    monkeypatch.setattr(rp, "ADLSClient", lambda: object())
    monkeypatch.setattr(rp, "MetadataManager", FakeMetadataManager)
    monkeypatch.setattr(rp, "Writer", FakeWriter)
    monkeypatch.setattr(rp, "Promoter", FakePromoter)


def test_pipeline_runs_all_checks_when_rows_exist(monkeypatch, patch_pipeline_base):
    class FakeExtractor:
        def __init__(self, conn):
            pass

        def extract_incremental(self, *args, **kwargs):
            chunks = iter(
                [
                    pd.DataFrame(
                        {
                            "id": [1, 2],
                            "dt": ["2026-01-01", "2026-01-01"],
                            "updated_at": [101, 102],
                        }
                    )
                ]
            )
            return 2, chunks

    calls = {"partition": 0, "dup": 0, "row": 0, "wm": 0}

    monkeypatch.setattr(rp, "Extractor", FakeExtractor)
    monkeypatch.setattr(rp, "validate_no_null_partition", lambda df, col: calls.__setitem__("partition", calls["partition"] + 1))
    monkeypatch.setattr(rp, "validate_no_duplicates", lambda df, key: calls.__setitem__("dup", calls["dup"] + 1))
    monkeypatch.setattr(rp, "validate_row_count", lambda s, w: (calls.__setitem__("row", calls["row"] + 1), assert_eq((s, w), (2, 2))))
    monkeypatch.setattr(rp, "validate_watermark_progression", lambda o, n: (calls.__setitem__("wm", calls["wm"] + 1), assert_eq((o, n), (100, 102))))

    rp.run()

    assert calls == {"partition": 1, "dup": 1, "row": 1, "wm": 1}


def test_pipeline_source_rows_zero_skips_watermark_check(monkeypatch, patch_pipeline_base):
    class FakeExtractor:
        def __init__(self, conn):
            pass

        def extract_incremental(self, *args, **kwargs):
            return 0, iter([])

    calls = {"row": 0, "wm": 0}

    monkeypatch.setattr(rp, "Extractor", FakeExtractor)
    monkeypatch.setattr(rp, "validate_no_null_partition", lambda df, col: None)
    monkeypatch.setattr(rp, "validate_no_duplicates", lambda df, key: None)
    monkeypatch.setattr(rp, "validate_row_count", lambda s, w: (calls.__setitem__("row", calls["row"] + 1), assert_eq((s, w), (0, 0))))
    monkeypatch.setattr(rp, "validate_watermark_progression", lambda o, n: calls.__setitem__("wm", calls["wm"] + 1))

    rp.run()

    assert calls["row"] == 1
    assert calls["wm"] == 0


def assert_eq(actual, expected):
    assert actual == expected
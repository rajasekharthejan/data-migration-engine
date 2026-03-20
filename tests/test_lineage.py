"""Tests for the metadata lineage tracking engine."""

import pytest

from src.lineage.lineage_engine import LineageEngine, LineageEntry


class TestLineageEngine:
    """Test suite for data lineage tracking."""

    @pytest.fixture
    def engine(self):
        return LineageEngine(batch_size=100)

    def test_track_single_entry(self, engine):
        entry = engine.track(
            job_id="JOB-001",
            record_id="POL-100001",
            source_table="db2_policies",
            source_column="PREM_AMT",
            source_value="1250.50",
            target_table="fast_policies",
            target_column="premium_amount",
            target_value="1250.50",
            transformation="packed_decimal_to_float",
        )

        assert isinstance(entry, LineageEntry)
        assert entry.job_id == "JOB-001"
        assert entry.transformation == "packed_decimal_to_float"

    def test_track_batch(self, engine):
        entries = engine.track_batch(
            job_id="JOB-001",
            record_id="POL-100001",
            source_table="db2_policies",
            target_table="fast_policies",
            field_mappings=[
                {
                    "source_column": "FIRST_NM",
                    "source_value": "JOHN",
                    "target_column": "first_name",
                    "target_value": "John",
                    "transformation": "name_title_case",
                },
                {
                    "source_column": "SSN",
                    "source_value": "123-45-6789",
                    "target_column": "ssn",
                    "target_value": "***-**-6789",
                    "transformation": "ssn_mask",
                },
            ],
        )

        assert len(entries) == 2

    def test_forward_lineage_query(self, engine):
        engine.track(
            job_id="JOB-001",
            record_id="R1",
            source_table="db2_policies",
            source_column="PREM_AMT",
            source_value="100",
            target_table="fast_policies",
            target_column="premium_amount",
            target_value="100.0",
            transformation="packed_decimal_to_float",
        )

        results = engine.query_forward("db2_policies", "PREM_AMT")
        assert len(results) == 1
        assert results[0]["target_column"] == "premium_amount"

    def test_backward_lineage_query(self, engine):
        engine.track(
            job_id="JOB-001",
            record_id="R1",
            source_table="db2_policies",
            source_column="PREM_AMT",
            source_value="100",
            target_table="fast_policies",
            target_column="premium_amount",
            target_value="100.0",
            transformation="packed_decimal_to_float",
        )

        results = engine.query_backward("fast_policies", "premium_amount")
        assert len(results) == 1
        assert results[0]["source_column"] == "PREM_AMT"

    def test_impact_analysis(self, engine):
        # Track multiple fields from same source column
        engine.track(
            job_id="J1", record_id="R1",
            source_table="db2_policies", source_column="STATUS_CD",
            source_value="A", target_table="fast_policies",
            target_column="status", target_value="active",
            transformation="status_code_map",
        )
        engine.track(
            job_id="J1", record_id="R1",
            source_table="db2_policies", source_column="STATUS_CD",
            source_value="A", target_table="fast_reports",
            target_column="policy_status", target_value="active",
            transformation="status_code_map",
        )

        impact = engine.impact_analysis("db2_policies", "STATUS_CD")
        assert impact["affected_tables"] == 2

    def test_flush(self, engine):
        for i in range(5):
            engine.track(
                job_id="J1", record_id=f"R{i}",
                source_table="src", source_column="col",
                source_value="v", target_table="tgt",
                target_column="col", target_value="v",
                transformation="copy",
            )

        flushed = engine.flush()
        assert flushed == 5
        assert engine.get_stats()["buffered"] == 0
        assert engine.get_stats()["flushed"] == 5

    def test_auto_flush_on_batch_size(self):
        engine = LineageEngine(batch_size=3)
        for i in range(5):
            engine.track(
                job_id="J1", record_id=f"R{i}",
                source_table="src", source_column="col",
                source_value="v", target_table="tgt",
                target_column="col", target_value="v",
                transformation="copy",
            )

        stats = engine.get_stats()
        assert stats["flushed"] == 3  # Auto-flushed at batch_size=3
        assert stats["buffered"] == 2

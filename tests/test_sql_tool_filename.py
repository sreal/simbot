"""Tests for the Slack SQL tool's filename builder and CSV helpers."""

import csv
import io

import pytest

from simbot.interfaces.slack.tools.sql_tool import DomainSQLTool


class TestBuildCsvFilename:
    def test_uuid_param_truncated_to_first_8(self):
        name = DomainSQLTool._build_csv_filename(
            "version_synapse",
            {"VersionID": "DBB7EC81-FE4D-443F-A8E3-831469AFB40F"},
        )
        assert name == "simbot_version_synapse_dbb7ec81.csv"

    def test_sub_label_inserted_before_param(self):
        name = DomainSQLTool._build_csv_filename(
            "version_sync",
            {"VersionID": "DBB7EC81-FE4D-443F-A8E3-831469AFB40F"},
            sub_label="cloud",
        )
        assert name == "simbot_version_sync_cloud_dbb7ec81.csv"

    def test_trigger_with_spaces_slugified(self):
        # mcp.name absent, falls back to trigger
        name = DomainSQLTool._build_csv_filename(
            "lookup account",
            {"AccountName": "Lite N Easy"},
        )
        assert name == "simbot_lookup_account_lite-n-easy.csv"

    def test_no_params(self):
        name = DomainSQLTool._build_csv_filename("scheduler", {})
        assert name == "simbot_scheduler.csv"

    def test_multiple_params_joined(self):
        name = DomainSQLTool._build_csv_filename(
            "beacons",
            {"AccountID": "123", "StartDate": "2026-01-01", "EndDate": "2026-01-31"},
        )
        assert name == "simbot_beacons_123_2026-01-01_2026-01-31.csv"

    def test_long_param_truncated_to_24_chars(self):
        long_value = "a" * 50
        name = DomainSQLTool._build_csv_filename("q", {"x": long_value})
        # Param slug capped at 24
        assert "_" + ("a" * 24) + ".csv" in name

    def test_empty_param_value_dropped(self):
        name = DomainSQLTool._build_csv_filename(
            "q",
            {"a": "", "b": "kept"},
        )
        assert name == "simbot_q_kept.csv"

    def test_total_length_capped_at_255(self):
        # Force a contrived multi-param call where naive concat would exceed 255.
        params = {f"p{i}": "x" * 50 for i in range(20)}
        name = DomainSQLTool._build_csv_filename("base", params)
        assert len(name) <= 255
        assert name.startswith("simbot_base_")
        assert name.endswith(".csv")

    def test_unsafe_characters_stripped(self):
        name = DomainSQLTool._build_csv_filename(
            "q",
            {"name": "../etc/passwd"},
        )
        # No path separators or dots in the slug
        assert "/" not in name
        assert ".." not in name
        assert name.endswith(".csv")


class TestFormatCsv:
    def test_round_trip_via_csv_reader(self):
        rows = [
            {"a": 1, "b": "hello"},
            {"a": 2, "b": "wor,ld"},  # comma forces quoting
            {"a": 3, "b": None},  # None becomes empty string
        ]
        text = DomainSQLTool._format_csv_text(rows)
        parsed = list(csv.reader(io.StringIO(text)))
        assert parsed[0] == ["a", "b"]
        assert parsed[1] == ["1", "hello"]
        assert parsed[2] == ["2", "wor,ld"]
        assert parsed[3] == ["3", ""]

    def test_format_csv_bytes_is_utf8(self):
        rows = [{"name": "é"}]
        b = DomainSQLTool._format_csv_bytes(rows)
        assert isinstance(b, bytes)
        assert "é" in b.decode("utf-8")

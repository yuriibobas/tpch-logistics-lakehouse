"""Local orchestration tests; actual Spark/Delta behavior is tested in Databricks.

These in-memory test doubles require neither a JVM nor installed PySpark.
Run: python3 -m unittest discover -s tests -p 'test_bronze_ingest.py'
"""

from __future__ import annotations

import io
import json
import unittest
from collections import Counter
from contextlib import redirect_stdout
from types import SimpleNamespace

from src.bronze.ingest import ingest_raw_tpch, verify_bronze_table


class Type:
    def __init__(self, name: str) -> None:
        self.name = name

    def json(self) -> str:
        return json.dumps(self.name)


class Frame:
    def __init__(self, spark, rows, columns=("r_regionkey", "r_name")) -> None:
        self.spark = spark
        self.rows = list(rows)
        self.schema = SimpleNamespace(
            fields=[
                SimpleNamespace(name=name, dataType=Type("string"), nullable=True)
                for name in columns
            ]
        )

    def count(self):
        return len(self.rows)

    def exceptAll(self, other):
        remainder = Counter(self.rows) - Counter(other.rows)
        return Frame(self.spark, list(remainder.elements()))

    def limit(self, count):
        return Frame(self.spark, self.rows[:count])

    @property
    def write(self):
        return Writer(self)


class Writer:
    def __init__(self, frame) -> None:
        self.frame = frame
        self.write_mode = None
        self.storage_format = None

    def format(self, storage_format):
        self.storage_format = storage_format
        return self

    def mode(self, mode):
        self.write_mode = mode
        return self

    def option(self, key, value):
        return self

    def saveAsTable(self, name):
        spark = self.frame.spark
        if self.write_mode != "overwrite":
            raise AssertionError("Ingestion must overwrite, never append.")
        spark.tables[name] = Frame(
            spark, self.frame.rows, [f.name for f in self.frame.schema.fields]
        )
        spark.formats[name] = self.storage_format
        spark.writes.append(name)


class Spark:
    def __init__(self) -> None:
        self.tables = {}
        self.formats = {}
        self.writes = []
        self.ddl = []
        self.catalog = SimpleNamespace(tableExists=lambda name: name in self.tables)

    def add(self, name, rows, columns=("r_regionkey", "r_name"), fmt="delta"):
        self.tables[name] = Frame(self, rows, columns)
        self.formats[name] = fmt

    def table(self, name):
        return self.tables[name]

    def sql(self, statement):
        if statement.startswith("CREATE SCHEMA"):
            self.ddl.append(statement)
            return None
        name = statement.removeprefix("DESCRIBE DETAIL ")
        return SimpleNamespace(first=lambda: {"format": self.formats[name]})


SOURCE = "`samples`.`tpch`.`region`"
TARGET = "`workspace`.`tpch_bronze`.`region`"


class BronzeIngestionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = Spark()
        self.rows = [(1, "AFRICA"), (1, "AFRICA"), (2, None)]
        self.spark.add(SOURCE, self.rows)

    def run_ingestion(self, **kwargs):
        options = {"catalog": "workspace", "bronze_schema": "tpch_bronze"}
        options.update(kwargs)
        with redirect_stdout(io.StringIO()):
            return ingest_raw_tpch(self.spark, **options)

    def verify(self, **kwargs):
        return verify_bronze_table(
            self.spark,
            "region",
            catalog="workspace",
            bronze_schema="tpch_bronze",
            **kwargs,
        )

    def test_rerun_preserves_duplicates_nulls_and_unselected_tables(self):
        unrelated = "`workspace`.`tpch_bronze`.`custom_table`"
        self.spark.add(unrelated, [(9, "KEEP")])
        self.run_ingestion(tables=("region",), verify_contents=True)
        result = self.run_ingestion(tables=("region",), verify_contents=True)[0]
        self.assertEqual(self.spark.tables[TARGET].rows, self.rows)
        self.assertEqual(result["bronze_rows"], 3)
        self.assertEqual(result["content_check"], "PASS")
        self.assertEqual(self.spark.tables[unrelated].rows, [(9, "KEEP")])

    def test_exact_check_detects_same_count_but_different_values(self):
        self.spark.add(TARGET, [(1, "AFRICA"), (2, None), (3, "WRONG")])
        result = self.verify(verify_contents=True)
        self.assertTrue(result["row_count_match"])
        self.assertEqual(result["content_check"], "FAIL")
        self.assertEqual(result["status"], "FAIL")

    def test_exact_check_detects_duplicate_multiplicity(self):
        self.spark.add(TARGET, [(1, "AFRICA"), (2, None), (2, None)])
        self.assertEqual(self.verify(verify_contents=True)["status"], "FAIL")

    def test_schema_column_order_is_checked(self):
        self.spark.add(TARGET, self.rows, columns=("r_name", "r_regionkey"))
        self.assertFalse(self.verify()["schema_match"])
        self.assertEqual(self.verify(verify_contents=True)["content_check"], "FAIL")

    def test_count_only_verification_reports_content_not_run(self):
        self.run_ingestion(tables=("region",))
        self.assertEqual(self.verify()["content_check"], "NOT_RUN")

    def test_invalid_table_lists_fail_before_any_writes(self):
        for tables in ((), ("region", "region"), ("typo",)):
            with self.subTest(tables=tables), self.assertRaises(ValueError):
                self.run_ingestion(tables=tables)
        self.assertEqual(self.spark.ddl, [])
        self.assertEqual(self.spark.writes, [])

    def test_string_table_list_is_rejected(self):
        with self.assertRaises(TypeError):
            self.run_ingestion(tables="region")
        self.assertEqual(self.spark.ddl, [])
        self.assertEqual(self.spark.writes, [])

    def test_source_destination_overlap_is_rejected(self):
        with self.assertRaises(ValueError):
            self.run_ingestion(catalog="samples", bronze_schema="tpch")
        self.assertEqual(self.spark.ddl, [])

    def test_non_delta_target_is_rejected_before_schema_creation(self):
        self.spark.add(TARGET, self.rows, fmt="parquet")
        with self.assertRaises(ValueError):
            self.run_ingestion(tables=("region",))
        self.assertEqual(self.spark.ddl, [])
        self.assertEqual(self.spark.writes, [])

    def test_all_sources_resolve_before_any_writes(self):
        with self.assertRaises(KeyError):
            self.run_ingestion(tables=("region", "nation"))
        self.assertEqual(self.spark.ddl, [])
        self.assertEqual(self.spark.writes, [])


if __name__ == "__main__":
    unittest.main()

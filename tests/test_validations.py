"""Silver contract checks using the active Databricks Spark session."""

from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DecimalType

from src.silver.quality import (
    ValidationResult,
    load_logistics_rules,
    record_validation_results,
    validate_categorical_values,
    validate_date_sequence,
    validate_foreign_key,
    validate_primary_key,
    validate_referential_integrity,
    validate_required_values,
    validate_silver_tables,
)
from src.silver.transform import (
    TABLE_SCHEMAS,
    TransformationResult,
    publish_silver_tables,
    transform_table,
    validate_lossless_casts,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError("Run checks on Databricks with an active session")
    return session


def test_all_contract_mappings(spark: SparkSession) -> None:
    assert len(TABLE_SCHEMAS) == 8
    assert sum(map(len, TABLE_SCHEMAS.values())) == 61
    for table, mappings in TABLE_SCHEMAS.items():
        values = []
        for _, _, dtype, _ in mappings:
            if dtype.startswith("DECIMAL"):
                values.append(Decimal("12.34"))
            elif dtype == "DATE":
                values.append(date(2026, 1, 1))
            elif dtype in {"BIGINT", "INT"}:
                values.append(1)
            else:
                values.append("value")
        source = spark.createDataFrame(
            [tuple(values)], [mapping[0] for mapping in mappings]
        )
        cleaned = transform_table(source, table)
        assert cleaned.columns == [mapping[1] for mapping in mappings]
        expected = spark.createDataFrame(
            [],
            ", ".join(f"{target} {dtype}" for _, target, dtype, _ in mappings),
        ).schema
        assert [field.dataType for field in cleaned.schema] == [
            field.dataType for field in expected
        ]
        assert cleaned.count() == 1
        assert validate_lossless_casts(source, table).count() == 0


def test_cast_rounding_and_overflow(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [
            (1, 2, 10, Decimal("12.340"), None),
            (1, 3, 10, Decimal("12.345"), None),
            (1, 4, 2**31, Decimal("12.340"), None),
            (1, 5, 10, Decimal("10000000000000000.000"), None),
        ],
        "ps_partkey LONG, ps_suppkey LONG, ps_availqty LONG, "
        "ps_supplycost DECIMAL(22, 3), ps_comment STRING",
    )
    invalid = validate_lossless_casts(source, "partsupp").collect()
    assert {row.ps_suppkey for row in invalid} == {3, 4, 5}
    cleaned = transform_table(source, "partsupp")
    assert cleaned.schema["supply_cost"].dataType == DecimalType(18, 2)
    assert cleaned.filter("supp_key = 5").first().supply_cost is None


def test_missing_source_columns(spark: SparkSession) -> None:
    source = spark.createDataFrame([(1,)], "r_regionkey LONG")
    with pytest.raises(ValueError, match="missing Bronze columns"):
        transform_table(source, "region")


def test_dates_categories_and_empty_orders(spark: SparkSession) -> None:
    orders = spark.createDataFrame(
        [(1, date(2026, 1, 2)), (2, date(2026, 1, 1))],
        "order_key LONG, order_date DATE",
    )
    lines = spark.createDataFrame(
        [
            (1, 1, date(2026, 1, 2), date(2026, 1, 2), "AIR", "N", "O"),
            (1, 2, date(2026, 1, 1), date(2026, 1, 3), "BAD", "N", "O"),
            (1, 3, date(2026, 1, 3), date(2026, 1, 2), "AIR", None, "O"),
            (1, 4, None, date(2026, 1, 3), "AIR", "N", "O"),
            (99, 5, date(2026, 1, 3), date(2026, 1, 4), "AIR", "N", "O"),
        ],
        "order_key LONG, line_number INT, ship_date DATE, receipt_date DATE, "
        "ship_mode STRING, return_flag STRING, line_status STRING",
    )
    assert {
        row.line_number
        for row in validate_date_sequence(lines, orders).collect()
    } == {2, 3, 4, 5}
    assert {
        row.line_number for row in validate_categorical_values(lines).collect()
    } == {2, 3}
    integrity = validate_referential_integrity(lines, orders)
    assert integrity["orphan_lineitems"].first().order_key == 99
    assert integrity["empty_orders"].first().order_key == 2


def test_composite_keys_and_pairs(spark: SparkSession) -> None:
    parent = spark.createDataFrame(
        [(1, 10), (2, 20)], "part_key LONG, supp_key LONG"
    )
    child = spark.createDataFrame(
        [(1, 10, 1, 1), (1, 20, 1, 2), (1, 10, 1, 1)],
        "part_key LONG, supp_key LONG, order_key LONG, line_number INT",
    )
    assert (
        validate_foreign_key(child, parent, ["part_key", "supp_key"]).count()
        == 1
    )
    assert (
        validate_primary_key(child, ["order_key", "line_number"]).count() == 2
    )
    assert validate_primary_key(parent, ["part_key", "supp_key"]).count() == 0


def test_required_and_nullable_fields(spark: SparkSession) -> None:
    region = spark.createDataFrame(
        [(1, "EUROPE", None), (2, None, None)],
        "region_key LONG, name STRING, comment STRING",
    )
    assert validate_required_values(region, "region").first().region_key == 2


def test_rules_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "rules.json"
    path.write_text('{"allowed_ship_modes": []}', encoding="utf-8")
    with pytest.raises(ValueError, match="whitelist"):
        load_logistics_rules(path)


def test_full_validation_preserves_parent_rows(spark: SparkSession) -> None:
    tables = {}
    cast_violations = {}
    for table, mappings in TABLE_SCHEMAS.items():
        values = []
        for _, column, dtype, nullable in mappings:
            if nullable:
                value = None
            elif dtype.startswith("DECIMAL"):
                value = (
                    Decimal("-1.00")
                    if column == "acct_balance"
                    else Decimal("1.00")
                )
            elif dtype == "DATE":
                value = date(2026, 1, 1)
            elif dtype in {"BIGINT", "INT"}:
                value = 1
            else:
                value = {
                    "ship_mode": "AIR",
                    "return_flag": "N",
                    "line_status": "O",
                    "order_status": "O",
                }.get(column, "value")
            values.append(value)
        schema = ", ".join(
            f"{source} {dtype}" for source, _, dtype, _ in mappings
        )
        rows = [tuple(values)]
        if table == "customer":
            rows.append((2, *values[1:]))
        bronze = spark.createDataFrame(rows, schema)
        tables[table] = transform_table(bronze, table)
        cast_violations[table] = validate_lossless_casts(bronze, table)
    validation = validate_silver_tables(
        TransformationResult(tables, cast_violations)
    )
    assert validation.report.count() == 38
    validation.assert_valid()
    assert tables["customer"].count() == 2
    tables["lineitem"] = tables["lineitem"].limit(0)
    rejected = validate_silver_tables(
        TransformationResult(tables, cast_violations)
    )
    with pytest.raises(ValueError, match="nonempty_order"):
        rejected.assert_valid()
    assert (
        rejected.quarantine["orders"].first()._validation_rule
        == "nonempty_order"
    )


def test_publication_gate(spark: SparkSession) -> None:
    writer = MagicMock(spec=SparkSession)
    report = spark.createDataFrame(
        [("lineitem", "primary_key", 1)],
        "table_name STRING, rule_name STRING, invalid_rows LONG",
    )
    validation = ValidationResult(report, {})
    with pytest.raises(ValueError, match="validation failed"):
        publish_silver_tables(writer, TransformationResult({}, {}), validation)
    writer.sql.assert_not_called()
    validation.report = report.withColumn(
        "invalid_rows", report.invalid_rows * 0
    )
    with pytest.raises(ValueError, match="Record validation"):
        publish_silver_tables(writer, TransformationResult({}, {}), validation)
    writer.sql.assert_not_called()


def test_saved_audit_report_is_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    expression = MagicMock()
    expression.__gt__.return_value = True
    expression.__eq__.return_value = True
    monkeypatch.setattr("src.silver.quality.F.lit", lambda value: expression)
    monkeypatch.setattr("src.silver.quality.F.col", lambda name: expression)
    writer = MagicMock(spec=SparkSession)
    original = MagicMock(spec=DataFrame)
    saved = MagicMock(spec=DataFrame)
    saved.filter.return_value.collect.return_value = []
    writer.table.return_value.filter.return_value.select.return_value = saved
    validation = ValidationResult(original, {})

    record_validation_results(
        writer, validation, "run-1", "workspace.tpch_silver"
    )

    assert validation.report is saved
    assert validation.recorded_run_id == "run-1"
    original.filter.assert_not_called()
    saved.filter.assert_called_once()
    writer.table.assert_called_once_with(
        "`workspace`.`tpch_silver`.`quality_validation_results`"
    )

"""Explicit Silver checks. Each rule produces a DataFrame of invalid rows which are
 later saved in quarantine and used to block publication if necessary."""

import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from config import env
from src.silver.transform import (
    PRIMARY_KEYS,
    TABLE_SCHEMAS,
    TransformationResult,
    qualified_name,
)

LOGGER = logging.getLogger(__name__)

FOREIGN_KEYS: tuple[tuple[str, tuple[str], str]] = (
    ("nation", ("region_key",), "region"),
    ("supplier", ("nation_key",), "nation"),
    ("customer", ("nation_key",), "nation"),
    ("partsupp", ("part_key",), "part"),
    ("partsupp", ("supp_key",), "supplier"),
    ("orders", ("cust_key",), "customer"),
    ("lineitem", ("order_key",), "orders"),
    ("lineitem", ("part_key", "supp_key"), "partsupp"),
    ("lineitem", ("part_key",), "part"),
    ("lineitem", ("supp_key",), "supplier"),
)


@dataclass
class ValidationResult:
    """Check counts and diagnostic rows, potentially once per failed rule."""

    report: DataFrame
    quarantine: dict[str, DataFrame]
    recorded_run_id: str | None = None

    def assert_valid(self) -> None:
        """Block publication if any check failed; never silently drop rows."""
        failures = self.report.filter(F.col("invalid_rows") > 0).collect()
        if failures:
            details = "; ".join(
                f"{row.table_name}.{row.rule_name}: {row.invalid_rows}"
                for row in failures
            )
            raise ValueError(
                f"Silver validation failed; see quarantine. {details}"
            )


def load_logistics_rules(
    path: str | Path | None = None,
) -> dict[str, list[str]]:
    """Load explicit, nonempty whitelists; malformed config fails closed."""
    rules_path = (
        Path(path)
        if path is not None
        else Path(__file__).resolve().parents[2]
        / "config"
        / "logistics_rules.json"
    )
    with rules_path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    rules: dict[str, list[str]] = {}
    for column, key in (
        ("ship_mode", "allowed_ship_modes"),
        ("return_flag", "allowed_return_flags"),
        ("line_status", "allowed_line_statuses"),
    ):
        values = config.get(key)
        if (
            not isinstance(values, list)
            or not values
            or not all(isinstance(value, str) and value for value in values)
            or len(set(values)) != len(values)
        ):
            raise ValueError(f"Invalid categorical whitelist: {key}")
        rules[column] = values
    return rules


def _invalid_rows(dataframe: DataFrame, errors: Sequence[Column]) -> DataFrame:
    return dataframe.withColumn(
        "_quality_errors",
        F.filter(F.array(*errors), lambda error: error.isNotNull()),
    ).filter(F.size("_quality_errors") > 0)


def validate_required_values(dataframe: DataFrame, table: str) -> DataFrame:
    """Return rows with null required attributes, including failed casts."""
    return _invalid_rows(
        dataframe,
        [
            F.when(F.col(column).isNull(), F.lit(f"required:{column}"))
            for _, column, _, nullable in TABLE_SCHEMAS[table]
            if not nullable
        ],
    )


def validate_primary_key(
    dataframe: DataFrame, keys: Sequence[str]
) -> DataFrame:
    """Return all duplicate-key rows, including composite-key duplicates."""
    return (
        dataframe.withColumn(
            "_key_count", F.count(F.lit(1)).over(Window.partitionBy(*keys))
        )
        .filter(F.col("_key_count") > 1)
        .drop("_key_count")
    )


def validate_foreign_key(
    child_df: DataFrame,
    parent_df: DataFrame,
    keys: Sequence[str],
) -> DataFrame:
    """Anti-join on the entire key; parent and child key names must match."""
    return child_df.join(parent_df.select(*keys), list(keys), "left_anti")


def validate_date_sequence(
    lineitem_df: DataFrame,
    orders_df: DataFrame,
) -> DataFrame:
    """Return lines violating order_date <= ship_date <= receipt_date.

    Missing dates and missing orders fail, rather than disappearing through
    SQL's three-valued null comparisons. Commit dates are not sequence rules.
    """
    joined = lineitem_df.alias("line").join(
        orders_df.select("order_key", "order_date").alias("orders"),
        "order_key",
        "left",
    )
    invalid = _invalid_rows(
        joined,
        [
            F.when(F.col("order_date").isNull(), F.lit("missing_order_date")),
            F.when(F.col("ship_date").isNull(), F.lit("missing_ship_date")),
            F.when(
                F.col("receipt_date").isNull(), F.lit("missing_receipt_date")
            ),
            F.when(
                F.col("order_date") > F.col("ship_date"),
                F.lit("ship_before_order"),
            ),
            F.when(
                F.col("ship_date") > F.col("receipt_date"),
                F.lit("receipt_before_ship"),
            ),
        ],
    )
    return invalid.select("line.*", "_quality_errors")


def validate_allowed_values(
    dataframe: DataFrame,
    allowed_values: Mapping[str, Sequence[str]],
) -> DataFrame:
    """Match whitelist values exactly, treating nulls as invalid categories."""
    return _invalid_rows(
        dataframe,
        [
            F.when(
                F.col(column).isNull() | ~F.col(column).isin(*values),
                F.lit(f"category:{column}"),
            )
            for column, values in allowed_values.items()
        ],
    )


def validate_categorical_values(
    lineitem_df: DataFrame,
    rules: Mapping[str, Sequence[str]] | None = None,
) -> DataFrame:
    """Validate ship_mode, return_flag, and line_status from the JSON rules."""
    allowed = load_logistics_rules() if rules is None else rules
    expected = {"ship_mode", "return_flag", "line_status"}
    if set(allowed) != expected or any(
        not values for values in allowed.values()
    ):
        raise ValueError(
            "Rules must provide all three nonempty lineitem whitelists"
        )
    return validate_allowed_values(lineitem_df, allowed)


def validate_referential_integrity(
    lineitem_df: DataFrame,
    orders_df: DataFrame,
) -> dict[str, DataFrame]:
    """Separate orphan lines and empty orders without deleting either."""
    return {
        "orphan_lineitems": validate_foreign_key(
            lineitem_df, orders_df, ["order_key"]
        ),
        "empty_orders": orders_df.join(
            lineitem_df.select("order_key"), "order_key", "left_anti"
        ),
    }


def validate_silver_tables(
    transformed: TransformationResult,
    rules: Mapping[str, Sequence[str]] | None = None,
) -> ValidationResult:
    """Build all check counts and quarantine frames before any Silver writes.

    Cast quarantine retains Bronze values; other quarantine frames retain
    Silver names. Repeated rows represent distinct failed rules, not entities.
    Record the report before gate checks so they can reuse its Delta rows.
    """
    tables = transformed.tables
    if set(tables) != set(TABLE_SCHEMAS) or set(
        transformed.cast_violations
    ) != set(TABLE_SCHEMAS):
        raise ValueError(
            "Validation requires all eight tables and cast diagnostics"
        )
    checks: list[tuple[str, str, DataFrame]] = []
    for table, dataframe in tables.items():
        checks.extend(
            [
                (
                    table,
                    "required_values",
                    validate_required_values(dataframe, table),
                ),
                (
                    table,
                    "primary_key",
                    validate_primary_key(dataframe, PRIMARY_KEYS[table]),
                ),
                (table, "lossless_casts", transformed.cast_violations[table]),
            ]
        )
    for child, keys, parent in FOREIGN_KEYS:
        checks.append(
            (
                child,
                f"fk_{parent}_{'_'.join(keys)}",
                validate_foreign_key(tables[child], tables[parent], keys),
            )
        )
    checks.extend(
        [
            (
                "lineitem",
                "date_sequence",
                validate_date_sequence(tables["lineitem"], tables["orders"]),
            ),
            (
                "lineitem",
                "categories",
                validate_categorical_values(tables["lineitem"], rules),
            ),
            (
                "orders",
                "order_status",
                validate_allowed_values(
                    tables["orders"], {"order_status": ["O", "F", "P"]}
                ),
            ),
            (
                "orders",
                "nonempty_order",
                validate_referential_integrity(
                    tables["lineitem"], tables["orders"]
                )["empty_orders"],
            ),
        ]
    )
    reports: list[DataFrame] = []
    quarantine: dict[str, DataFrame] = {}
    for table, rule, invalid in checks:
        reports.append(
            invalid.agg(F.count(F.lit(1)).alias("invalid_rows"))
            .withColumn("table_name", F.lit(table))
            .withColumn("rule_name", F.lit(rule))
            .select("table_name", "rule_name", "invalid_rows")
        )
        diagnostic = invalid.withColumn("_validation_rule", F.lit(rule))
        if "_quality_errors" not in diagnostic.columns:
            diagnostic = diagnostic.withColumn(
                "_quality_errors", F.array(F.lit(rule))
            )
        name = f"{table}_casts" if rule == "lossless_casts" else table
        if name in quarantine:
            quarantine[name] = quarantine[name].unionByName(diagnostic)
        else:
            quarantine[name] = diagnostic
    report = reduce(lambda left, right: left.unionByName(right), reports)
    report = report.withColumn(
        "status",
        F.when(F.col("invalid_rows") > 0, "FAILED").otherwise("PASSED"),
    )
    return ValidationResult(report, quarantine)


def record_validation_results(
    spark: SparkSession,
    validation: ValidationResult,
    run_id: str,
    silver_namespace: str | None = None,
) -> None:
    """Append audit results and failed-rule rows to Silver-side Delta tables.

    Call this before ``assert_valid`` so rejected runs leave durable evidence.
    Failure to write diagnostics also blocks the notebook from publishing.
    Later checks read this run's saved report without serverless cache APIs.
    """
    namespace = silver_namespace or f"{env.CATALOG}.{env.SILVER_SCHEMA}"
    timestamp = datetime.now(timezone.utc)
    schema_name = qualified_name(namespace, "unused").rsplit(".", 1)[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    (
        validation.report.withColumn("run_id", F.lit(run_id))
        .withColumn("validated_at", F.lit(timestamp))
        .write.format("delta")
        .mode("append")
        .saveAsTable(qualified_name(namespace, "quality_validation_results"))
    )
    validation.report = (
        spark.table(qualified_name(namespace, "quality_validation_results"))
        .filter(F.col("run_id") == run_id)
        .select("table_name", "rule_name", "invalid_rows", "status")
    )
    failures = validation.report.filter(F.col("invalid_rows") > 0).collect()
    failed_tables = {
        row.table_name for row in failures if row.rule_name != "lossless_casts"
    }
    failed_casts = {
        f"{row.table_name}_casts"
        for row in failures
        if row.rule_name == "lossless_casts"
    }
    for name in failed_tables | failed_casts:
        (
            validation.quarantine[name]
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_validated_at", F.lit(timestamp))
            .write.format("delta")
            .mode("append")
            .saveAsTable(qualified_name(namespace, f"quarantine_{name}"))
        )
    for row in failures:
        LOGGER.error(
            "Silver run %s: %s.%s has %s invalid rows",
            run_id,
            row.table_name,
            row.rule_name,
            row.invalid_rows,
        )
    LOGGER.info("Silver validation recorded for run %s", run_id)
    validation.recorded_run_id = run_id

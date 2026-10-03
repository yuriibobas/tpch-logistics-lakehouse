"""Prepare the eight contract-defined Silver entities without publishing."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from config import env

if TYPE_CHECKING:
    from src.silver.quality import ValidationResult

ColumnMapping = tuple[str, str, str, bool]

TABLE_SCHEMAS: dict[str, tuple[ColumnMapping]] = {
    "region": (
        ("r_regionkey", "region_key", "BIGINT", False),
        ("r_name", "name", "STRING", False),
        ("r_comment", "comment", "STRING", True),
    ),
    "nation": (
        ("n_nationkey", "nation_key", "BIGINT", False),
        ("n_name", "name", "STRING", False),
        ("n_regionkey", "region_key", "BIGINT", False),
        ("n_comment", "comment", "STRING", True),
    ),
    "supplier": (
        ("s_suppkey", "supp_key", "BIGINT", False),
        ("s_name", "name", "STRING", False),
        ("s_address", "address", "STRING", False),
        ("s_nationkey", "nation_key", "BIGINT", False),
        ("s_phone", "phone", "STRING", False),
        ("s_acctbal", "acct_balance", "DECIMAL(18, 2)", False),
        ("s_comment", "comment", "STRING", True),
    ),
    "customer": (
        ("c_custkey", "cust_key", "BIGINT", False),
        ("c_name", "name", "STRING", False),
        ("c_address", "address", "STRING", False),
        ("c_nationkey", "nation_key", "BIGINT", False),
        ("c_phone", "phone", "STRING", False),
        ("c_acctbal", "acct_balance", "DECIMAL(18, 2)", False),
        ("c_mktsegment", "market_segment", "STRING", False),
        ("c_comment", "comment", "STRING", True),
    ),
    "part": (
        ("p_partkey", "part_key", "BIGINT", False),
        ("p_name", "name", "STRING", False),
        ("p_mfgr", "manufacturer", "STRING", False),
        ("p_brand", "brand", "STRING", False),
        ("p_type", "type", "STRING", False),
        ("p_size", "size", "INT", False),
        ("p_container", "container", "STRING", False),
        ("p_retailprice", "retail_price", "DECIMAL(18, 2)", False),
        ("p_comment", "comment", "STRING", True),
    ),
    "partsupp": (
        ("ps_partkey", "part_key", "BIGINT", False),
        ("ps_suppkey", "supp_key", "BIGINT", False),
        ("ps_availqty", "avail_quantity", "INT", False),
        ("ps_supplycost", "supply_cost", "DECIMAL(18, 2)", False),
        ("ps_comment", "comment", "STRING", True),
    ),
    "orders": (
        ("o_orderkey", "order_key", "BIGINT", False),
        ("o_custkey", "cust_key", "BIGINT", False),
        ("o_orderstatus", "order_status", "STRING", False),
        ("o_totalprice", "total_price", "DECIMAL(18, 2)", False),
        ("o_orderdate", "order_date", "DATE", False),
        ("o_orderpriority", "order_priority", "STRING", False),
        ("o_clerk", "clerk", "STRING", False),
        ("o_shippriority", "ship_priority", "INT", False),
        ("o_comment", "comment", "STRING", True),
    ),
    "lineitem": (
        ("l_orderkey", "order_key", "BIGINT", False),
        ("l_partkey", "part_key", "BIGINT", False),
        ("l_suppkey", "supp_key", "BIGINT", False),
        ("l_linenumber", "line_number", "INT", False),
        ("l_quantity", "quantity", "DECIMAL(18, 2)", False),
        ("l_extendedprice", "extended_price", "DECIMAL(18, 2)", False),
        ("l_discount", "discount", "DECIMAL(18, 2)", False),
        ("l_tax", "tax", "DECIMAL(18, 2)", False),
        ("l_returnflag", "return_flag", "STRING", False),
        ("l_linestatus", "line_status", "STRING", False),
        ("l_shipdate", "ship_date", "DATE", False),
        ("l_commitdate", "commit_date", "DATE", False),
        ("l_receiptdate", "receipt_date", "DATE", False),
        ("l_shipinstruct", "ship_instruct", "STRING", True),
        ("l_shipmode", "ship_mode", "STRING", False),
        ("l_comment", "comment", "STRING", True),
    ),
}

PRIMARY_KEYS: dict[str, tuple[str]] = {
    "region": ("region_key",),
    "nation": ("nation_key",),
    "supplier": ("supp_key",),
    "customer": ("cust_key",),
    "part": ("part_key",),
    "partsupp": ("part_key", "supp_key"),
    "orders": ("order_key",),
    "lineitem": ("order_key", "line_number"),
}


@dataclass
class TransformationResult:
    """Typed entities and raw rows whose casts fail or change their value."""

    tables: dict[str, DataFrame]
    cast_violations: dict[str, DataFrame]


def qualified_name(namespace: str, table: str) -> str:
    """Quote catalog/schema/table identifiers for SQL and Spark table reads."""
    return ".".join(
        "`" + component.replace("`", "``") + "`"
        for component in [*namespace.split("."), table]
    )


def transform_table(bronze_df: DataFrame, table: str) -> DataFrame:
    """Apply exact mappings, retaining parent rows and DECIMAL(18, 2) values.

    Invalid casts become null even with ANSI mode enabled. Always validate
    lossless casts and required values before publishing this frame.
    """
    mappings = TABLE_SCHEMAS[table]
    missing = {source for source, _, _, _ in mappings} - set(bronze_df.columns)
    if missing:
        raise ValueError(f"{table}: missing Bronze columns: {sorted(missing)}")
    return bronze_df.select(
        *[
            F.col(source).try_cast(dtype).alias(target)
            for source, target, dtype, _ in mappings
        ]
    )


def validate_lossless_casts(bronze_df: DataFrame, table: str) -> DataFrame:
    """Return raw rows with per-column errors for failed or lossy conversions.

    Round-trip casts compare with the original typed Bronze value, detecting
    decimal rounding, counter truncation, date time loss, and numeric overflow.
    Null inputs are handled separately by required-value validation.
    """
    errors: list[Column] = []
    for source, target, dtype, _ in TABLE_SCHEMAS[table]:
        original = F.col(source)
        converted = original.try_cast(dtype)
        restored = converted.try_cast(bronze_df.schema[source].dataType)
        invalid = original.isNotNull() & (
            converted.isNull() | ~original.eqNullSafe(restored)
        )
        errors.append(F.when(invalid, F.lit(f"lossy_cast:{target}")))
    return bronze_df.withColumn(
        "_quality_errors",
        F.filter(F.array(*errors), lambda error: error.isNotNull()),
    ).filter(F.size("_quality_errors") > 0)


def clean_lineitem(spark: SparkSession) -> DataFrame:
    """Read configured Bronze lineitem and apply the exact Silver schema."""
    namespace = f"{env.CATALOG}.{env.BRONZE_SCHEMA}"
    return transform_table(
        spark.table(qualified_name(namespace, "lineitem")), "lineitem"
    )


def clean_orders(spark: SparkSession) -> DataFrame:
    """Read configured Bronze orders and apply the exact Silver schema."""
    namespace = f"{env.CATALOG}.{env.BRONZE_SCHEMA}"
    return transform_table(
        spark.table(qualified_name(namespace, "orders")), "orders"
    )


def run_silver_transformations(
    spark: SparkSession,
    bronze_namespace: str | None = None,
) -> TransformationResult:
    """Read all eight Delta entities from a verified, idle Bronze refresh.

    Requires Databricks compute providing PySpark 4 Column.try_cast.
    This function deliberately does not write Silver. Separate Delta tables
    are not an atomic snapshot; the caller must prevent concurrent refreshes.
    """
    namespace = bronze_namespace or f"{env.CATALOG}.{env.BRONZE_SCHEMA}"
    tables: dict[str, DataFrame] = {}
    cast_violations: dict[str, DataFrame] = {}
    for table in TABLE_SCHEMAS:
        source_name = qualified_name(namespace, table)
        detail = spark.sql(f"DESCRIBE DETAIL {source_name}").first()
        if detail is None or detail.format != "delta":
            raise ValueError(
                f"{source_name}: Bronze source must be a Delta table"
            )
        bronze_df = spark.table(source_name)
        tables[table] = transform_table(bronze_df, table)
        cast_violations[table] = validate_lossless_casts(bronze_df, table)
    return TransformationResult(tables, cast_violations)


def publish_silver_tables(
    spark: SparkSession,
    transformed: TransformationResult,
    validation: "ValidationResult",
    silver_namespace: str | None = None,
) -> None:
    """Publish only after successful validation and durable audit recording.

    Create Delta tables with actual NOT NULL constraints, not informational
    keys. Reject existing schema drift before replacing any entity's data.
    The eight overwrites are separate transactions: keep downstream readers
    and other Silver writers paused until the entire notebook succeeds.
    """
    validation.assert_valid()
    if validation.recorded_run_id is None:
        raise ValueError("Record validation results before publishing Silver")
    if set(transformed.tables) != set(TABLE_SCHEMAS):
        raise ValueError("Publishing requires all eight Silver tables")
    namespace = silver_namespace or f"{env.CATALOG}.{env.SILVER_SCHEMA}"
    schema_name = qualified_name(namespace, "unused").rsplit(".", 1)[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    for table, mappings in TABLE_SCHEMAS.items():
        target_name = qualified_name(namespace, table)
        fields = ", ".join(
            f"`{column}` {dtype}" + ("" if nullable else " NOT NULL")
            for _, column, dtype, nullable in mappings
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {target_name} ({fields}) USING DELTA"
        )
        actual = spark.table(target_name).schema
        expected_types = spark.createDataFrame(
            [],
            ", ".join(
                f"`{column}` {dtype}" for _, column, dtype, _ in mappings
            ),
        ).schema
        expected = [
            (column, expected_types[column].dataType, nullable)
            for _, column, _, nullable in mappings
        ]
        observed = [
            (field.name, field.dataType, field.nullable) for field in actual
        ]
        source = transformed.tables[table].schema
        if observed != expected or [
            (field.name, field.dataType) for field in source
        ] != [(column, dtype) for column, dtype, _ in expected]:
            raise ValueError(
                f"{target_name}: schema differs from the Silver contract"
            )
    for table in TABLE_SCHEMAS:
        transformed.tables[table].write.mode("overwrite").insertInto(
            qualified_name(namespace, table)
        )

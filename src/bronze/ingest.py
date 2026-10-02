"""Copy the eight TPC-H source tables into managed Bronze Delta tables."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, TypedDict

from config import env

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

TPCH_TABLES = (
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
)


class IngestionResult(TypedDict):
    """One table's verification results, suitable for notebook display."""

    source_table: str
    target_table: str
    source_rows: int
    bronze_rows: int
    row_count_match: bool
    schema_match: bool
    format: str
    content_check: str
    status: str


def _identifier(name: str) -> str:
    """Quote one SQL identifier rather than interpolating unescaped SQL."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Catalog, schema, and table names must be non-empty.")
    return "`" + name.replace("`", "``") + "`"


def _table_name(catalog: str, schema: str, table: str) -> str:
    return ".".join(_identifier(part) for part in (catalog, schema, table))


def _schema_signature(frame: DataFrame) -> list[tuple[str, str]]:
    """Compare column order, names, and types; Delta may relax nullability."""
    return [(field.name, field.dataType.json()) for field in frame.schema.fields]


def verify_bronze_table(
    spark: SparkSession,
    table: str,
    *,
    catalog: str | None = None,
    bronze_schema: str | None = None,
    verify_contents: bool = False,
) -> IngestionResult:
    """Read-only verification; optional multiset comparison retains duplicates.

    Counts and matching schemas alone do not prove equality of every value.
    Set verify_contents=True for exact, two-way EXCEPT ALL checks. These can
    be expensive on the larger tables. No Spark Classic-only APIs are used.
    """
    if table not in TPCH_TABLES:
        raise ValueError(f"Not a TPC-H table: {table!r}")
    catalog = env.CATALOG if catalog is None else catalog
    bronze_schema = env.BRONZE_SCHEMA if bronze_schema is None else bronze_schema
    source_name = _table_name(env.SOURCE_CATALOG, env.SOURCE_SCHEMA, table)
    target_name = _table_name(catalog, bronze_schema, table)
    source = spark.table(source_name)
    bronze = spark.table(target_name)
    source_rows = source.count()
    bronze_rows = bronze.count()
    schema_match = _schema_signature(source) == _schema_signature(bronze)
    storage_format = str(
        spark.sql(f"DESCRIBE DETAIL {target_name}").first()["format"]
    ).lower()
    content_check = "NOT_RUN"
    if verify_contents:
        if not schema_match:
            content_check = "FAIL"
        else:
            # EXCEPT ALL compares by position and preserves duplicate multiplicity.
            source_only = source.exceptAll(bronze).limit(1).count()
            bronze_only = bronze.exceptAll(source).limit(1).count()
            content_check = "PASS" if source_only == bronze_only == 0 else "FAIL"
    passed = (
        source_rows == bronze_rows
        and schema_match
        and storage_format == "delta"
        and content_check != "FAIL"
    )
    return IngestionResult(
        source_table=f"{env.SOURCE_CATALOG}.{env.SOURCE_SCHEMA}.{table}",
        target_table=f"{catalog}.{bronze_schema}.{table}",
        source_rows=source_rows,
        bronze_rows=bronze_rows,
        row_count_match=source_rows == bronze_rows,
        schema_match=schema_match,
        format=storage_format,
        content_check=content_check,
        status="PASS" if passed else "FAIL",
    )


def ingest_raw_tpch(
    spark: SparkSession,
    *,
    catalog: str | None = None,
    bronze_schema: str | None = None,
    tables: Sequence[str] = TPCH_TABLES,
    verify_contents: bool = False,
) -> list[IngestionResult]:
    """Overwrite managed Delta tables with unchanged samples.tpch DataFrames.

    Each table is replaced atomically by Delta; the eight writes are not a
    single transaction. A failed run can leave a partially populated Bronze
    layer. Rerun after correcting the error. Do not run concurrent ingestion
    or Silver reads during a refresh. Only selected TPC-H tables are touched.

    No casts, renames, filters, joins, deduplication, or metadata columns are
    applied. Existing target schemas are replaced with the source schemas.
    """
    catalog = env.CATALOG if catalog is None else catalog
    bronze_schema = env.BRONZE_SCHEMA if bronze_schema is None else bronze_schema
    # Validate all arguments before any write.
    target_schema = f"{_identifier(catalog)}.{_identifier(bronze_schema)}"
    if isinstance(tables, str):
        raise TypeError("tables must be a sequence of table names, not a string.")
    selected = tuple(tables)
    if not selected or len(selected) != len(set(selected)):
        raise ValueError("Choose at least one table, with no repeated names.")
    unknown = set(selected).difference(TPCH_TABLES)
    if unknown:
        raise ValueError(f"Not TPC-H tables: {sorted(unknown)}")
    if (catalog.casefold(), bronze_schema.casefold()) == (
        env.SOURCE_CATALOG.casefold(),
        env.SOURCE_SCHEMA.casefold(),
    ):
        raise ValueError("The Bronze destination cannot be the source schema.")

    source_frames: dict[str, DataFrame] = {}
    for table in selected:
        source = spark.table(_table_name(env.SOURCE_CATALOG, env.SOURCE_SCHEMA, table))
        _schema_signature(source)  # Resolve every source before the first write.
        source_frames[table] = source
        target_name = _table_name(catalog, bronze_schema, table)
        if spark.catalog.tableExists(target_name):
            storage_format = spark.sql(f"DESCRIBE DETAIL {target_name}").first()[
                "format"
            ]
            if str(storage_format).lower() != "delta":
                raise ValueError(f"Refusing to overwrite non-Delta {target_name}.")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    results: list[IngestionResult] = []
    for table in selected:
        target_name = _table_name(catalog, bronze_schema, table)
        print(f"Copying {table} -> {target_name}", flush=True)
        (
            source_frames[table]
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_name)
        )
        result = verify_bronze_table(
            spark,
            table,
            catalog=catalog,
            bronze_schema=bronze_schema,
            verify_contents=verify_contents,
        )
        print(json.dumps(result, sort_keys=True), flush=True)
        results.append(result)
        if result["status"] != "PASS":
            raise RuntimeError(f"Bronze verification failed: {json.dumps(result)}")
    return results

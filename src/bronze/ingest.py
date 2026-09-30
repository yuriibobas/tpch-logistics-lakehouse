from pyspark.sql import SparkSession

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders", 
    "part", "partsupp", "region", "supplier"
]

def ingest_raw_tpch(spark: SparkSession) -> None:
    """Copies raw tables from samples.tpch into Bronze-layer as-is."""

from pyspark.sql import DataFrame, SparkSession


def clean_lineitem(spark: SparkSession) -> DataFrame:
    """
    Read bronze.lineitem, normalize column names to snake_case,
    convert dates to DATE type, and monetary values to DECIMAL(15, 2).
    """
    raise NotImplementedError("Implement lineitem cleaning")


def clean_orders(spark: SparkSession) -> DataFrame:
    """
    Read bronze.orders and normalize data types and column names.
    """
    raise NotImplementedError("Implement orders cleaning")


def run_silver_transformations(spark: SparkSession) -> None:
    """Orchestrate the writing of cleaned tables to the Silver layer."""

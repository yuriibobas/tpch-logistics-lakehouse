"""Monitoring and alerting logic for Logistics SLA metrics."""

import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import env

LOGGER = logging.getLogger(__name__)

def compute_monthly_delay_rate(lineitem_df: DataFrame) -> DataFrame:
    """
    Calculates the number of items, received after commit_date, 
    monthly (l_receiptdate > l_commitdate).
    """
    return (
        lineitem_df.withColumn(
            "receipt_month", F.date_trunc("month", F.col("receipt_date"))
        )
        .withColumn(
            "is_delayed",
            F.when(F.col("receipt_date") > F.col("commit_date"), 1).otherwise(0),
        )
        .groupBy("receipt_month")
        .agg(
            F.count("*").alias("total_receipts"),
            F.sum("is_delayed").alias("delayed_items"),
        )
        .withColumn(
            "delay_rate_pct",
            F.round((F.col("delayed_items") / F.col("total_receipts")) * 100, 2),
        )
        .orderBy("receipt_month")
    )

def check_delay_alert_threshold(
    delay_metrics_df: DataFrame, threshold_pct: float = 15.0
) -> DataFrame:
    """
    Returns the periods, where delay rate reaches threshold (alert rule).
    """
    return delay_metrics_df.filter(F.col("delay_rate_pct") > threshold_pct)


def run_delay_monitoring(
    spark: SparkSession,
    threshold_pct: float = 15.0,
    silver_namespace: str | None = None,
) -> dict[str, DataFrame]:
    namespace = silver_namespace or f"{env.CATALOG}.{env.SILVER_SCHEMA}"
    lineitem_df = spark.table(f"{namespace}.lineitem")

    metrics_df = compute_monthly_delay_rate(lineitem_df)
    alerts_df = check_delay_alert_threshold(metrics_df, threshold_pct)

    LOGGER.info("Monitoring pipeline executed successfully.")
    
    return {
        "metrics": metrics_df,
        "alerts": alerts_df,
    }
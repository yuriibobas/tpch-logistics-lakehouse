"""Monitoring and alerting logic for Logistics SLA metrics."""

import logging
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from config import env

LOGGER = logging.getLogger(__name__)

def compute_monthly_delay_rate(lineitem_df: DataFrame) -> DataFrame:
    monthly_base = (
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
    )

    window_spec = Window.orderBy("receipt_month")
    return (
        monthly_base.withColumn(
            "prev_month_rate", F.lag("delay_rate_pct", 1).over(window_spec)
        )
        .withColumn(
            "rate_change_pct_points",
            F.round(F.col("delay_rate_pct") - F.col("prev_month_rate"), 2),
        )
        .orderBy("receipt_month")
    )

def check_delay_alert_threshold(
    delay_metrics_df: DataFrame,
    absolute_threshold_pct: float = 65.0,
    mom_spike_threshold: float = 5.0,
) -> DataFrame:
    return delay_metrics_df.filter(
        (F.col("delay_rate_pct") > absolute_threshold_pct)
        | (F.col("rate_change_pct_points") > mom_spike_threshold)
    )


def run_delay_monitoring(
    spark: SparkSession,
    absolute_threshold_pct: float = 65.0,
    mom_spike_threshold: float = 5.0,
    silver_namespace: str | None = None,
) -> dict[str, DataFrame]:
    namespace = silver_namespace or f"{env.CATALOG}.{env.SILVER_SCHEMA}"
    lineitem_df = spark.table(f"{namespace}.lineitem")

    metrics_df = compute_monthly_delay_rate(lineitem_df)
    alerts_df = check_delay_alert_threshold(
        metrics_df, absolute_threshold_pct, mom_spike_threshold
    )

    LOGGER.info("Monitoring pipeline executed successfully.")
    return {
        "metrics": metrics_df,
        "alerts": alerts_df,
    }
"""Gold layer marts answering Logistics business questions Q1-Q4.

Run via notebooks/03_gold_layer.ipynb after Silver tables are published.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import env

def build_transit_time_mart(lineitem_df: DataFrame) -> DataFrame:
    """
    Q1: Calculate the median and p90 transit time
    (ship_date to receipt_date) for each ship_mode.
    """
    return (
        lineitem_df.withColumn(
            "transit_days",
            F.datediff(F.col("receipt_date"), F.col("ship_date")),
        )
        .groupBy("ship_mode")
        .agg(
            F.percentile_approx("transit_days", 0.5).alias("median_transit_days"),
            F.percentile_approx("transit_days", 0.9).alias("p90_transit_days"),
            F.count("*").alias("total_shipments"),
        )
    )


def build_order_fulfillment_sla_mart(
    orders_df: DataFrame, lineitem_df: DataFrame
) -> DataFrame:
    """
    Q2: Calculate the share of orders where ALL line items
    arrived on or before commit_date, and compare it with
    the share of individual line items delivered on time.
    """
    line_status = lineitem_df.withColumn(
        "is_on_time",
        F.when(F.col("receipt_date") <= F.col("commit_date"), 1).otherwise(0),
    )

    order_status = line_status.groupBy("order_key").agg(
        F.count("*").alias("total_lines"),
        F.sum("is_on_time").alias("on_time_lines"),
    )

    order_status = order_status.withColumn(
        "order_fully_on_time",
        F.when(F.col("total_lines") == F.col("on_time_lines"), 1).otherwise(0),
    )

    return (
        order_status.join(orders_df.select("order_key", "order_date"), "order_key")
        .withColumn("order_month", F.date_trunc("month", F.col("order_date")))
        .groupBy("order_month")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("order_fully_on_time").alias("fully_on_time_orders"),
            F.sum("total_lines").alias("total_line_items"),
            F.sum("on_time_lines").alias("on_time_line_items"),
        )
        .withColumn(
            "order_fully_on_time_rate",
            F.round((F.col("fully_on_time_orders") / F.col("total_orders")) * 100, 2),
        )
        .withColumn(
            "lineitem_on_time_rate",
            F.round((F.col("on_time_line_items") / F.col("total_line_items")) * 100, 2),
        )
    )


def build_ship_mode_delay_mart(lineitem_df: DataFrame) -> DataFrame:
    """
    Q3: Calculate the share of delayed deliveries
    (receipt_date > commit_date) for each ship_mode.
    """
    return (
        lineitem_df.withColumn(
            "is_delayed",
            F.when(F.col("receipt_date") > F.col("commit_date"), 1).otherwise(0),
        )
        .groupBy("ship_mode")
        .agg(
            F.count("*").alias("total_items_shipped"),
            F.sum("is_delayed").alias("delayed_items_count"),
        )
        .withColumn(
            "delay_rate_pct",
            F.round(
                (F.col("delayed_items_count") / F.col("total_items_shipped")) * 100, 2
            ),
        )
        .orderBy(F.desc("delay_rate_pct"))
    )


def build_priority_fulfillment_mart(
    orders_df: DataFrame, lineitem_df: DataFrame
) -> DataFrame:
    """
    Q4: Analyze fulfillment speed for urgent orders (order_priority)
    compared with regular orders.
    """
    final_receipt = lineitem_df.groupBy("order_key").agg(
        F.max("receipt_date").alias("final_receipt_date")
    )

    fulfillment = final_receipt.join(
        orders_df.select("order_key", "order_date", "order_priority"), "order_key"
    ).withColumn(
        "fulfillment_days",
        F.datediff(F.col("final_receipt_date"), F.col("order_date")),
    )

    return (
        fulfillment.groupBy("order_priority")
        .agg(
            F.round(F.avg("fulfillment_days"), 2).alias("avg_fulfillment_days"),
            F.percentile_approx("fulfillment_days", 0.5).alias("median_fulfillment_days"),
            F.count("*").alias("total_orders"),
        )
        .orderBy("order_priority")
    )


def run_gold_marts(
    spark: SparkSession, silver_namespace: str | None = None
) -> dict[str, DataFrame]:
    """Read verified Silver entities and return computed Gold marts."""
    namespace = silver_namespace or f"{env.CATALOG}.{env.SILVER_SCHEMA}"
    
    orders_df = spark.table(f"{namespace}.orders")
    lineitem_df = spark.table(f"{namespace}.lineitem")

    return {
        "transit_time_performance": build_transit_time_mart(lineitem_df),
        "order_fulfillment_sla": build_order_fulfillment_sla_mart(orders_df, lineitem_df),
        "ship_mode_delays": build_ship_mode_delay_mart(lineitem_df),
        "priority_fulfillment_speed": build_priority_fulfillment_mart(orders_df, lineitem_df),
    }


def publish_gold_marts(
    spark: SparkSession,
    marts: dict[str, DataFrame],
    gold_namespace: str | None = None,
) -> None:
    """Save Gold marts as Delta tables for dashboard consumption."""
    namespace = gold_namespace or f"{env.CATALOG}.{env.GOLD_SCHEMA}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")

    for mart_name, dataframe in marts.items():
        target_table = f"{namespace}.{mart_name}"
        dataframe.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target_table)
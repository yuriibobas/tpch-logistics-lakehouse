from pyspark.sql import DataFrame, SparkSession


def build_transit_time_mart(spark: SparkSession) -> DataFrame:
    """
    Q1: Calculate the median and p90 transit time
    (ship_date to receipt_date) for each ship_mode.
    """
    raise NotImplementedError(
        "Calculate median and p90 transit time for each ship_mode"
    )


def build_on_time_orders_mart(spark: SparkSession) -> DataFrame:
    """
    Q2: Calculate the share of orders where ALL line items
    arrived on or before commit_date, and compare it with
    the share of individual line items delivered on time.
    """
    raise NotImplementedError(
        "Calculate on-time order share vs. on-time line item share"
    )


def build_ship_mode_delay_mart(spark: SparkSession) -> DataFrame:
    """
    Q3: Calculate the share of delayed deliveries
    (receipt_date > commit_date) for each ship_mode.
    """
    raise NotImplementedError(
        "Calculate delay share for each ship_mode"
    )


def build_priority_fulfillment_mart(spark: SparkSession) -> DataFrame:
    """
    Q4: Analyze fulfillment speed for urgent orders (order_priority)
    compared with regular orders.
    """
    raise NotImplementedError(
        "Compare delivery speed across order priorities"
    )
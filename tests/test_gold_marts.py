"""Gold layer mart logic checks using a local Spark session."""

from datetime import date

import pytest
from pyspark.sql import SparkSession

from src.gold.logistics_marts import (
    build_order_fulfillment_sla_mart,
    build_priority_fulfillment_mart,
    build_ship_mode_delay_mart,
    build_transit_time_mart,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Provide a Spark session for local pytest execution."""
    session = SparkSession.getActiveSession()
    if session is None:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("gold-tests")
            .getOrCreate()
        )
    return session


def test_build_transit_time_mart(spark: SparkSession) -> None:
    """Test Q1: Transit time median and percentiles."""
    lines = spark.createDataFrame(
        [
            ("AIR", date(2026, 1, 1), date(2026, 1, 5)),   # 4 days
            ("AIR", date(2026, 1, 1), date(2026, 1, 11)),  # 10 days
            ("TRUCK", date(2026, 1, 1), date(2026, 1, 2)), # 1 day
        ],
        "ship_mode STRING, ship_date DATE, receipt_date DATE",
    )
    result = build_transit_time_mart(lines).collect()
    
    # Extract results by mode
    air = next(row for row in result if row.ship_mode == "AIR")
    truck = next(row for row in result if row.ship_mode == "TRUCK")
    
    assert air.total_shipments == 2
    assert air.median_transit_days == 4  # lower value for median in even count
    assert truck.total_shipments == 1
    assert truck.median_transit_days == 1


def test_build_order_fulfillment_sla_mart(spark: SparkSession) -> None:
    """Test Q2: Order fully on-time rate vs lineitem on-time rate."""
    orders = spark.createDataFrame(
        [
            (1, date(2026, 1, 10)),  # Fully on time order
            (2, date(2026, 2, 10)),  # Partially delayed order
        ],
        "order_key LONG, order_date DATE",
    )
    lines = spark.createDataFrame(
        [
            # Order 1: 2 items, both on time (receipt <= commit)
            (1, date(2026, 1, 20), date(2026, 1, 18)),
            (1, date(2026, 1, 20), date(2026, 1, 20)),
            # Order 2: 2 items, 1 on time, 1 delayed
            (2, date(2026, 2, 20), date(2026, 2, 19)),
            (2, date(2026, 2, 20), date(2026, 2, 25)),
        ],
        "order_key LONG, commit_date DATE, receipt_date DATE",
    )
    
    result = build_order_fulfillment_sla_mart(orders, lines).orderBy("order_month").collect()
    
    assert len(result) == 2
    
    # January (Order 1)
    assert result[0].total_orders == 1
    assert result[0].order_fully_on_time_rate == 100.0
    assert result[0].lineitem_on_time_rate == 100.0
    
    # February (Order 2)
    assert result[1].total_orders == 1
    assert result[1].order_fully_on_time_rate == 0.0   # 1 line was late, so whole order fails SLA
    assert result[1].lineitem_on_time_rate == 50.0     # 1 out of 2 lines was on time


def test_build_ship_mode_delay_mart(spark: SparkSession) -> None:
    """Test Q3: Delay rates by ship mode."""
    lines = spark.createDataFrame(
        [
            ("SHIP", date(2026, 1, 10), date(2026, 1, 11)), # Delayed
            ("SHIP", date(2026, 1, 10), date(2026, 1, 15)), # Delayed
            ("SHIP", date(2026, 1, 10), date(2026, 1, 9)),  # On time
            ("AIR", date(2026, 1, 10), date(2026, 1, 9)),   # On time
        ],
        "ship_mode STRING, commit_date DATE, receipt_date DATE",
    )
    result = build_ship_mode_delay_mart(lines).collect()
    
    ship = next(row for row in result if row.ship_mode == "SHIP")
    air = next(row for row in result if row.ship_mode == "AIR")
    
    assert ship.total_items_shipped == 3
    assert ship.delayed_items_count == 2
    assert ship.delay_rate_pct == 66.67
    
    assert air.total_items_shipped == 1
    assert air.delayed_items_count == 0
    assert air.delay_rate_pct == 0.0


def test_build_priority_fulfillment_mart(spark: SparkSession) -> None:
    """Test Q4: Fulfillment speed by order priority."""
    orders = spark.createDataFrame(
        [
            (1, date(2026, 1, 1), "1-URGENT"),
            (2, date(2026, 1, 1), "3-MEDIUM"),
        ],
        "order_key LONG, order_date DATE, order_priority STRING",
    )
    lines = spark.createDataFrame(
        [
            (1, date(2026, 1, 3)), # 2 days
            (1, date(2026, 1, 5)), # 4 days -> Final receipt is 4 days
            (2, date(2026, 1, 11)),# 10 days -> Final receipt is 10 days
        ],
        "order_key LONG, receipt_date DATE",
    )
    
    result = build_priority_fulfillment_mart(orders, lines).collect()
    
    urgent = next(row for row in result if row.order_priority == "1-URGENT")
    medium = next(row for row in result if row.order_priority == "3-MEDIUM")
    
    assert urgent.total_orders == 1
    assert urgent.avg_fulfillment_days == 4.0
    
    assert medium.total_orders == 1
    assert medium.avg_fulfillment_days == 10.0
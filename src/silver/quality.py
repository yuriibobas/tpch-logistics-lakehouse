from pyspark.sql import DataFrame


def validate_date_sequence(
    lineitem_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    """
    Validate the temporal logic:
    order_date <= ship_date <= receipt_date.
    Return a DataFrame containing rows that violate this rule.
    """
    raise NotImplementedError("Implement date sequence validation")


def validate_categorical_values(lineitem_df: DataFrame) -> DataFrame:
    """
    Validate that ship_mode, return_flag, and line_status
    belong to the allowed values defined in logistics_rules.json.
    """
    raise NotImplementedError("Implement categorical value validation")


def validate_referential_integrity(lineitem_df: DataFrame, orders_df: DataFrame) -> None:
    """
    Validate:
    1. Every lineitem.order_key exists in orders.
    2. There are no orders with zero line items.
    """
    raise NotImplementedError("Implement referential integrity validation")
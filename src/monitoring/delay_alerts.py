from pyspark.sql import DataFrame, SparkSession


def compute_monthly_delay_rate(spark: SparkSession) -> DataFrame:
    """
    Calculates the number of items, received after commit_date, 
    monthly (l_receiptdate > l_commitdate).
    """
    raise NotImplementedError("Implement monthly delay rate computing")

def check_delay_alert_threshold(
    delay_metrics_df: DataFrame, 
    threshold_pct: float = 15.0
) -> DataFrame:
    """
    Returns the periods, where delay rate reaches threshold (alert rule).
    """
    raise NotImplementedError("Implement alerting")
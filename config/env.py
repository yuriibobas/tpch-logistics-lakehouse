import os

CATALOG = os.getenv("DB_CATALOG", "hive_metastore")
SCHEMA_PREFIX = os.getenv("SCHEMA_PREFIX", "tpch")

BRONZE_SCHEMA = f"{SCHEMA_PREFIX}_bronze"
SILVER_SCHEMA = f"{SCHEMA_PREFIX}_silver"
GOLD_SCHEMA = f"{SCHEMA_PREFIX}_gold"

SOURCE_CATALOG = "samples"
SOURCE_SCHEMA = "tpch"
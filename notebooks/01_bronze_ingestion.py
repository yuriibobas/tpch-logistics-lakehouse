# Databricks notebook source
# COMMAND ----------
from src.bronze.ingest import ingest_raw_tpch
ingest_raw_tpch(spark)
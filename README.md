# Big Data Analytics: TPC-H Logistics Lakehouse

Group assignment for the Big Data course at the Ukrainian Catholic University (UCU). This repository implements an automated, Medallion Lakehouse architecture (Bronze, Silver, Gold) on Databricks to analyze delivery performance, SLA adherence, and transit time predictability for the **Logistics** customer profile.

## 📌 Project Links
- **Presentation:** https://canva.link/gx6ote4vejn9oxi
- **Databricks Environment:** https://dbc-6636093d-2727.cloud.databricks.com/
- **Silver ER Diagram:** [docs/diagrams/silver_er.png](docs/diagrams/silver_er.png)

## 🏗 Medallion Architecture
- **Bronze Layer (`tpch_bronze`):** Raw, unprocessed ingestion of 8 TPC-H tables directly from `samples.tpch` using Delta format.
- **Silver Layer (`tpch_silver`):** Cleaned, 3NF-compliant tables with enforced `snake_case` naming conventions, casted data types, and robust data quality rules. Invalid rows are quarantined, preserving parent record integrity.
- **Gold Layer (`tpch_gold`):** Business-level aggregations answering core SLA questions (transit time percentiles, order vs. lineitem fulfillment gaps, priority speed, and delay rate monitoring).

## 🛠 Local Setup & Development
The project strictly uses `uv` for dependency management.

```bash
# Clone the repository
git clone https://github.com/yuriibobas/tpch-logistics-lakehouse.git
cd tpch-logistics-lakehouse

# Install dependencies using uv
uv sync

# Run data quality & validation tests locally
uv run pytest

# Check code formatting
uv run ruff check .
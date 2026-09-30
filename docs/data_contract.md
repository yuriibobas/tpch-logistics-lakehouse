# Logistics Data Contract & Quality Specification

## 1. Overview
This document specifies the schema, data quality expectations, integrity rules, and analytical metrics for the **Logistics** customer profile migration to the Databricks Medallion Lakehouse. All transformations from Bronze to Silver and Gold must strictly adhere to this contract.

---

## 2. Medallion Layer Schemas

### 2.1. Bronze Layer (`tpch_bronze`)
- **Ingestion Strategy**: Raw ingestion from `samples.tpch` *as-is* with no transformations.
- **Storage Format**: Delta Lake.
- **Naming**: Direct mirror of source table names (`orders`, `lineitem`, `customer`, `part`, `partsupp`, `supplier`, `nation`, `region`).

### 2.2. Silver Layer (`tpch_silver`)
- **Design Paradigm**: Third Normal Form (3NF) relational model.
- **Conventions**: Lowercase `snake_case` column naming without prefixes (e.g., `l_orderkey` to `order_key`).

#### Core Table: `orders`
| Column Name | Data Type | Nullable | Description & Constraints |
| :--- | :--- | :--- | :--- |
| `order_key` | `BIGINT` | No | Primary Key |
| `cust_key` | `BIGINT` | No | Foreign Key to `customer.cust_key` |
| `order_status` | `STRING` | No | Status code (`O`, `F`, `P`) |
| `total_price` | `DECIMAL(18, 2)` | No | Order total value |
| `order_date` | `DATE` | No | Placement date |
| `order_priority`| `STRING` | No | Priority level  |
| `clerk` | `STRING` | No | Clerk identifier |
| `ship_priority` | `INT` | No | Shipping priority rank |
| `comment` | `STRING` | Yes | Order remarks |

#### Core Table: `lineitem`
| Column Name | Data Type | Nullable | Description & Constraints |
| :--- | :--- | :--- | :--- |
| `order_key` | `BIGINT` | No | Composite PK / FK to `orders.order_key` |
| `part_key` | `BIGINT` | No | Foreign Key to `part.part_key` |
| `supp_key` | `BIGINT` | No | Foreign Key to `supplier.supp_key` |
| `line_number` | `INT` | No | Composite PK (Sequence number within order) |
| `quantity` | `DECIMAL(18, 2)` | No | Ordered item quantity |
| `extended_price`| `DECIMAL(18, 2)` | No | Base price before discount |
| `discount` | `DECIMAL(18, 2)` | No | Discount rate |
| `tax` | `DECIMAL(18, 2)` | No | Applicable tax rate |
| `return_flag` | `STRING` | No | Return status (`A`, `N`, `R`) |
| `line_status` | `STRING` | No | Line item state (`O`, `F`) |
| `ship_date` | `DATE` | No | Fulfillment shipping date |
| `commit_date` | `DATE` | No | Promised delivery commitment date |
| `receipt_date` | `DATE` | No | Actual customer receipt date |
| `ship_instruct` | `STRING` | Yes | Delivery instructions |
| `ship_mode` | `STRING` | No | Transportation mode |
| `comment` | `STRING` | Yes | Line remarks |

---

## 3. Data Quality & Validation Rules (Silver Layer Enforcement)

Every ETL pipeline executing the Silver layer build must enforce and record validation status for the following rules:

### 3.1. Temporal & Chronological Consistency
- **Shipment Sequence**: An order cannot be shipped before it is placed:
  $$\text{orders.order\_date} \le \text{lineitem.ship\_date}$$

- **Transit Sequence**: An item cannot be received before it has been shipped:
  $$\text{lineitem.ship\_date} \le \text{lineitem.receipt\_date}$$


### 3.2. Categorical Whitelists (Allowed Values)
Categorical attributes must match the whitelists profiled from source data:
- `ship_mode` in `['AIR', 'FOB', 'MAIL', 'RAIL', 'REG AIR', 'SHIP', 'TRUCK']`
- `line_status` in `['O', 'F']`
- `return_flag` in `['A', 'N', 'R']`

### 3.3. Referential & Structural Integrity
- **Child-to-Parent Integrity**: Every line item must resolve to an existing `orders.order_key`.
- **Non-Empty Orders**: No order record may contain zero line items.

---

## 4. Gold Layer Specifications (Business Marts)

The Gold layer must provide aggregated data structures to directly answer the customer's analytical questions:

### 4.1. `mart_transit_time_performance` (Question 1)
- **Granularity**: `ship_mode`
- **Metrics**: 
  - `median_transit_days`: 50th percentile of (receipt_date - ship_date).
  - `p90_transit_days`: 90th percentile of (receipt_date - ship_date).
- **Objective**: Identify the fastest mode (lowest median) vs. the most predictable mode (lowest spread/p90).

### 4.2. `mart_order_fulfillment_sla` (Question 2)
- **Granularity**: Aggregate / Monthly
- **Metrics**:
  - `lineitem_on_time_rate`: Share of line items where receipt_date <= commit_date.
  - `order_fully_on_time_rate`: Share of orders where **all** constituent line items met receipt_date <= commit_date.
- **Objective**: Quantify and explain the SLA gap between individual line item delivery and whole-order completion.

### 4.3. `mart_ship_mode_delays` (Question 3)
- **Granularity**: `ship_mode`
- **Metrics**:
  - `total_items_shipped`: Total count of line items.
  - `delayed_items_count`: Count of items where receipt_date > commit_date.
  - `delay_rate_pct`: delayed_items_count / total_items_shipped * 100.
- **Objective**: Identify the worst-performing delivery partner/mode by delay frequency.

### 4.4. `mart_priority_fulfillment_speed` (Question 4)
- **Granularity**: `order_priority`
- **Metrics**:
  - Average and median order fulfillment duration.
- **Objective**: Empirically evaluate whether urgent-priority orders are fulfilled significantly faster than routine orders.

---

## 5. Continuous Monitoring & Alerting Specification
- **Metric to Track**: Monthly `delay_rate_pct` across all fulfilled shipments.
- **Alert Condition**: Trigger an alert when the monthly delay rate exceeds a designated SLA threshold (e.g., > 15% or a 20% relative increase period-over-period).
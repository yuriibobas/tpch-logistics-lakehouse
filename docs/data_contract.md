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

#### Core-table source-to-Silver mapping

The core-table types and nullability above remain unchanged. Use these exact
column mappings; the six supporting tables include their mappings below.

| Table | Source column | Silver column |
| :--- | :--- | :--- |
| `orders` | `o_orderkey` | `order_key` |
| `orders` | `o_custkey` | `cust_key` |
| `orders` | `o_orderstatus` | `order_status` |
| `orders` | `o_totalprice` | `total_price` |
| `orders` | `o_orderdate` | `order_date` |
| `orders` | `o_orderpriority` | `order_priority` |
| `orders` | `o_clerk` | `clerk` |
| `orders` | `o_shippriority` | `ship_priority` |
| `orders` | `o_comment` | `comment` |
| `lineitem` | `l_orderkey` | `order_key` |
| `lineitem` | `l_partkey` | `part_key` |
| `lineitem` | `l_suppkey` | `supp_key` |
| `lineitem` | `l_linenumber` | `line_number` |
| `lineitem` | `l_quantity` | `quantity` |
| `lineitem` | `l_extendedprice` | `extended_price` |
| `lineitem` | `l_discount` | `discount` |
| `lineitem` | `l_tax` | `tax` |
| `lineitem` | `l_returnflag` | `return_flag` |
| `lineitem` | `l_linestatus` | `line_status` |
| `lineitem` | `l_shipdate` | `ship_date` |
| `lineitem` | `l_commitdate` | `commit_date` |
| `lineitem` | `l_receiptdate` | `receipt_date` |
| `lineitem` | `l_shipinstruct` | `ship_instruct` |
| `lineitem` | `l_shipmode` | `ship_mode` |
| `lineitem` | `l_comment` | `comment` |

#### Supporting tables: exact Silver schema

The following six tables complete the eight-table Silver contract. Column order
follows the source. The source-to-Silver mappings in this section cover **all 61 columns**; use
explicit mappings rather than just removing prefixes (for example,
`ps_availqty` becomes `avail_quantity`). These
are target design decisions, not a claim about the observed source types.
Existing `orders` and `lineitem` names, types, and nullability above are preserved.

#### Table: `region`

| Source column | Silver column | Data type | Nullable | Constraints / description |
| :--- | :--- | :--- | :--- | :--- |
| `r_regionkey` | `region_key` | `BIGINT` | No | PK; Region identifier |
| `r_name` | `name` | `STRING` | No | Region name |
| `r_comment` | `comment` | `STRING` | Yes | Region remarks |

#### Table: `nation`

| Source column | Silver column | Data type | Nullable | Constraints / description |
| :--- | :--- | :--- | :--- | :--- |
| `n_nationkey` | `nation_key` | `BIGINT` | No | PK; Nation identifier |
| `n_name` | `name` | `STRING` | No | Nation name |
| `n_regionkey` | `region_key` | `BIGINT` | No | FK to region.region_key; Region reference |
| `n_comment` | `comment` | `STRING` | Yes | Nation remarks |

#### Table: `supplier`

| Source column | Silver column | Data type | Nullable | Constraints / description |
| :--- | :--- | :--- | :--- | :--- |
| `s_suppkey` | `supp_key` | `BIGINT` | No | PK; Supplier identifier |
| `s_name` | `name` | `STRING` | No | Supplier name |
| `s_address` | `address` | `STRING` | No | Supplier address |
| `s_nationkey` | `nation_key` | `BIGINT` | No | FK to nation.nation_key; Nation reference |
| `s_phone` | `phone` | `STRING` | No | Phone stored as text |
| `s_acctbal` | `acct_balance` | `DECIMAL(18, 2)` | No | Account balance; negative values are allowed |
| `s_comment` | `comment` | `STRING` | Yes | Supplier remarks |

#### Table: `customer`

| Source column | Silver column | Data type | Nullable | Constraints / description |
| :--- | :--- | :--- | :--- | :--- |
| `c_custkey` | `cust_key` | `BIGINT` | No | PK; Customer identifier |
| `c_name` | `name` | `STRING` | No | Customer name |
| `c_address` | `address` | `STRING` | No | Customer address |
| `c_nationkey` | `nation_key` | `BIGINT` | No | FK to nation.nation_key; Nation reference |
| `c_phone` | `phone` | `STRING` | No | Phone stored as text |
| `c_acctbal` | `acct_balance` | `DECIMAL(18, 2)` | No | Account balance; negative values are allowed |
| `c_mktsegment` | `market_segment` | `STRING` | No | Customer market segment |
| `c_comment` | `comment` | `STRING` | Yes | Customer remarks |

#### Table: `part`

| Source column | Silver column | Data type | Nullable | Constraints / description |
| :--- | :--- | :--- | :--- | :--- |
| `p_partkey` | `part_key` | `BIGINT` | No | PK; Part identifier |
| `p_name` | `name` | `STRING` | No | Part name |
| `p_mfgr` | `manufacturer` | `STRING` | No | Manufacturer name |
| `p_brand` | `brand` | `STRING` | No | Brand name |
| `p_type` | `type` | `STRING` | No | Part type |
| `p_size` | `size` | `INT` | No | Part size |
| `p_container` | `container` | `STRING` | No | Container category |
| `p_retailprice` | `retail_price` | `DECIMAL(18, 2)` | No | Retail price |
| `p_comment` | `comment` | `STRING` | Yes | Part remarks |

#### Table: `partsupp`

| Source column | Silver column | Data type | Nullable | Constraints / description |
| :--- | :--- | :--- | :--- | :--- |
| `ps_partkey` | `part_key` | `BIGINT` | No | Composite PK; FK to part.part_key; Part reference; composite primary key |
| `ps_suppkey` | `supp_key` | `BIGINT` | No | Composite PK; FK to supplier.supp_key; Supplier reference; composite primary key |
| `ps_availqty` | `avail_quantity` | `INT` | No | Available inventory quantity |
| `ps_supplycost` | `supply_cost` | `DECIMAL(18, 2)` | No | Supply cost |
| `ps_comment` | `comment` | `STRING` | Yes | Part-supplier remarks |

#### Keys and relationship cardinality

| Child table / column(s) | Parent table / primary key | Cardinality |
| :--- | :--- | :--- |
| `nation(region_key)` | `region(region_key)` | Each child has exactly one parent; each parent has zero or more children |
| `supplier(nation_key)` | `nation(nation_key)` | Each child has exactly one parent; each parent has zero or more children |
| `customer(nation_key)` | `nation(nation_key)` | Each child has exactly one parent; each parent has zero or more children |
| `partsupp(part_key)` | `part(part_key)` | Each child has exactly one parent; each parent has zero or more children |
| `partsupp(supp_key)` | `supplier(supp_key)` | Each child has exactly one parent; each parent has zero or more children |
| `orders(cust_key)` | `customer(cust_key)` | Each child has exactly one parent; each parent has zero or more children |
| `lineitem(order_key)` | `orders(order_key)` | Each child has exactly one parent; each parent has one or more children |
| `lineitem(part_key, supp_key)` | `partsupp(part_key, supp_key)` | Each child has exactly one parent; each parent has zero or more children |
| `lineitem(part_key)` | `part(part_key)` | Each child has exactly one parent; each parent has zero or more children |
| `lineitem(supp_key)` | `supplier(supp_key)` | Each child has exactly one parent; each parent has zero or more children |

- `lineitem` primary key is **(`order_key`, `line_number`)**; neither column
  alone is unique. `partsupp` primary key is **(`part_key`, `supp_key`)**.
- `lineitem(part_key, supp_key)` must reference **one matching partsupp row**.
  Independent existence checks against part and supplier do not prove that the
  supplier offers that part. Direct part/supplier links are also documented;
  they are implied by the composite partsupp link and its validated parents.
- Preserve customers without orders, suppliers/parts without sales, and
  partsupp rows never ordered. Do not use joins that drop these parent rows.
- `orders` must have at least one line item, per the Logistics assignment.
- All identifiers use BIGINT; sequence/size/inventory counters use INT.
  Numeric measures use DECIMAL(18, 2), matching the existing core contract.
  Phone numbers remain STRING. Comments and lineitem delivery instructions
  are nullable; every other attribute is required.
- These are Silver requirements, not transformations or constraints imposed
  on Bronze. Silver must detect nulls, duplicate keys, invalid casts, and lost
  decimal precision before publishing data, and record its validation results.
- Target location: `workspace.tpch_silver`. Bronze is
  `workspace.tpch_bronze`; set `DB_CATALOG=workspace` and `SCHEMA_PREFIX=tpch`
  before importing the shared environment configuration in each notebook.

#### Normalization and enforcement

Retain the eight TPC-H entities, with one row per declared primary key. Store
geographical descriptions only in nation/region, customer details only in
customer, and supplier details only in supplier. `partsupp` resolves the
many-to-many relationship between parts and suppliers: inventory and supply
cost depend on the entire pair. Order attributes depend on order_key; line
attributes depend on the entire (order_key, line_number) key. Do not materialize
joined parent descriptions or Gold aggregates in Silver. This is the team's
3NF design under these declared dependencies; any additional business
functional dependencies discovered during profiling must be reviewed before
claiming stricter normalization. Retain the existing total_price attribute.

Databricks primary/foreign key declarations are **informational, not enforced**.
The Silver implementation must explicitly validate uniqueness, required
values, and **every** relationship above (including the two-column join).
The ER diagram documents the intended validated Silver state; it does not
claim these constraints have already been implemented.

Diagram source: `docs/diagrams/silver_er.mmd`. Presentation image:
`docs/diagrams/silver_er.png`. Keep the Mermaid attributes and relationships
aligned with this contract. Regenerate the PNG from the repository root with
`python3 docs/render_silver_er.py` (requires Graphviz). The script creates only
the PNG; it does not save intermediate DOT or SVG files. `DECIMAL_18_2` in
Mermaid means SQL `DECIMAL(18, 2)`.

Sources: [TPC-H schema and table definitions, clauses 1.2 and 1.4](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.18.0.pdf),
[Databricks constraint enforcement](https://docs.databricks.com/aws/en/tables/constraints).


---

## 3. Data Quality & Validation Rules (Silver Layer Enforcement)

Every ETL pipeline executing the Silver layer build must enforce and record validation status for the following rules:

### 3.1. Temporal & Chronological Consistency
- **Shipment Sequence**: An order cannot be shipped before it is placed (orders.order_date <= lineitem.ship_date)

- **Transit Sequence**: An item cannot be received before it has been shipped (lineitem.ship_date <= lineitem.receipt_date)


### 3.2. Categorical Whitelists (Allowed Values)
Categorical attributes must match the whitelists profiled from source data:
- `ship_mode` in `['AIR', 'FOB', 'MAIL', 'RAIL', 'REG AIR', 'SHIP', 'TRUCK']`
- `line_status` in `['O', 'F']`
- `return_flag` in `['A', 'N', 'R']`

### 3.3. Referential & Structural Integrity
- **Child-to-Parent Integrity**: Every line item must resolve to an existing `orders.order_key`.
- **Non-Empty Orders**: No order record may contain zero line items.
- **All-Table Keys**: Enforce the primary keys, required values, and all foreign keys listed in section 2.2, including the composite lineitem-to-partsupp reference.

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

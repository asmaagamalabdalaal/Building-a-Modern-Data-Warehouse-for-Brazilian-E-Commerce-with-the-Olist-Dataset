# 🛒 Building a Modern Data Warehouse for Brazilian E-Commerce with the Olist Dataset


## 1. Project Overview

This project implements a **Modern Data Warehouse** solution for the **Brazilian E-Commerce Olist dataset**, following a full **ELT (Extract, Load, Transform)** pipeline architecture. The platform ingests raw transactional data, processes it through layered transformations using **dbt Core**, and delivers clean, analytics-ready models into **Snowflake** for business intelligence reporting via **Power BI**.

The pipeline is fully orchestrated with **Apache Airflow**, containerized using **Docker**, and follows the **Medallion Architecture** pattern (Bronze → Silver → Gold) to ensure data quality, traceability, and scalability.

---

## 2. System Architecture

The end-to-end data flow follows a structured three-layer medallion pattern:

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│              Olist Brazilian E-Commerce CSV Files               │
└────────────────────────────┬────────────────────────────────────┘
                             │  Python ETL (Airflow DAG)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                               │
│          Raw data loaded as-is into Snowflake schemas           │
│  customers | orders | order_items | products | sellers          │
└────────────────────────────┬────────────────────────────────────┘
                             │  dbt Silver Models + Tests
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                               │
│         Cleaned, standardized, validated staging models         │
│  customers | orders | order_items | products | sellers          │
└────────────────────────────┬────────────────────────────────────┘
                             │  dbt Gold Models + Tests
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER                                │
│       Business-ready dimensional models & aggregations          │
│  dim_customers | dim_products | dim_sellers                     │
│  fact_orders   | agg_sales_by_day                               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      VISUALIZATION                              │
│              Power BI Dashboard (Snowflake connector)           │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Key Technologies

| Category | Technology | Role |
|---|---|---|
| **Orchestration** | Apache Airflow 2.7.1 | DAG scheduling, dependency management, pipeline monitoring |
| **Transformation** | dbt Core | SQL-based ELT transformations, testing, and documentation |
| **Data Warehouse** | Snowflake | Cloud-native analytical data warehouse |
| **Containerization** | Docker & Docker Compose | Reproducible, portable runtime environment |
| **Language** | Python 3.8+ | ETL scripting, Airflow DAGs |
| **Visualization** | Power BI | Business intelligence dashboards and reporting |
| **Message Broker** | Redis | Celery backend for Airflow task queue |
| **Metadata DB** | PostgreSQL | Airflow metadata store |

---

## 4. DAG Pipeline Structure

All workflows are managed as **Directed Acyclic Graphs (DAGs)** in Apache Airflow. The pipeline executes in this order:

```
create_bronze_schema
        │
create_bronze_tables
        │
   ┌────┴────────────────────────────────────┐
   │         │           │         │         │
load_      load_       load_     load_     load_
customers  orders   order_items products  sellers
   │         │           │         │         │
   └────┬────────────────────────────────────┘
        │
  dbt_run_silver ──► dbt_test_silver
                            │
              ┌─────────────┴─────────────┐
        dbt_run_dimensions         dbt_run_facts
              └─────────────┬─────────────┘
                            │
                  dbt_run_aggregations
                            │
                    dbt_test_gold
```

**Airflow DAGs View:**

![Airflow DAGs](airflow%20dags.png)

---

## 5. Data Layers

### 5.1 Bronze Layer — Raw Ingestion

The Bronze layer ingests data directly from the Olist CSV source files into Snowflake **without any transformation**, preserving the raw state for auditability and re-processing.

| Table | Source File |
|---|---|
| `bronze.customers` | `olist_customers_dataset.csv` |
| `bronze.orders` | `olist_orders_dataset.csv` |
| `bronze.order_items` | `olist_order_items_dataset.csv` |
| `bronze.products` | `olist_products_dataset.csv` |
| `bronze.sellers` | `olist_sellers_dataset.csv` |

### 5.2 Silver Layer — Cleaned & Standardized

dbt Silver models apply cleaning rules, type casting, null handling, and standardization. dbt tests validate primary key uniqueness and referential integrity at this layer.

| Model | Description |
|---|---|
| `silver.customers` | Standardized customer addresses and demographic fields |
| `silver.orders` | Validated order records with parsed timestamps |
| `silver.order_items` | Normalized line items with price and freight validations |
| `silver.products` | Standardized product info with category translations |
| `silver.sellers` | Cleaned seller data with location standardization |

### 5.3 Gold Layer — Analytics-Ready Models

The Gold layer follows a **Star Schema** design optimized for Power BI and analytical reporting.

**Dimension Tables:**

| Model | Description |
|---|---|
| `gold.dim_customers` | Customer dimension with full purchase history attributes |
| `gold.dim_products` | Product dimension with enriched category hierarchy |
| `gold.dim_sellers` | Seller dimension with computed performance metrics |

**Fact & Aggregate Tables:**

| Model | Description |
|---|---|
| `gold.fact_orders` | Central fact table linking all dimensions with order measures |
| `gold.agg_sales_by_day` | Pre-aggregated daily sales metrics for fast BI queries |

---

## 6. Data Quality

dbt tests are applied at both the Silver and Gold layers to enforce:

- **Primary key uniqueness** — no duplicate records in dimension tables
- **Referential integrity** — all foreign keys resolve to valid dimension rows
- **Date range validations** — order timestamps fall within expected windows
- **Value range checks** — prices, freight, and quantities are non-negative
- **Not-null constraints** — critical fields like `order_id` and `customer_id` are always populated

---

## 7. Power BI Dashboard

The Power BI dashboard connects directly to the **Snowflake Gold layer** and provides interactive business intelligence across key reporting areas.

> 📁 Dashboard file: [`Powerbi Dashboard/`](Powerbi%20Dashboard/)

![Power BI Dashboard](https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/blob/3cdd40fadec940efba306a308395df956a0e8f37/Powerbi%20Dashboard/Annotation%202025-10-04%20201400.png)
![Power BI Dashboard](https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/blob/3cdd40fadec940efba306a308395df956a0e8f37/Powerbi%20Dashboard/Annotation%202025-10-04%20201442.png)
![Power BI Dashboard](https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/blob/3cdd40fadec940efba306a308395df956a0e8f37/Powerbi%20Dashboard/Annotation%202025-10-04%20201601.png)
![Power BI Dashboard](https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/blob/3cdd40fadec940efba306a308395df956a0e8f37/Powerbi%20Dashboard/Annotation%202025-10-04%20201743.png)
![Power BI Dashboard](https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/blob/3cdd40fadec940efba306a308395df956a0e8f37/Powerbi%20Dashboard/Annotation%202025-10-08%20150758.png)
**Key report pages include:**

- **Sales Overview** — Total revenue, order volume, average order value, and growth trends
- **Customer Analysis** — Segmentation by geography, purchase frequency, and lifetime value
- **Product Performance** — Top-selling categories, revenue breakdown, and return rates
- **Seller Insights** — Seller rankings, delivery performance, and regional distribution
- **Time Intelligence** — Month-over-month and year-over-year comparisons

---

## 8. dbt Documentation

dbt auto-generates interactive data lineage graphs and model documentation, covering column-level metadata, test coverage reports, and the full Bronze → Silver → Gold lineage.

> 📁 dbt docs: [`dbt docs/`](dbt%20docs/)
![dbt docs](https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/blob/ea1dfe7a2b71c5f594796a85209be13e34bfec7d/dbt%20docs/customers.png)
![dbt docs](https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/blob/ea1dfe7a2b71c5f594796a85209be13e34bfec7d/dbt%20docs/fact_orders.png)
![dbt docs](https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/blob/ea1dfe7a2b71c5f594796a85209be13e34bfec7d/dbt%20docs/products.png)
![dbt docs](https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/blob/ea1dfe7a2b71c5f594796a85209be13e34bfec7d/dbt%20docs/sellers.png)
![dbt docs](https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/blob/ea1dfe7a2b71c5f594796a85209be13e34bfec7d/dbt%20docs/agg_sales_by_day%20G.png)

---

## 9. Project Structure

```
Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset/
│
├── dags/                              # Apache Airflow DAG definitions
│   ├── dbt_olist_pipeline.py          # dbt orchestration DAG
│   └── olist_etl_pipeline.py          # Bronze ingestion DAG
│
├── dbt_olistproject/                  # dbt Core project
│   ├── models/
│   │   ├── silver/                    # Cleaning & standardization models
│   │   │   ├── customers.sql
│   │   │   ├── orders.sql
│   │   │   ├── order_items.sql
│   │   │   ├── products.sql
│   │   │   └── sellers.sql
│   │   └── gold/                      # Dimensional & aggregate models
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       ├── dim_sellers.sql
│   │       ├── fact_orders.sql
│   │       └── agg_sales_by_day.sql
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── dbt docs/                          # Generated dbt HTML documentation
│
├── Powerbi Dashboard/                 # Power BI report file (.pbix) & screenshots
│
├── airflow dags.png                   # Screenshot: Airflow DAG graph view
├── Dockerfile                         # Custom Airflow image with dbt + Snowflake
├── docker-compose.yaml                # Full service stack definition
├── requirements.txt                   # Python dependencies
├── E-Commerce with the Olist.pdf      # Project report / presentation deck
├── .gitignore
└── README.md
```

---

## 10. Setup & Deployment

### Prerequisites

- Docker Desktop with at least **4 GB RAM** allocated
- A **Snowflake account** with `ACCOUNTADMIN` or equivalent role
- Docker Compose v2+

### Step-by-Step

**1. Clone the repository**

```bash
git clone https://github.com/asmaagamalabdalaal/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset.git
cd Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset
```

**2. Configure environment variables**

Create a `.env` file in the project root:

```env
AIRFLOW_UID=50000
AIRFLOW_GID=0
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

AIRFLOW_VAR_SNOWFLAKE_ACCOUNT=your_account
AIRFLOW_VAR_SNOWFLAKE_USER=your_username
AIRFLOW_VAR_SNOWFLAKE_PASSWORD=your_password
AIRFLOW_VAR_SNOWFLAKE_DATABASE=OLIST_DB
AIRFLOW_VAR_SNOWFLAKE_WAREHOUSE=COMPUTE_WH
AIRFLOW_VAR_SNOWFLAKE_ROLE=ACCOUNTADMIN
```

**3. Create required directories**

```bash
mkdir -p ./dags ./logs ./plugins
```

**4. Initialize Airflow**

```bash
docker compose up airflow-init
```

**5. Start all services**

```bash
docker compose up -d
```

**6. Access the Airflow UI**

```
URL:      http://localhost:8080
Username: airflow
Password: airflow
```

**7. Configure Snowflake connection in Airflow UI**

Navigate to **Admin → Connections** and add:

| Field | Value |
|---|---|
| Connection Id | `snowflake_default` |
| Connection Type | `Snowflake` |
| Host | `your_account.snowflakecomputing.com` |
| Login | `your_username` |
| Password | `your_password` |
| Schema | `PUBLIC` |
| Extra | `{"account": "your_account", "warehouse": "COMPUTE_WH", "database": "OLIST_DB", "role": "ACCOUNTADMIN"}` |

**8. Trigger the pipeline**

Enable and trigger the `olist_etl_pipeline` DAG from the Airflow UI. The full Bronze → Silver → Gold pipeline will execute automatically.

### Stopping Services

```bash
# Stop services while preserving data
docker compose down

# Full clean reset (removes all volumes and data)
docker compose down --volumes
```

---

## 11. Monitoring

| Layer | Tool | What to Monitor |
|---|---|---|
| Pipeline execution | Airflow Web UI | DAG run status, task logs, retry history |
| Data lineage & testing | dbt docs | Model graph, test results, column coverage |
| Query performance | Snowflake Query History | Execution time, credit consumption, slow queries |

---

## 12. About the Olist Dataset

**Olist** is the largest department store in Brazilian marketplaces, connecting thousands of small businesses to major e-commerce channels under a single contract. The public dataset released on Kaggle contains approximately **100,000 orders** placed between **2016 and 2018** across multiple Brazilian marketplaces. It covers the full customer journey — from order placement and payment to product delivery and customer reviews.

📦 Dataset source: [Kaggle — Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

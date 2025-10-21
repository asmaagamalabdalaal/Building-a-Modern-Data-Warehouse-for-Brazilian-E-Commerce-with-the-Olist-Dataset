# Building a Modern Data Warehouse for Brazilian E-Commerce with the Olist Dataset# Apache Airflow with Docker



This project implements a modern data warehouse solution using the Olist Brazilian E-commerce dataset, implementing a robust ELT (Extract, Load, Transform) pipeline with Apache Airflow, dbt, and Snowflake.This project contains a Docker Compose setup for running Apache Airflow 2.7.1.



## Architecture## Prerequisites



```- Docker and Docker Compose installed on your system

Raw Data → Bronze → Silver → Gold- At least 4GB of RAM allocated to Docker

                     ↓         ↓

                  Cleaned    Facts## Directory Structure

                   Data      Dims

                            AggsThe project creates the following directory structure:

```

```

### Data Layers.

├── dags/            # Put your DAG files here

- **Bronze Layer**: Raw data loaded directly from source├── logs/            # Airflow logs

- **Silver Layer**: Cleaned and standardized data├── plugins/         # Custom Airflow plugins

- **Gold Layer**: ├── .env            # Environment variables

  - Dimensional Models (Facts & Dimensions)└── docker-compose.yaml

  - Aggregated Analytics Tables```



## Tech Stack## Getting Started



- **Orchestration**: Apache Airflow 2.7.11. Create the necessary directories:

- **Transformation**: dbt Core```bash

- **Data Warehouse**: Snowflakemkdir -p ./dags ./logs ./plugins

- **Containerization**: Docker & Docker Compose```

- **Language**: Python 3.8+

2. Initialize the environment:

## Project Structure```bash

docker compose up airflow-init

``````

airflowproject/

├── dags/3. Start all services:

│   ├── dbt_olist_pipeline.py```bash

│   └── olist_etl_pipeline.pydocker compose up -d

├── dbt_olistproject/```

│   ├── models/

│   │   ├── silver/4. Access the Airflow web interface:

│   │   │   ├── customers.sql   - URL: http://localhost:8080

│   │   │   ├── orders.sql   - Username: airflow

│   │   │   ├── order_items.sql   - Password: airflow

│   │   │   ├── products.sql

│   │   │   └── sellers.sql## Services

│   │   └── gold/

│   │       ├── dim_customers.sqlThe following services will be created:

│   │       ├── dim_products.sql- Airflow Webserver (Port 8080)

│   │       ├── dim_sellers.sql- Airflow Scheduler

│   │       ├── fact_orders.sql- Airflow Worker

│   │       └── agg_sales_by_day.sql- Airflow Triggerer

│   ├── dbt_project.yml- PostgreSQL (Database)

│   └── profiles.yml- Redis (Message Broker)

├── docker-compose.yaml

├── Dockerfile## Stopping the Services

└── requirements.txt

```To stop all services:

```bash

## Data Modelsdocker compose down

```

### Silver Layer Models

- **customers**: Cleaned customer data with standardized addressesTo stop and delete volumes (this will delete all data):

- **orders**: Validated order information```bash

- **order_items**: Normalized order items with price validationsdocker compose down --volumes

- **products**: Standardized product information```

- **sellers**: Cleaned seller data with location standardization

## Adding Custom DAGs

### Gold Layer Models

- **dim_customers**: Customer dimension with full historyPlace your DAG files in the `dags` directory. They will be automatically picked up by Airflow.

- **dim_products**: Product dimension with categories

- **dim_sellers**: Seller dimension with performance metrics## Environment Variables

- **fact_orders**: Main fact table linking all dimensions

- **agg_sales_by_day**: Daily sales aggregationsThe main environment variables are set in the `.env` file:

- `AIRFLOW_UID`: Set to 50000 (for Linux/macOS compatibility)

## Prerequisites- `AIRFLOW_GID`: Set to 0

- `_AIRFLOW_WWW_USER_USERNAME`: Default admin username

- Docker and Docker Compose installed on your system- `_AIRFLOW_WWW_USER_PASSWORD`: Default admin password
- At least 4GB of RAM allocated to Docker
- Snowflake account with appropriate permissions

## Setup Instructions

1. Clone the repository:
```bash
git clone https://github.com/yourusername/Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset.git
cd Building-a-Modern-Data-Warehouse-for-Brazilian-E-Commerce-with-the-Olist-Dataset
```

2. Set up environment variables in `.env`:
```env
AIRFLOW_VAR_SNOWFLAKE_ACCOUNT=your_account
AIRFLOW_VAR_SNOWFLAKE_USER=your_username
AIRFLOW_VAR_SNOWFLAKE_PASSWORD=your_password
AIRFLOW_VAR_SNOWFLAKE_DATABASE=OLIST_DB
AIRFLOW_VAR_SNOWFLAKE_WAREHOUSE=COMPUTE_WH
AIRFLOW_VAR_SNOWFLAKE_ROLE=ACCOUNTADMIN
```

3. Create the necessary directories:
```bash
mkdir -p ./dags ./logs ./plugins
```

4. Initialize the environment:
```bash
docker compose up airflow-init
```

5. Build and start the containers:
```bash
docker compose up -d
```

6. Access Airflow UI:
- URL: http://localhost:8080
- Username: airflow
- Password: airflow

7. Set up Snowflake connection in Airflow:
- Connection Id: snowflake_default
- Connection Type: Snowflake
- Host: your_account.snowflakecomputing.com
- Login: your_username
- Password: your_password
- Schema: PUBLIC
- Extra: {"account": "your_account", "warehouse": "COMPUTE_WH", "database": "OLIST_DB", "role": "ACCOUNTADMIN"}

## DAG Structure

### olist_etl_pipeline
```
create_bronze_schema >> create_bronze_tables
create_bronze_tables >> [load_customers, load_orders, load_order_items, load_products, load_sellers]
[load_*] >> dbt_run_silver >> dbt_test_silver
dbt_test_silver >> [dbt_run_dimensions, dbt_run_facts]
[dbt_run_dimensions, dbt_run_facts] >> dbt_run_aggregations >> dbt_test_gold
```

## Data Quality Checks

- Primary key validations
- Referential integrity checks
- Date range validations
- Value range checks
- Null checks for required fields

## Services

The following services are included:
- Airflow Webserver (Port 8080)
- Airflow Scheduler
- Airflow Worker
- Airflow Triggerer
- PostgreSQL (Database)
- Redis (Message Broker)

## Monitoring

Monitor pipeline execution through:
1. Airflow UI - DAG runs and task status
2. dbt documentation - Data lineage and model descriptions
3. Snowflake query history - Performance metrics

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details

## Acknowledgments

- [Olist](https://olist.com/) for providing the dataset
- Apache Airflow community
- dbt community
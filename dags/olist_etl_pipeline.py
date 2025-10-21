from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os

# Get Snowflake credentials from environment variables
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"  # Default warehouse
SNOWFLAKE_DATABASE = "OLIST_DB"     # Your database name
SNOWFLAKE_ROLE = "ACCOUNTADMIN"     # Your role
SNOWFLAKE_SCHEMA = "BRONZE"         # Bronze layer schema

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 10, 3),
}

# Get Snowflake connection details from Airflow variables
SNOWFLAKE_CONN_ID = "snowflake_default"
DBT_PROJECT_DIR = "/opt/airflow/olistproject"

# SQL for creating bronze layer tables
CREATE_CUSTOMERS_BRONZE = """
CREATE TABLE IF NOT EXISTS BRONZE_CUSTOMERS (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
);
"""

CREATE_ORDERS_BRONZE = """
CREATE TABLE IF NOT EXISTS BRONZE_ORDERS (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);
"""

CREATE_ORDER_ITEMS_BRONZE = """
CREATE TABLE IF NOT EXISTS BRONZE_ORDER_ITEMS (
    order_id STRING,
    order_item_id INTEGER,
    product_id STRING,
    seller_id STRING,
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT
);
"""

CREATE_PRODUCTS_BRONZE = """
CREATE TABLE IF NOT EXISTS BRONZE_PRODUCTS (
    product_id STRING,
    product_category_name STRING,
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);
"""

CREATE_SELLERS_BRONZE = """
CREATE TABLE IF NOT EXISTS BRONZE_SELLERS (
    seller_id STRING,
    seller_zip_code_prefix STRING,
    seller_city STRING,
    seller_state STRING
);
"""

with DAG(
    'olist_etl_pipeline',
    default_args=default_args,
    description='End-to-end ETL pipeline for Olist data with bronze, silver, and gold layers',
    schedule_interval='@daily',
    catchup=False,
    tags=['olist', 'etl'],
) as dag:

    # Bronze Layer - Create Schema
    create_bronze_schema = SnowflakeOperator(
        task_id='create_bronze_schema',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}",
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
        database=SNOWFLAKE_DATABASE
    )

    # Bronze Layer - Create Tables
    create_bronze_tables = SnowflakeOperator(
        task_id='create_bronze_tables',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=[
            CREATE_CUSTOMERS_BRONZE,
            CREATE_ORDERS_BRONZE,
            CREATE_ORDER_ITEMS_BRONZE,
            CREATE_PRODUCTS_BRONZE,
            CREATE_SELLERS_BRONZE
        ],
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        autocommit=True
    )

    # Load Bronze Layer Data
    load_customers = SnowflakeOperator(
        task_id='load_customers',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.olist_customers
            FROM '@olist_stage/customers'
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )

    load_orders = SnowflakeOperator(
        task_id='load_orders',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.olist_orders
            FROM '@olist_stage/orders'
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )

    load_order_items = SnowflakeOperator(
        task_id='load_order_items',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.olist_order_items
            FROM '@olist_stage/order_items'
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )

    load_products = SnowflakeOperator(
        task_id='load_products',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.olist_products
            FROM '@olist_stage/products'
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )

    load_sellers = SnowflakeOperator(
        task_id='load_sellers',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.olist_sellers
            FROM '@olist_stage/sellers'
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )

    # Silver Layer - Transform data using dbt
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select path:silver',
    )

    # Test silver models
    dbt_test_silver = BashOperator(
        task_id='dbt_test_silver',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --select path:silver',
    )

    # Gold Layer - Dimensions
    dbt_run_dimensions = BashOperator(
        task_id='dbt_run_dimensions',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select dim_*',
    )

    # Gold Layer - Facts
    dbt_run_facts = BashOperator(
        task_id='dbt_run_facts',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select fact_*',
    )

    # Gold Layer - Aggregations
    dbt_run_aggregations = BashOperator(
        task_id='dbt_run_aggregations',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select agg_*',
    )

    # Test gold layer
    dbt_test_gold = BashOperator(
        task_id='dbt_test_gold',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --select path:gold',
    )

    # Task Dependencies for Bronze Layer
    create_bronze_schema >> create_bronze_tables
    create_bronze_tables >> [load_customers, load_orders, load_order_items, load_products, load_sellers]
    
    # Bronze to Silver Layer
    [load_customers, load_orders, load_order_items, load_products, load_sellers] >> dbt_run_silver >> dbt_test_silver
    
    # Silver to Gold Layer
    dbt_test_silver >> [dbt_run_dimensions, dbt_run_facts]
    [dbt_run_dimensions, dbt_run_facts] >> dbt_run_aggregations >> dbt_test_gold
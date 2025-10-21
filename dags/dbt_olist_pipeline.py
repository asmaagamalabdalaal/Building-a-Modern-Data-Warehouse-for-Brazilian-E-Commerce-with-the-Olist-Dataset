from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_olist_pipeline',
    default_args=default_args,
    description='A DAG to run dbt models for Olist project',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 10, 4),
    catchup=False,
    tags=['dbt', 'olist'],
) as dag:

    # Set the path to your dbt project
    DBT_PROJECT_DIR = r'C:\Users\ASUS\olistproject'
    
    # dbt debug task to ensure everything is set up correctly
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt debug',
    )

    # dbt run task to execute all models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run',
    )

    # dbt test task to run tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test',
    )

    # Set up task dependencies
    dbt_debug >> dbt_run >> dbt_test
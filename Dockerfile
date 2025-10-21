FROM apache/airflow:2.7.1

USER root

# Install required system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    libpq-dev \
    build-essential \
    && apt-get clean

# Install Python packages
RUN pip install --no-cache-dir \
    dbt-core==1.5.3 \
    dbt-snowflake==1.5.3 \
    snowflake-connector-python==3.1.1 \
    snowflake-sqlalchemy==1.5.0

# Create dbt directories
RUN mkdir -p /home/airflow/.dbt && \
    chown -R airflow:root /home/airflow/.dbt

USER airflow

# Set environment variables
ENV DBT_PROFILES_DIR=/home/airflow/.dbt

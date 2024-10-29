FROM apache/airflow:2.10.2-python3.10
USER airflow
COPY . $AIRFLOW_HOME

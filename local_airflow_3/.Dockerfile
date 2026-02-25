FROM apache/airflow:3.1.1
USER airflow
RUN pip install airflow-dag-dependencies

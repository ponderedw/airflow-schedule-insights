from airflow.sdk import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time


@dag(
    dag_id="transform_enrollment_metrics",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=False,
    catchup=False,
    schedule=(Dataset("consolidate_academic_records")),
)
def dag_transform_enrollment_metrics():
    @task(outlets=[Dataset("transform_enrollment_metrics")])
    def end_task():
        time.sleep(25)

    end_task()


dag_transform_enrollment_metrics()

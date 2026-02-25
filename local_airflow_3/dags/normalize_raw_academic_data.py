from airflow.sdk import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time


@dag(
    dag_id="normalize_raw_academic_data",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=False,
    catchup=False,
    schedule="*/30 * * * *",
)
def dag_normalize_raw_academic_data():
    @task(outlets=[Dataset("normalize_raw_academic_data")])
    def end_task():
        time.sleep(40)

    end_task()


dag_normalize_raw_academic_data()

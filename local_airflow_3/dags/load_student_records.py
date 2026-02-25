from airflow.sdk import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time


@dag(
    dag_id="load_student_records",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=False,
    catchup=False,
    schedule="*/10 * * * *",
)
def dag_load_student_records():
    @task(outlets=[Dataset("load_student_records")])
    def end_task():
        time.sleep(150)

    end_task()


dag_load_student_records()

from airflow.sdk import dag, task
from pendulum import datetime
import time


@dag(
    dag_id="load_lms_activity",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=True,
    catchup=False,
    schedule=None,
)
def dag_load_lms_activity():
    @task
    def try_task():
        time.sleep(20)

    try_task()


dag_load_lms_activity()

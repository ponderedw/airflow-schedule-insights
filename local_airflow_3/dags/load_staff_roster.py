from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    dag_id="load_staff_roster",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=False,
    catchup=False,
    schedule=None,
)
def dag_load_staff_roster():
    @task
    def try_task():
        pass

    try_task()


dag_load_staff_roster()

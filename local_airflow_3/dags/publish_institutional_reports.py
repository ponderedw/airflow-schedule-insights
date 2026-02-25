from airflow.sdk import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time


@dag(
    dag_id="publish_institutional_reports",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=False,
    catchup=False,
    schedule=(
        Dataset("consolidate_academic_records") & Dataset("transform_enrollment_metrics")
    ),
)
def dag_publish_institutional_reports():
    @task(outlets=[Dataset("publish_institutional_reports")])
    def end_task():
        time.sleep(25)

    end_task()


dag_publish_institutional_reports()

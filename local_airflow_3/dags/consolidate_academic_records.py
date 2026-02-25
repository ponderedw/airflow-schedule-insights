from airflow.sdk import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time


@dag(
    dag_id="consolidate_academic_records",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=True,
    catchup=False,
    schedule=(
        Dataset("normalize_raw_academic_data") & Dataset("transform_assessment_scores")
    ),
)
def dag_consolidate_academic_records():
    @task(outlets=[Dataset("consolidate_academic_records")])
    def end_task():
        time.sleep(25)

    end_task()


dag_consolidate_academic_records()

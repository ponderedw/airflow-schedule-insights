from airflow.sdk import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time


@dag(
    dag_id="transform_student_performance",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=False,
    catchup=False,
    schedule=(
        Dataset("normalize_raw_academic_data") & Dataset("transform_assessment_scores")
    ),
)
def dag_transform_student_performance():
    @task(outlets=[Dataset("transform_student_performance")])
    def end_task():
        time.sleep(25)
        a = 1 / 0
        return a

    end_task()


dag_transform_student_performance()

from airflow.sdk import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time


@dag(
    dag_id="transform_assessment_scores",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=False,
    catchup=False,
    schedule=(
        (Dataset("normalize_raw_academic_data") & Dataset("load_exam_results"))
        | Dataset("load_student_records")
    ),
)
def dag_transform_assessment_scores():
    @task(outlets=[Dataset("transform_assessment_scores")])
    def end_task():
        time.sleep(60)

    end_task()


dag_transform_assessment_scores()

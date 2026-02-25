from airflow.sdk import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import time


@dag(
    dag_id="load_exam_results",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=False,
    catchup=False,
    schedule="*/25 * * * *",
)
def dag_load_exam_results():
    @task(outlets=[Dataset("load_exam_results")])
    def end_task():
        time.sleep(120)

    TriggerDagRunOperator(
        task_id="trigger_normalize_academic_data",
        trigger_dag_id="normalize_raw_academic_data",
        wait_for_completion=False,
    )

    end_task()


dag_load_exam_results()

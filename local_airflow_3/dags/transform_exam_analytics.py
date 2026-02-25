from airflow.sdk import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable


@dag(
    dag_id="transform_exam_analytics",
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=False,
    catchup=False,
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("*/40 * * * *", timezone="UTC"),
        datasets=(Dataset("consolidate_academic_records")),
    ),
)
def dag_transform_exam_analytics():
    @task(outlets=[Dataset("transform_exam_analytics")])
    def end_task():
        time.sleep(25)

    end_task()


dag_transform_exam_analytics()

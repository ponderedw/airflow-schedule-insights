from airflow.decorators import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable


@dag(dag_id='secondary_dag_5', max_active_runs=1, start_date=datetime(2023, 1, 1),
     is_paused_upon_creation=False, catchup=False, schedule_interval=DatasetOrTimeSchedule(
                        timetable=CronTriggerTimetable("*/40 * * * *",
                                                       timezone="UTC"),
                        datasets=(Dataset('secondary_dag_3'))))
def dag_test():
    @task(outlets=[Dataset('secondary_dag_5')])
    def end_task():
        time.sleep(25)

    end_task()


dag_test()

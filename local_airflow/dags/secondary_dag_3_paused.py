from airflow.decorators import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time


@dag(dag_id='secondary_dag_3', max_active_runs=1, start_date=datetime(2023, 1, 1),
     is_paused_upon_creation=True, catchup=False, schedule_interval=(Dataset('secondary_dag_1') & Dataset('secondary_dag_2')))
def dag_test():
    @task(outlets=[Dataset('secondary_dag_3')])
    def end_task():
        time.sleep(25)

    end_task()


dag_test()

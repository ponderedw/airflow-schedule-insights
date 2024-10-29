from airflow.decorators import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time


@dag(dag_id='secondary_dag_7', max_active_runs=1, start_date=datetime(2023, 1, 1),
     is_paused_upon_creation=False, catchup=False, schedule_interval=(Dataset('secondary_dag_1') & Dataset('secondary_dag_2')))
def dag_test():
    @task(outlets=[Dataset('secondary_dag_7')])
    def end_task():
        time.sleep(25)
        a = 1 / 0
        return a

    end_task()


dag_test()

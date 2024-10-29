from airflow.decorators import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
import time


@dag(dag_id='secondary_dag_1', max_active_runs=1, start_date=datetime(2023, 1, 1),
     is_paused_upon_creation=False, catchup=False, schedule_interval='*/30 * * * *')
def dag_test():
    @task(outlets=[Dataset('secondary_dag_1')])
    def end_task():
        time.sleep(40)

    end_task()


dag_test()

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from Delta_Ops.Delta_Ops import credits_organizer,movies_organizer

from airflow.operators.python import PythonOperator

start_date = datetime(2022, 11, 5)
current_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
default_args = {
    'owner': 'train',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('orchestra_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    start = DummyOperator(task_id="start")
    push_credits_silver = PythonOperator(task_id='pushing_credits_silver', python_callable=credits_organizer)
    push_movies_silver = PythonOperator(task_id='pushing_movies_silver', python_callable=movies_organizer)
    stop = DummyOperator(task_id="stop")

    start >> push_credits_silver >> push_movies_silver >> stop
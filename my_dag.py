from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 12, 9, 45, 0),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='final exam',
    schedule_interval='@daily',
)

start = DummyOperator(
    task_id = 'start',
    dag = dag,
)

def print_hello():
    print("Hello 1T")
    
python_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

start >> python_task

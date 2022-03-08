from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

def second_func():
    print('ini task kedua') 
    
with DAG(
    dag_id='dag_ds_intro_jilli',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2022,3,4),
    catchup=False
) as dag:
    run_first = DummyOperator(
    task_id='run_first'
    )
    run_second = PythonOperator(
    task_id='run_second',
    python_callable=second_func
    )
    run_third = BashOperator(
    task_id='run_third',
    bash_command='echo "ini task ketiga"'
    )
    run_first >> run_second >> run_third
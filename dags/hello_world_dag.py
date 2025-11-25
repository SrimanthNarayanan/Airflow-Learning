from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id = 'hello_world_dag',
    start_date = datetime(2024, 1, 1),
    schedule = '@daily',   
    catchup = True
) as dag:
    hello_task = BashOperator(
        task_id = 'hello_task',
        bash_command = 'echo "Dai Thambi!"'   
)
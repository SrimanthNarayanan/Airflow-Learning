from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


def transform_data():
    return("Transforming data in python")

with DAG(
    dag_id = "pipe_line_dag",
    schedule = '@daily',
    start_date = datetime(2024, 1, 1),
    catchup = False
) as dag:
    bash_extract = BashOperator(
        task_id = "bash_extract",
        bash_command = "echo 'extracting data'"
    )
    py_transform = PythonOperator(
        task_id = "py_transform",
        python_callable = transform_data
    )
    bash_load = BashOperator(
        task_id = "bash_load",
        bash_command = "echo 'Loading data'"
    )

    bash_extract >> py_transform >> bash_load
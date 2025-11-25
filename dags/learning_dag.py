from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    return("Hello From python Function")

with DAG(
    dag_id =  "Learning_dag",
    start_date = datetime(2024, 1, 1),
    schedule = '@daily',
    catchup = True
) as dag :
    bashtask = BashOperator(
        task_id = 'bash_task',
        bash_command = 'echo "Hello from Bash Operator"'
    )
    python_task = PythonOperator(
        task_id = 'python_task',
        python_callable = hello
    )

    bashtask >> python_task
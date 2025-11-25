from airflow import DAG
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator

with DAG(
    dag_id='run_phentaho',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
):

    load = SSHOperator(
        task_id='book_load',
        ssh_conn_id="shh_conn", 
        command = (
                    '"C:/Program Files/pdi/pdi-ce-9.3.0.0-428/data-integration/Pan.bat" '
                    '/norep '
                    '/nopvm '
                    '/file:"C:/Users/LENOVO/OneDrive/Documents/ETL/Airflow_ETL_Schedule/Transformation 1.ktr" '
                    '/level:Basic '
                    '/logfile:"C:/Users/LENOVO/OneDrive/Documents/ETL/Airflow_ETL_Schedule/log_fil/dummy_book_load.txt"'
                )

    )

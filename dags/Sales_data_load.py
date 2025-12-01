from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
import pendulum
from datetime import datetime

local_tz = pendulum.timezone("Asia/Kolkata")

with DAG(
    dag_id="Sales_data_load",
    schedule="29 18 * * *",
    catchup=False,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz)
):

    load = SSHOperator(
        task_id="sales_data_load",
        ssh_conn_id="shh_conn",                                            
        command=(
            'cmd /c "'
            '"C:\\Program Files\\pdi\\pdi-ce-9.3.0.0-428\\data-integration\\Pan.bat" '
            '/norep '
            '/nopvm '
            '/file:"C:\\Users\\LENOVO\\OneDrive\\Documents\\ETL\\Airflow_ETL_Schedule\\Exercise\\Sales_data_load.ktr" '
            '/param:"RUN_DATE={{ ds }}" '
            '/level:Basic '
            '/logfile:"C:\\Users\\LENOVO\\OneDrive\\Documents\\ETL\\Airflow_ETL_Schedule\\log_fil\\Sales_Data_load.txt"'
            '"'
        )

    )

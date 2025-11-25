from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

with DAG(
    dag_id = 'taskflow_example',
    schedule = '@daily',
    catchup = False,
    start_date = datetime(2025,1,1)
) :
    @task
    def api():
        return 100

    @task
    def transform(x):
        return x * 100

    @task
    def load(x):
        print(str(x) + 'is the result')
        hook = MsSqlHook(mssql_conn_id="my_mssql_conn")
        df = hook.get_pandas_df("SELECT TOP 5 * FROM dbo.CUSTOMERS")
        print(df)



    data = api()

    tans_data = transform(data)

    load(tans_data)



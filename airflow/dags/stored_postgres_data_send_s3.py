from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime

def fetch_data_from_postgres():
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM your_table;")
    data = cursor.fetchall()
    return data

def upload_to_s3(data):
    s3_hook = S3Hook(aws_conn_id='your_aws_conn_id')
    s3_hook.load_string(
        string_data=str(data),
        key='your_s3_key',
        bucket_name='streaming-project-stored-data-s3',
        replace=True
    )

default_args = {
    'start_date': datetime(2023, 1, 1),
}

with DAG('postgres_to_s3', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_data_from_postgres',
        python_callable=fetch_data_from_postgres
    )

    upload_data = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={'data': '{{ ti.xcom_pull(task_ids="fetch_data_from_postgres") }}'}
    )

    fetch_data >> upload_data

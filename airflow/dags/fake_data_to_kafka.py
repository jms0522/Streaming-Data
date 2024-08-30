from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from modules.generate_fake_data import generate_fake_data
from modules.send_to_kafka import send_to_kafka

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 28),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15),
}

dag = DAG(
    'fake_data_to_kafka',
    default_args=default_args,
    description='DAG to generate fake data and send to Kafka',
    schedule_interval='@daily',
    catchup=False,
)

# Task 1: Fake 데이터를 생성
def task_generate_fake_data():
    return generate_fake_data(30)

# Task 2: Kafka로 데이터 전송
def task_send_to_kafka(ti):
    data = ti.xcom_pull(task_ids='generate_fake_data')
    for user in data:
        send_to_kafka('fake-data', user)

generate_data = PythonOperator(
    task_id='generate_fake_data',
    python_callable=task_generate_fake_data,
    dag=dag,
)

send_data = PythonOperator(
    task_id='send_data',
    python_callable=task_send_to_kafka,
    dag=dag,
)

generate_data >> send_data

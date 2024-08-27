from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from modules.generate_fake_data import generate_fake_data
from modules.send_to_kafka import send_to_kafka

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'fake_data_to_kafka',
    default_args=default_args,
    description='DAG to generate fake data and send to Kafka',
    schedule_interval='@daily',  # 매일 실행
)

# Task 1: Fake 데이터를 생성하는 작업
def task_generate_fake_data(**context):
    data = generate_fake_data(30)  # 30명의 가짜 데이터 생성
    return data

# Task 2: 생성된 데이터를 Kafka로 보내는 작업
def task_send_to_kafka(**context):
    data = context['task_instance'].xcom_pull(task_ids='generate_data')
    for user in data:
        send_to_kafka('fake-data', user)

# PythonOperator로 Task를 정의
generate_data = PythonOperator(
    task_id='generate_data',
    python_callable=task_generate_fake_data,
    provide_context=True,
    dag=dag,
)

send_data = PythonOperator(
    task_id='send_data',
    python_callable=task_send_to_kafka,
    provide_context=True,
    dag=dag,
)

# Task의 실행 순서 정의
generate_data >> send_data
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from modules.generate_fake_data import generate_fake_data
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.dialects.postgresql import JSONB

# Postgres 연결 설정
DATABASE_URI = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
engine = create_engine(DATABASE_URI)
metadata = MetaData()

# 데이터 저장을 위한 테이블 정의
fake_data_table = Table('fake_data', metadata,
    Column('id', Integer, primary_key=True),
    Column('data', JSONB)
)

metadata.create_all(engine)

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fake_data_to_kafka',
    default_args=default_args,
    description='DAG to generate fake data and send to Kafka',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=2,
)

# Task 1: Fake 데이터를 생성하고 Postgres에 저장하는 작업
def task_generate_and_store_fake_data(**context):
    data = generate_fake_data(30)
    with engine.connect() as conn:
        for user in data:
            conn.execute(fake_data_table.insert().values(data=user))

generate_data = PythonOperator(
    task_id='generate_and_store_data',
    python_callable=task_generate_and_store_fake_data,
    dag=dag,
)

# Task 2: Postgres에서 데이터를 읽어와 Kafka로 보내는 작업
def task_send_to_kafka(**context):
    with engine.connect() as conn:
        result = conn.execute(fake_data_table.select())
        for row in result:
            send_to_kafka('fake-data', row['data'])

send_data = PythonOperator(
    task_id='send_data',
    python_callable=task_send_to_kafka,
    dag=dag,
    execution_timeout=timedelta(minutes=15)
)

# Task의 실행 순서 정의
generate_data >> send_data
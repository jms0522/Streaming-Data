from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, MetaData
from airflow.exceptions import AirflowException
from modules.generate_fake_data import generate_fake_data
from modules.send_to_kafka import send_to_kafka

# Postgres 연결 설정
DATABASE_URI = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
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
def task_generate_and_store_fake_data():
    try:
        data = generate_fake_data(30)  # 30개의 Fake 데이터 생성
        engine = create_engine(DATABASE_URI)
        metadata = MetaData(bind=engine)
        fake_data_table = Table('fake_data', metadata, autoload_with=engine)

        with engine.begin() as conn:
            conn.execute(fake_data_table.insert(), [{'data': user} for user in data])
    except Exception as e:
        raise AirflowException(f"Error in task_generate_and_store_fake_data: {str(e)}")

generate_data = PythonOperator(
    task_id='generate_and_store_data',
    python_callable=task_generate_and_store_fake_data,
    dag=dag,
)

# Task 2: Postgres에서 데이터를 읽어와 Kafka로 보내는 작업
def task_send_to_kafka():
    try:
        engine = create_engine(DATABASE_URI)
        metadata = MetaData(bind=engine)
        fake_data_table = Table('fake_data', metadata, autoload_with=engine)

        with engine.connect() as conn:
            result = conn.execute(fake_data_table.select())
            for row in result:
                send_to_kafka('fake-data', row['data'])
    except Exception as e:
        raise AirflowException(f"Error in task_send_to_kafka: {str(e)}")

send_data = PythonOperator(
    task_id='send_data',
    python_callable=task_send_to_kafka,
    dag=dag,
    execution_timeout=timedelta(minutes=15)
)

# Task의 실행 순서 정의
generate_data >> send_data

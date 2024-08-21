from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'run_api_send_to_topic',
    default_args=default_args,
    description='DAG to run the api_send_to_topic.py script using BashOperator',
    schedule_interval=timedelta(days=1),  # 매일 실행
)

# BashOperator를 사용하여 Python 스크립트 실행
run_python_script = BashOperator(
    task_id='run_api_send_to_topic_script',
    bash_command='python /opt/airflow/code/api_send_to_topic.py',  # 정확한 스크립트 경로 지정
    dag=dag,
)

run_python_script
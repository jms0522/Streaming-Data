from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.hooks.slack import SlackHook
from faker import Faker
import shortuuid
from datetime import datetime
from airflow.utils.dates import days_ago
import os
import requests

# 가짜 데이터 생성
def create_fake_user() -> dict:
    fake = Faker()
    fake_profile = fake.profile()

    key_list = ["name", "job", "residence", "blood_group", "sex", "birthdate"]
    fake_dict = {}

    for key in key_list:
        fake_dict[key] = fake_profile[key]

    fake_dict["phone_number"] = fake.phone_number()
    fake_dict["email"] = fake.email()
    fake_dict["uuid"] = shortuuid.uuid()
    fake_dict['birthdate'] = fake_dict['birthdate'].strftime("%Y%m%d")
    fake_dict['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return fake_dict

def generate_fake_data(num_records: int, **context):
    fake_users = []
    for _ in range(num_records):
        user = create_fake_user()
        fake_users.append(user)
    
    # XCom에 데이터 전달
    context['ti'].xcom_push(key='fake_data', value=fake_users)

# PostgreSQL에 데이터 삽입
def insert_data_into_postgres(**context):
    fake_data = context['ti'].xcom_pull(key='fake_data', task_ids='generate_fake_data')
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connector')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # 테이블 생성 쿼리
    create_table_query = """
    CREATE TABLE IF NOT EXISTS new_fake_data (
        uuid VARCHAR(50),
        name VARCHAR(100),
        job VARCHAR(100),
        residence VARCHAR(255),
        blood_group VARCHAR(30),
        sex VARCHAR(20),
        birthdate VARCHAR(30),
        phone_number VARCHAR(100),
        email VARCHAR(100),
        timestamp TIMESTAMP
    );
    """

    cursor.execute(create_table_query)

    # 데이터 삽입 쿼리
    insert_query = """
    INSERT INTO new_fake_data (uuid, name, job, residence, blood_group, sex, birthdate, phone_number, email, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for user in fake_data:
        cursor.execute(insert_query, (
            user['uuid'], user['name'], user['job'], user['residence'],
            user['blood_group'], user['sex'], user['birthdate'],
            user['phone_number'], user['email'], user['timestamp']
        ))

    conn.commit()
    cursor.close()
    conn.close()

# 성공 시 Slack 알림 전송
def send_slack_notification(**context):
    completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    slack_msg = f"Data Generate -> Insert to Postgres. Success Time: {completion_time}"
    webhook_url = 'https://hooks.slack.com/services/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'
    
    # Webhook 요청을 통해 메시지 전송
    payload = {
        "text": slack_msg,
        "channel": "#alerts"
    }
    response = requests.post(webhook_url, json=payload)

    if response.status_code != 200:
        raise ValueError(f"Request to Slack returned an error {response.status_code}, the response is: {response.text}")

# 실패 시 Slack 알림 전송
def task_failure_alert(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')

    slack_msg = f"Task failed: DAG {dag_id}, Task {task_id}, Execution Date: {execution_date}"
    webhook_url = 'https://hooks.slack.com/services/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'

    # Webhook 요청을 통해 메시지 전송
    payload = {
        "text": slack_msg,
        "channel": "#alerts"
    }
    response = requests.post(webhook_url, json=payload)

    if response.status_code != 200:
        raise ValueError(f"Request to Slack returned an error {response.status_code}, the response is: {response.text}")

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'on_failure_callback': task_failure_alert
}

# DAG 정의
with DAG(
    dag_id='fake_data_pipeline',
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    catchup=False,
) as dag:

    # 가짜 데이터 생성 Task
    generate_fake_data_task = PythonOperator(
        task_id='generate_fake_data',
        python_callable=generate_fake_data,
        op_args=[1000],  # 생성할 데이터 수 설정
    )

    # PostgreSQL에 데이터 삽입 Task
    insert_data_task = PythonOperator(
        task_id='insert_data_into_postgres',
        python_callable=insert_data_into_postgres,
    )

    # 성공 시 Slack 알림 Task
    slack_notification_task = PythonOperator(
        task_id='send_slack_notification',
        python_callable=send_slack_notification,
    )

    # Task 순서 정의
    generate_fake_data_task >> insert_data_task >> slack_notification_task

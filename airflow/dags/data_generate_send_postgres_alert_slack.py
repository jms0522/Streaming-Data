from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from faker import Faker
import shortuuid
from datetime import datetime
from airflow.utils.dates import days_ago
import os

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

def generate_fake_data(num_records: int, **context) -> list:
    fake_users = []
    for _ in range(num_records):
        user = create_fake_user()
        fake_users.append(user)
    return fake_users

# PostgreSQL에 데이터 삽입
def insert_data_into_postgres(**context):
    fake_data = context['ti'].xcom_pull(task_ids='generate_fake_data')
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
    slack_msg = f"데이터 생성 후 데이터베이스 (Postgres)에 저장을 완료했습니다.\n완료 시간: {completion_time}"

    slack_alert = SlackAPIPostOperator(
        task_id='send_slack_notification',
        slack_conn_id='slack_webhook',
        text=slack_msg,
        channel='#alerts',
        username='airflow'
    )

    return slack_alert.execute(context=context)

# 실패 시 Slack 알림 전송
def task_failure_alert(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url

    slack_msg = f"""
    :red_circle: Task Failed.
    *DAG*: {dag_id}
    *Task*: {task_id}
    *Execution Time*: {execution_date}
    *Log URL*: {log_url}
    """

    slack_alert = SlackAPIPostOperator(
        task_id='slack_failure_notification',
        slack_conn_id='slack_webhook',
        text=slack_msg,
        channel='#alerts',
        username='airflow'
    )

    return slack_alert.execute(context=context)

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
        op_args=[1000],  # 생성할 데이터 수 설정하기
        provide_context=True
    )

    # PostgreSQL에 데이터 삽입 Task
    insert_data_task = PythonOperator(
        task_id='insert_data_into_postgres',
        python_callable=insert_data_into_postgres,
        provide_context=True
    )

    # 성공 시 Slack 알림 Task
    slack_notification_task = PythonOperator(
        task_id='send_slack_notification',
        python_callable=send_slack_notification,
        provide_context=True
    )

    # Task 순서 정의
    generate_fake_data_task >> insert_data_task >> slack_notification_task

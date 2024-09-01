from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from io import StringIO

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG("monitor_airflow_logs", default_args=default_args, schedule_interval=timedelta(days=1))

def check_logs():
    pg_hook = PostgresHook(postgres_conn_id='postgres_log_connector')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM log WHERE event = 'FAILED';")
    failed_logs = cursor.fetchall()

    if failed_logs:
        output = StringIO()
        for log in failed_logs:
            output.write(f"{log}\n")
        output.seek(0)

        s3_hook = S3Hook(aws_conn_id='aws_s3')
        s3_hook.load_string(
            string_data=output.getvalue(),
            key=f"logs/failed_logs_{datetime.now().strftime('%Y%m%d')}.txt",
            bucket_name='streaming-project-stored-data-s3',
            replace=True
        )
        return True
    return False

check_logs_task = PythonOperator(
    task_id='check_logs_and_upload_to_s3',
    python_callable=check_logs,
    dag=dag,
)

alert_slack = SlackWebhookOperator(
    task_id='send_slack_alert',
    slack_webhook_conn_id='slack_webhook',
    message="Airflow Task Failed! Check logs for more details.",
    channel="#alerts",
    dag=dag,
)

# Task 순서 정의
check_logs_task >> alert_slack

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import csv

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
        file_path = "/home/ubuntu/streamingdata_project/airflow/logs/failed_logs.csv"
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([desc[0] for desc in cursor.description])  # Write header
            writer.writerows(failed_logs)
        return True
    return False

check_logs_task = PythonOperator(
    task_id='check_logs_and_save_csv',
    python_callable=check_logs,
    dag=dag,
)

alert_slack = SlackWebhookOperator(
    task_id='send_slack_alert',
    slack_webhook_conn_id='slack_webhook',
    message="Airflow Task Completed! Logs have been saved.",
    channel="#alerts",
    dag=dag,
)

# Task 순서 정의
check_logs_task >> alert_slack

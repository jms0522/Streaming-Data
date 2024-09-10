from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import time
import psycopg2

# PostgreSQL 연결 정보
POSTGRES_CONN_ID = 'postgres_connector'

# 업비트에서 마켓 데이터 가져와 PostgreSQL에 저장
def fetch_and_store_markets(**context):
    url = "https://api.upbit.com/v1/market/all"
    response = requests.get(url)
    
    if response.status_code == 200:
        markets_data = response.json()
        postgres_hook = PostgresHook(postgres_conn_id=postgres_connector)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # 테이블 생성 쿼리
        create_table_query = """
        CREATE TABLE IF NOT EXISTS upbit_markets (
            market VARCHAR(50) PRIMARY KEY,
            korean_name VARCHAR(100),
            english_name VARCHAR(100)
        );
        """
        cursor.execute(create_table_query)

        # 데이터 삽입 쿼리
        insert_query = """
        INSERT INTO upbit_markets (market, korean_name, english_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (market) DO NOTHING;
        """
        for market in markets_data:
            cursor.execute(insert_query, (market['market'], market['korean_name'], market['english_name']))
        
        conn.commit()
        cursor.close()
        conn.close()
    else:
        raise Exception(f"Failed to fetch market data, status code: {response.status_code}")

# 업비트에서 각 마켓의 가격 정보 가져와 PostgreSQL에 저장
def fetch_and_store_prices(**context):
    postgres_hook = PostgresHook(postgres_conn_id=postgres_connector)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # 마켓 데이터 가져오기
    cursor.execute("SELECT market FROM upbit_markets")
    markets = cursor.fetchall()

    # 테이블 생성 쿼리
    create_table_query = """
    CREATE TABLE IF NOT EXISTS upbit_prices (
        market VARCHAR(50),
        trade_price FLOAT,
        trade_volume FLOAT,
        PRIMARY KEY (market)
    );
    """
    cursor.execute(create_table_query)

    # 각 마켓에 대해 가격 데이터 가져오기
    for market in markets:
        market_code = market[0]
        url = f"https://api.upbit.com/v1/ticker?markets={market_code}"
        response = requests.get(url)

        if response.status_code == 200:
            price_data = response.json()[0]
            insert_query = """
            INSERT INTO upbit_prices (market, trade_price, trade_volume)
            VALUES (%s, %s, %s)
            ON CONFLICT (market) DO NOTHING;
            """
            cursor.execute(insert_query, (price_data['market'], price_data['trade_price'], price_data['trade_volume']))
        else:
            print(f"Failed to fetch price data for market {market_code}, status code: {response.status_code}")
        
        time.sleep(1)  # API rate limit 방지를 위한 지연

    conn.commit()
    cursor.close()
    conn.close()

# 마켓 데이터와 가격 데이터를 결합하여 최종 테이블에 저장
def combine_market_and_price_data(**context):
    postgres_hook = PostgresHook(postgres_conn_id=postgres_connector)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # 결합된 테이블 생성
    create_table_query = """
    CREATE TABLE IF NOT EXISTS upbit_combined_data AS
    SELECT m.market, m.korean_name, m.english_name, p.trade_price, p.trade_volume
    FROM upbit_markets m
    JOIN upbit_prices p ON m.market = p.market;
    """
    cursor.execute(create_table_query)

    conn.commit()
    cursor.close()
    conn.close()

# 성공 시 Slack 알림 전송
def send_slack_notification(**context):
    completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    slack_msg = f"Upbit Data Pipeline Completed. Success Time: {completion_time}"
    webhook_url = 'https://hooks.slack.com/services/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'

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
    'start_date': datetime(2024, 9, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_alert
}

# DAG 정의
with DAG(
    dag_id='upbit_data_pipeline',
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    catchup=False,
) as dag:

    # 마켓 데이터 가져오기 및 저장 Task
    fetch_markets_task = PythonOperator(
        task_id='fetch_and_store_markets',
        python_callable=fetch_and_store_markets,
    )

    # 가격 데이터 가져오기 및 저장 Task
    fetch_prices_task = PythonOperator(
        task_id='fetch_and_store_prices',
        python_callable=fetch_and_store_prices,
    )

    # 결합 테이블 생성 Task
    combine_data_task = PythonOperator(
        task_id='combine_market_and_price_data',
        python_callable=combine_market_and_price_data,
    )

    # 성공 시 Slack 알림 Task
    slack_notification_task = PythonOperator(
        task_id='send_slack_notification',
        python_callable=send_slack_notification,
    )

    # Task 순서 정의
    fetch_markets_task >> fetch_prices_task >> combine_data_task >> slack_notification_task

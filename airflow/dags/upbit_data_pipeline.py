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
    try:
        url = "https://api.upbit.com/v1/market/all"
        response = requests.get(url)

        if response.status_code == 200:
            markets_data = response.json()
            postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
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

    except Exception as e:
        print(f"Error occurred while fetching and storing markets: {e}")

# 업비트에서 각 마켓의 가격 정보 가져와 PostgreSQL에 저장
def fetch_and_store_prices(**context):
    def backoff(retry_count):
        return min(60 * (2 ** retry_count), 3600)  # 최대 1시간까지 백오프

    retry_count = 0
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        # 마켓 데이터 가져오기
        cursor.execute("SELECT market FROM upbit_markets")
        markets = cursor.fetchall()

        # 테이블 생성 쿼리
        create_table_query = """
        CREATE TABLE IF NOT EXISTS upbit_prices (
            market VARCHAR(50),
            trade_date DATE,
            trade_time VARCHAR(10),
            trade_price FLOAT,
            opening_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            prev_closing_price FLOAT,
            change VARCHAR(10),
            change_price FLOAT,
            change_rate FLOAT,
            trade_volume FLOAT,
            acc_trade_price FLOAT,
            acc_trade_price_24h FLOAT,
            acc_trade_volume FLOAT,
            acc_trade_volume_24h FLOAT,
            highest_52_week_price FLOAT,
            highest_52_week_date DATE,
            lowest_52_week_price FLOAT,
            lowest_52_week_date DATE,
            timestamp BIGINT,
            PRIMARY KEY (market)
        );
        """
        cursor.execute(create_table_query)

        # 각 마켓에 대해 가격 데이터 가져오기
        for market in markets:
            market_code = market[0]
            success = False
            while not success and retry_count < 5:
                try:
                    url = f"https://api.upbit.com/v1/ticker?markets={market_code}"
                    response = requests.get(url)

                    if response.status_code == 200:
                        success = True
                        print(f"Price data fetched for {market_code}")
                        price_data = response.json()[0]
                        insert_query = """
                        INSERT INTO upbit_prices (
                            market, trade_date, trade_time, trade_price, opening_price, high_price, low_price, prev_closing_price,
                            change, change_price, change_rate, trade_volume, acc_trade_price, acc_trade_price_24h,
                            acc_trade_volume, acc_trade_volume_24h, highest_52_week_price, highest_52_week_date,
                            lowest_52_week_price, lowest_52_week_date, timestamp
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (market) DO NOTHING;
                        """
                        cursor.execute(insert_query, (
                            price_data['market'],
                            price_data['trade_date'],
                            price_data['trade_time'],
                            price_data['trade_price'],
                            price_data['opening_price'],
                            price_data['high_price'],
                            price_data['low_price'],
                            price_data['prev_closing_price'],
                            price_data['change'],
                            price_data['change_price'],
                            price_data['change_rate'],
                            price_data['trade_volume'],
                            price_data['acc_trade_price'],
                            price_data['acc_trade_price_24h'],
                            price_data['acc_trade_volume'],
                            price_data['acc_trade_volume_24h'],
                            price_data['highest_52_week_price'],
                            price_data['highest_52_week_date'],
                            price_data['lowest_52_week_price'],
                            price_data['lowest_52_week_date'],
                            price_data['timestamp']
                        ))
                    elif response.status_code == 429:
                        print(f"Rate limit exceeded for {market_code}. Waiting {backoff(retry_count)} seconds...")
                        time.sleep(backoff(retry_count))
                        retry_count += 1
                    else:
                        print(f"Failed to fetch price data for {market_code}, status code: {response.status_code}")
                        success = True  # 다른 오류는 재시도하지 않음

                except Exception as e:
                    print(f"Error occurred for {market_code}: {e}")
                    success = True  # 오류 발생 시 루프 종료

        conn.commit()

    except Exception as e:
        print(f"Error occurred while fetching and storing prices: {e}")
    
    finally:
        cursor.close()
        conn.close()
        print("PostgreSQL connection closed")

# 마켓 데이터와 가격 데이터를 결합하여 최종 테이블에 저장
def combine_market_and_price_data(**context):
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
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
        print("Market and price data combined successfully.")

    except Exception as e:
        print(f"Error occurred while combining market and price data: {e}")

# 성공 시 Slack 알림 전송
def send_slack_notification(**context):
    try:
        completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        slack_msg = f"Upbit Data Pipeline Completed. Success Time: {completion_time}"
        webhook_url = 'https://hooks.slack.com/services/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'  # 실제 Webhook URL 입력

        payload = {
            "text": slack_msg,
            "channel": "#airflow"
        }
        response = requests.post(webhook_url, json=payload)

        if response.status_code != 200:
            raise ValueError(f"Request to Slack returned an error {response.status_code}, the response is: {response.text}")

    except Exception as e:
        print(f"Error occurred while sending Slack notification: {e}")

# 실패 시 Slack 알림 전송
def task_failure_alert(context):
    try:
        task_instance = context.get('task_instance')
        dag_id = context.get('dag').dag_id
        task_id = task_instance.task_id
        execution_date = context.get('execution_date')

        slack_msg = f"Task failed: DAG {dag_id}, Task {task_id}, Execution Date: {execution_date}"
        webhook_url = 'https://hooks.slack.com/services/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' 

        payload = {
            "text": slack_msg,
            "channel": "#airflow"
        }
        response = requests.post(webhook_url, json=payload)

        if response.status_code != 200:
            raise ValueError(f"Request to Slack returned an error {response.status_code}, the response is: {response.text}")

    except Exception as e:
        print(f"Error occurred while sending failure Slack notification: {e}")

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

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

# .env 파일 사용하여 환경 변수 로드
load_dotenv()

def send_to_kafka():
    api_key = os.getenv("RAPIDAPI_KEY")

    url = "https://instagram-scraper-api2.p.rapidapi.com/v1/likes"
    querystring = {"code_or_id_or_url": "CxYQJO8xuC6"}

    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "instagram-scraper-api2.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    # Kafka Producer 설정
    kafka_brokers = "kafka-kafka-1-1:9092,kafka-kafka-2-1:9093,kafka-kafka-3-1:9094"  # Kafka 컨테이너 호스트 이름 사용
    topic_name = "instagram_scrapper_api"  # 생성한 토픽 이름으로 변경

    conf = {'bootstrap.servers': kafka_brokers}
    producer = Producer(**conf)

    # Instagram API에서 받은 데이터
    data = response.json()
    print(data)

    # 각 아이템을 개별적으로 Kafka에 전송
    for item in data.get('data', {}).get('items', []):
        producer.produce(topic_name, str(item))
        producer.flush()

    print(f"Data sent to Kafka topic {topic_name}.")

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'run_api_send_to_kafka_topic',
    default_args=default_args,
    description='DAG to run the Instagram API script and send data to Kafka',
    schedule_interval=timedelta(days=1),  # 매일 실행
)

# PythonOperator를 사용하여 Python 함수를 실행
run_script = PythonOperator(
    task_id='run_api_send_to_kafka',
    python_callable=send_to_kafka,
    dag=dag,
)

run_script
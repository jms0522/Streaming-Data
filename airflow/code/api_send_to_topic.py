import os
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

# .env 파일 사용하여 환경 변수 로드
load_dotenv()

api_key = os.getenv("RAPIDAPI_KEY")

url = "https://instagram-scraper-api2.p.rapidapi.com/v1/likes"
querystring = {"code_or_id_or_url": "CxYQJO8xuC6"}

headers = {
    "x-rapidapi-key": api_key,
    "x-rapidapi-host": "instagram-scraper-api2.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# Kafka Producer 설정
kafka_brokers = "kafka-kafka-1-1:9092,kafka-kafka-2-1:9093,kafka-kafka-3-1:9094" # 모든 Kafka 브로커 주소 추가
topic_name = "instagram_scrapper_api"  # 생성한 토픽 이름으로 변경

conf = {'bootstrap.servers': kafka_brokers}
producer = Producer(**conf)

# 데이터를 Kafka에 전송하는 함수
def send_to_kafka(topic, value):
    producer.produce(topic, value.encode('utf-8'))
    producer.flush()

# Instagram API에서 받은 데이터
data = response.json()
print(data)

# 각 아이템을 개별적으로 Kafka에 전송
for item in data['data']['items']:
    send_to_kafka(topic_name, str(item))

print(f"Data sent to Kafka topic {topic_name}.")
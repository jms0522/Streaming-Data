from confluent_kafka import Producer
import json
from modules.generate_fake_data import generate_fake_data

bootstrap_servers = 'kafka:9092,kafka:9093,kafka:9094'
topic = 'fake-data'

producer = Producer({
    'bootstrap.servers': bootstrap_servers
})

def send_to_kafka(topic, message):
    producer.produce(topic, key=message['uuid'], value=json.dumps(message))
    producer.flush()

if __name__ == "__main__":
    # 여기서 몇명으로 할건지 선택
    data = generate_fake_data(30)

    # Kafka로 데이터 전송
    for user in data:
        print(f"Sending: {user['uuid']} to Kafka topic {topic}")
        send_to_kafka(topic, user)

    print("모든 데이터가 kafka로 전송되었습니다.")
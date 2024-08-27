FROM apache/airflow:2.10.0

USER root

# 필요한 Python 패키지를 설치
RUN pip install --user confluent_kafka faker shortuuid

USER airflow
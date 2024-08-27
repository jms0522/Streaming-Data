FROM apache/airflow:2.10.0

USER root

# 필요한 Python 패키지를 설치 (글로벌 경로에 설치)
RUN pip install confluent_kafka faker shortuuid

# 필요 시 추가 설정 (환경 변수 등)
ENV PYTHONPATH="/opt/airflow/modules:/opt/airflow/dags:$PYTHONPATH"

USER airflow
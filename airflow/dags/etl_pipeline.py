from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
import json
import pandas as pd

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jiseo33668@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

# DAG 정의
with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline using Airflow',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 데이터 추출: 외부 API에서 데이터 가져오기
    extract = SimpleHttpOperator(
        task_id='extract_data',
        method='GET',
        http_conn_id='external_api',  
        endpoint='/data',
        headers={"Content-Type": "application/json"},
        response_filter=lambda response: response.json(),
        log_response=True,
    )

    # 데이터 변환: JSON 데이터를 DataFrame으로 변환하고 정제
    def transform(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract_data')
        df = pd.DataFrame(data)
        # 데이터 정제 로직 추가
        df = df.dropna()
        # 변환된 데이터를 다시 XCom에 푸시
        ti.xcom_push(key='transformed_data', value=df.to_json(orient='records'))

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
        provide_context=True,
    )

    # 데이터 적재: PostgreSQL에 데이터 로드
    def load(**kwargs):
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
        df = pd.read_json(transformed_data, orient='records')
        postgres_hook = PostgresHook(postgres_conn_id='postgres_connector')
        # 테이블에 데이터 적재
        postgres_hook.insert_rows(
            table='target_table',
            rows=df.values.tolist(),
            target_fields=df.columns.tolist(),
            commit_every=1000
        )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load,
        provide_context=True,
    )

    # PostgreSQL 테이블 생성 (필요 시)
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_connector',
        sql="""
        CREATE TABLE IF NOT EXISTS target_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            value FLOAT,
            timestamp TIMESTAMP
        );
        """,
    )

    # 태스크 순서 정의
    extract >> transform_data >> create_table >> load_data

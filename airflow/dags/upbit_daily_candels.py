from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import asyncio
import aiohttp
import pandas as pd
from sqlalchemy import create_engine, exc, insert, Column, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from airflow.hooks.base_hook import BaseHook
import logging

# 로깅 설정
logging.basicConfig(
    filename='/home/ubuntu/streamingdata_project/upbit_data_collection.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# Airflow DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],  # 필요 시 이메일 설정
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'collect_upbit_daily_candles',
    default_args=default_args,
    description='Collect Upbit daily candle data and store in PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 4, 1),
    catchup=False,
)

# ORM 베이스 클래스
Base = declarative_base()

# 테이블 정의
class Market(Base):
    __tablename__ = 'markets'
    market = Column(String, primary_key=True)

class DailyCandle(Base):
    __tablename__ = 'daily_candles'
    market = Column(String, ForeignKey('markets.market'), nullable=False)
    candle_date_time_kst = Column(DateTime, nullable=False)
    opening_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    trade_price = Column(Float)
    candle_acc_trade_volume = Column(Float)
    __table_args__ = (
        # Primary Key는 이미 설정됨 (market, candle_date_time_kst)
    )

# Airflow Connection에서 PostgreSQL 연결 정보 가져오기
def get_postgres_connection():
    conn = BaseHook.get_connection('upbit_postgres')
    return {
        'user': conn.login,
        'password': conn.password,
        'host': conn.host,
        'port': conn.port,
        'database': conn.schema,
    }

# markets 테이블에 데이터 삽입하는 함수
def insert_markets():
    logging.info("Markets 데이터 삽입 시작")
    conn_info = get_postgres_connection()
    DATABASE_URL = f"postgresql+psycopg2://{conn_info['user']}:{conn_info['password']}@{conn_info['host']}:{conn_info['port']}/{conn_info['database']}"
    engine = create_engine(DATABASE_URL, echo=False, future=True)
    
    # ORM 베이스 클래스와 테이블 정의
    Base.metadata.create_all(engine)
    
    # Upbit API 엔드포인트
    MARKET_URL = 'https://api.upbit.com/v1/market/all?isDetails=false'
    
    async def fetch_markets(session):
        async with session.get(MARKET_URL) as response:
            response.raise_for_status()
            return await response.json()
    
    async def get_markets():
        async with aiohttp.ClientSession() as session:
            markets = await fetch_markets(session)
            return [market['market'] for market in markets]
    
    async def main():
        markets = await get_markets()
        markets_to_insert = [{'market': market} for market in markets]
        
        # markets 테이블 객체 가져오기
        markets_table = Market.__table__
        
        # Prepare insert statement with on conflict do nothing
        stmt = insert(markets_table).values(markets_to_insert)
        stmt = stmt.on_conflict_do_nothing(index_elements=['market'])
        
        # 데이터베이스에 삽입
        try:
            with engine.begin() as conn:
                conn.execute(stmt)
            logging.info("Markets 데이터가 PostgreSQL에 성공적으로 삽입되었습니다.")
        except exc.SQLAlchemyError as e:
            logging.error(f"Markets 데이터 삽입 중 오류 발생: {e}")
    
    asyncio.run(main())

# daily_candles 테이블에 데이터 삽입하는 함수
def fetch_and_insert_candles():
    logging.info("Daily candles 데이터 수집 및 삽입 시작")
    conn_info = get_postgres_connection()
    DATABASE_URL = f"postgresql+psycopg2://{conn_info['user']}:{conn_info['password']}@{conn_info['host']}:{conn_info['port']}/{conn_info['database']}"
    engine = create_engine(DATABASE_URL, echo=False, future=True)
    
    # ORM 베이스 클래스와 테이블 정의
    Base.metadata.create_all(engine)
    
    # Upbit API 엔드포인트
    CANDLE_DAY_URL = 'https://api.upbit.com/v1/candles/days'
    
    # 주요 시장 리스트 정의 (필요 시 업데이트 가능)
    MAJOR_MARKETS = [
        'KRW-BTC',
        'KRW-ETH',
        'KRW-XRP',
        'KRW-BCH',
        'KRW-ADA',
        'USDT-BTC',
        'USDT-ETH',
        # 추가로 원하는 시장을 여기에 추가하세요
    ]
    
    REQUESTS_PER_SECOND = 5  # 초당 요청 수 제한
    
    async def fetch(session, url, params=None):
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()
    
    async def get_upbit_daily_candles(session, market, count=200, to=None):
        params = {
            'market': market,
            'count': count
        }
        if to:
            params['to'] = to
        try:
            candles = await fetch(session, CANDLE_DAY_URL, params)
            return candles
        except Exception as e:
            logging.error(f"Error fetching candles for {market}: {e}")
            return []
    
    async def collect_new_candles(session, market, last_date, count=200):
        new_candles = []
        to = last_date
        while True:
            candles = await get_upbit_daily_candles(session, market, count=count, to=to)
            if not candles:
                break
            new_candles.extend(candles)
            last_candle_time = candles[-1]['candle_date_time_kst']
            to_datetime = datetime.strptime(last_candle_time, '%Y-%m-%dT%H:%M:%S') - timedelta(days=1)
            to = to_datetime.strftime('%Y-%m-%dT%H:%M:%S')
            if len(candles) < count:
                break
            await asyncio.sleep(1 / REQUESTS_PER_SECOND)
        return new_candles
    
    async def main():
        async with aiohttp.ClientSession() as session:
            all_new_data = []
            for idx, market in enumerate(MAJOR_MARKETS, start=1):
                logging.info(f"Processing: {market} ({idx}/{len(MAJOR_MARKETS)})")
                
                # 데이터베이스에서 해당 시장의 최신 날짜 조회
                query = """
                SELECT candle_date_time_kst FROM daily_candles
                WHERE market = :market
                ORDER BY candle_date_time_kst DESC
                LIMIT 1;
                """
                try:
                    with engine.connect() as conn:
                        result = conn.execute(query, {'market': market})
                        latest_entry = result.fetchone()
                        if latest_entry:
                            last_date = latest_entry[0].strftime('%Y-%m-%dT%H:%M:%S')
                        else:
                            last_date = None  # 데이터가 없으면 최신 200개 데이터 수집
                except exc.SQLAlchemyError as e:
                    logging.error(f"Error querying latest date for {market}: {e}")
                    last_date = None
                
                # 새로운 캔들 데이터 수집
                if last_date:
                    new_candles = await collect_new_candles(session, market, last_date, count=200)
                else:
                    new_candles = await get_upbit_daily_candles(session, market, count=200)
                
                if new_candles:
                    for candle in new_candles:
                        candle_datetime = datetime.strptime(candle['candle_date_time_kst'], '%Y-%m-%dT%H:%M:%S')
                        all_new_data.append({
                            'market': candle['market'],
                            'candle_date_time_kst': candle_datetime,
                            'opening_price': candle['opening_price'],
                            'high_price': candle['high_price'],
                            'low_price': candle['low_price'],
                            'trade_price': candle['trade_price'],
                            'candle_acc_trade_volume': candle['candle_acc_trade_volume']
                        })
                    logging.info(f"{market}: Collected {len(new_candles)} new candles")
                else:
                    logging.info(f"{market}: No new candles found or failed to collect")
                await asyncio.sleep(1 / REQUESTS_PER_SECOND)
            
            if all_new_data:
                new_df = pd.DataFrame(all_new_data)
                # 날짜 형식 재변환 (필요 시)
                new_df['candle_date_time_kst'] = pd.to_datetime(new_df['candle_date_time_kst'])
                
                # daily_candles 테이블 객체 가져오기
                candles_table = DailyCandle.__table__
                
                # Prepare insert statement with on conflict do nothing
                candles_records = new_df.to_dict(orient='records')
                stmt = insert(candles_table).values(candles_records)
                stmt = stmt.on_conflict_do_nothing(index_elements=['market', 'candle_date_time_kst'])
                
                # 데이터베이스에 삽입
                try:
                    with engine.begin() as conn:
                        conn.execute(stmt)
                    logging.info("New data successfully inserted into PostgreSQL")
                except exc.SQLAlchemyError as e:
                    logging.error(f"Error inserting data into PostgreSQL: {e}")
            else:
                logging.info("No new data to insert")
    
    asyncio.run(main())

# Airflow 태스크 정의
insert_markets_task = PythonOperator(
    task_id='insert_markets',
    python_callable=insert_markets,
    dag=dag,
)

fetch_and_insert_candles_task = PythonOperator(
    task_id='fetch_and_insert_candles',
    python_callable=fetch_and_insert_candles,
    dag=dag,
)

# 태스크 간의 의존성 설정
insert_markets_task >> fetch_and_insert_candles_task

import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, DateTime, ForeignKey, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import os

# PostgreSQL 연결 정보
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'
DB_HOST = '13.125.199.68'  # 실제 PostgreSQL 서버 주소
DB_PORT = '5432'
DB_NAME = 'airflow'

# CSV 파일 경로
MARKET_CSV_PATH = '/home/ubuntu/streamingdata_project/upbit_market_list_20240914-141011.csv'
DAILY_CANDLES_CSV_PATH = '/home/ubuntu/streamingdata_project/upbit_daily_candles_20240914-142131.csv'

# SQLAlchemy 엔진 생성
DATABASE_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(DATABASE_URL, echo=True, future=True)

# 세션 생성
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

# ORM 베이스 클래스
Base = declarative_base()

# 테이블 정의
class Market(Base):
    __tablename__ = 'markets'
    market = Column(String, primary_key=True)

class DailyCandle(Base):
    __tablename__ = 'daily_candles'
    market = Column(String, ForeignKey('markets.market'), primary_key=True, nullable=False)
    candle_date_time_kst = Column(DateTime, primary_key=True, nullable=False)
    opening_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    trade_price = Column(Float)
    candle_acc_trade_volume = Column(Float)

# 테이블 생성
Base.metadata.create_all(engine)

def insert_markets(csv_path, engine):
    """
    markets 테이블에 CSV 데이터를 삽입합니다.
    중복된 market은 무시합니다.
    """
    # CSV 파일 로드
    markets_df = pd.read_csv(csv_path)
    
    # 'market' 컬럼이 있는지 확인
    if 'market' not in markets_df.columns:
        print("CSV 파일에 'market' 컬럼이 없습니다.")
        return
    
    # 중복 제거
    markets = markets_df['market'].drop_duplicates().tolist()
    markets_to_insert = [{'market': market} for market in markets]
    
    # markets 테이블 객체 가져오기
    markets_table = Market.__table__
    
    # Insert statement with conflict handling
    stmt = insert(markets_table).values(markets_to_insert)
    stmt = stmt.on_conflict_do_nothing(index_elements=['market'])
    
    # 데이터베이스에 삽입
    try:
        with engine.begin() as conn:
            conn.execute(stmt)
        print("Markets 데이터가 PostgreSQL에 성공적으로 삽입되었습니다.")
    except SQLAlchemyError as e:
        print(f"Markets 데이터 삽입 중 오류 발생: {e}")

def insert_daily_candles(csv_path, engine):
    """
    daily_candles 테이블에 CSV 데이터를 삽입합니다.
    중복된 데이터는 무시합니다.
    """
    # CSV 파일 로드
    candles_df = pd.read_csv(csv_path)
    
    # 필요한 컬럼이 있는지 확인
    required_columns = ['market', 'candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price', 'candle_acc_trade_volume']
    if not all(col in candles_df.columns for col in required_columns):
        print("CSV 파일에 필요한 컬럼이 없습니다.")
        return
    
    # 날짜 형식 변환
    candles_df['candle_date_time_kst'] = pd.to_datetime(candles_df['candle_date_time_kst'])
    
    # 데이터 준비
    candles_records = candles_df.to_dict(orient='records')
    
    # daily_candles 테이블 객체 가져오기
    candles_table = DailyCandle.__table__
    
    # Insert statement with conflict handling
    stmt = insert(candles_table).values(candles_records)
    stmt = stmt.on_conflict_do_nothing(index_elements=['market', 'candle_date_time_kst'])
    
    # 데이터베이스에 삽입
    try:
        with engine.begin() as conn:
            conn.execute(stmt)
        print("Daily candles 데이터가 PostgreSQL에 성공적으로 삽입되었습니다.")
    except SQLAlchemyError as e:
        print(f"Daily candles 데이터 삽입 중 오류 발생: {e}")

if __name__ == "__main__":
    # markets 테이블에 데이터 삽입
    if os.path.exists(MARKET_CSV_PATH):
        insert_markets(MARKET_CSV_PATH, engine)
    else:
        print(f"Market CSV 파일이 존재하지 않습니다: {MARKET_CSV_PATH}")
    
    # daily_candles 테이블에 데이터 삽입
    if os.path.exists(DAILY_CANDLES_CSV_PATH):
        insert_daily_candles(DAILY_CANDLES_CSV_PATH, engine)
    else:
        print(f"Daily candles CSV 파일이 존재하지 않습니다: {DAILY_CANDLES_CSV_PATH}")
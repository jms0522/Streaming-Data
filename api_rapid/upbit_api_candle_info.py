import aiohttp
import asyncio
import time
import pandas as pd
from typing import List, Dict

# Upbit API 엔드포인트
MARKET_URL = 'https://api.upbit.com/v1/market/all'
CANDLE_DAY_URL = 'https://api.upbit.com/v1/candles/days'

# API 호출 제한 설정 (Upbit의 현재 제한을 확인하세요)
MAX_CONCURRENT_REQUESTS = 10  # 동시에 실행할 최대 요청 수
REQUESTS_PER_SECOND = 5        # 초당 요청 수 제한

async def fetch(session: aiohttp.ClientSession, url: str, params: Dict = None) -> Dict:
    """
    주어진 URL로 HTTP GET 요청을 보내고 JSON 응답을 반환합니다.
    """
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        return await response.json()

async def get_all_markets(session: aiohttp.ClientSession) -> List[str]:
    """
    Upbit API에서 모든 시장 정보를 가져와 'market' 리스트를 반환합니다.
    """
    try:
        markets = await fetch(session, MARKET_URL)
        market_list = [market['market'] for market in markets]
        return market_list
    except Exception as e:
        print(f"시장 정보를 가져오는 중 오류 발생: {e}")
        return []

async def get_upbit_daily_candles(session: aiohttp.ClientSession, market: str, count: int = 200, to: str = None) -> List[Dict]:
    """
    특정 시장의 일별 캔들 데이터를 가져옵니다.
    
    :param session: aiohttp 클라이언트 세션
    :param market: 시장 코드 (예: 'KRW-BTC')
    :param count: 요청할 캔들 데이터의 개수 (최대 200)
    :param to: 특정 시점 이전의 데이터를 가져올 때 사용 (ISO 8601 형식)
    :return: 캔들 데이터 리스트
    """
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
        print(f"캔들 데이터를 가져오는 중 오류 발생 for {market}: {e}")
        return []

async def collect_all_daily_candles(session: aiohttp.ClientSession, market: str) -> List[Dict]:
    """
    모든 일별 캔들 데이터를 수집하여 리스트로 반환합니다.
    
    :param session: aiohttp 클라이언트 세션
    :param market: 시장 코드 (예: 'KRW-BTC')
    :return: 모든 일별 캔들 데이터 리스트
    """
    all_candles = []
    to = None

    while True:
        candles = await get_upbit_daily_candles(session, market, count=200, to=to)
        if not candles:
            break

        all_candles.extend(candles)
        print(f"{market}: 수집된 캔들 데이터 개수: {len(all_candles)}")

        # 마지막 캔들의 'candle_date_time_kst'를 'to' 파라미터로 설정하여 다음 요청에 사용
        last_candle_time = candles[-1]['candle_date_time_kst']
        to = last_candle_time

        # Upbit API는 초당 10회 이하의 요청을 권장 
        await asyncio.sleep(0.2)  # 200ms 지연

        # 더 이상 가져올 데이터가 없으면 중단
        if len(candles) < 200:
            break

    return all_candles

async def fetch_and_save_daily_candles(session: aiohttp.ClientSession, market_list: List[str]):
    """
    모든 시장에 대해 일별 캔들 데이터를 가져와 CSV 파일로 저장합니다.
    """
    all_data = []

    for idx, market in enumerate(market_list, start=1):
        print(f"처리 중: {market} ({idx}/{len(market_list)})")
        candles = await collect_all_daily_candles(session, market)
        if candles:
            for candle in candles:
                all_data.append({
                    'market': market,
                    'candle_date_time_kst': candle['candle_date_time_kst'],
                    'opening_price': candle['opening_price'],
                    'high_price': candle['high_price'],
                    'low_price': candle['low_price'],
                    'trade_price': candle['trade_price'],
                    'candle_acc_trade_volume': candle['candle_acc_trade_volume']
                })
        else:
            print(f"{market}: 일별 캔들 데이터를 가져오지 못했습니다.")
        
        # API 호출 간에 지연을 추가
        await asyncio.sleep(1 / REQUESTS_PER_SECOND) 

    # 데이터프레임으로 변환
    df = pd.DataFrame(all_data)

    # CSV 파일로 저장
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    csv_filename = f"upbit_daily_candles_{timestamp}.csv"
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    print(f"일별 데이터가 '{csv_filename}' 파일로 저장되었습니다.")

async def main():
    start_time = time.time()
    
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        # 1. 모든 시장 코드 가져오기
        market_list = await get_all_markets(session)
        if not market_list:
            print("시장 정보를 가져오는 데 실패했습니다.")
            return

        print(f"총 {len(market_list)}개의 시장을 가져왔습니다.")

        # 2. 시장 정보를 CSV 파일로 저장
        market_df = pd.DataFrame({'market': market_list})
        market_csv_filename = f"upbit_market_list_{time.strftime('%Y%m%d-%H%M%S')}.csv"
        market_df.to_csv(market_csv_filename, index=False, encoding='utf-8-sig')
        print(f"시장 목록이 '{market_csv_filename}' 파일로 저장되었습니다.")

        # 3. 모든 시장에 대한 일별 캔들 데이터 요청 및 저장
        await fetch_and_save_daily_candles(session, market_list)
    
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"전체 작업 소요 시간: {elapsed:.2f}초")

if __name__ == "__main__":
    asyncio.run(main())
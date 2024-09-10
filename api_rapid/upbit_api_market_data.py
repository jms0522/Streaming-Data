import requests
import time

# 첫 번째 API 호출로 모든 시장 정보 가져오기
url = "https://api.upbit.com/v1/market/all"
response = requests.get(url)
markets_data = response.json()

for market_info in markets_data:
    market_code = market_info['market']
    
    # 두 번째 API 호출로 해당 마켓의 가격 정보 가져오기
    url2 = f"https://api.upbit.com/v1/ticker?markets={market_code}"
    response2 = requests.get(url2)
    
    if response2.status_code == 200:
        data2 = response2.json()
        print(f"Market: {market_code}")
        print(data2)
    else:
        print(f"Failed to retrieve data for market: {market_code}, Status Code: {response2.status_code}")
    
    time.sleep(1)
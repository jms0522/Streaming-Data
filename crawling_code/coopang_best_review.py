# 스크롤 내릴 때 마다 생성되는 스크롤 방식이라
# Html 파싱으론 안 될 거 같아서 
# 셀리니움으로 진행.
# 한 번 로딩되는 28개씩 밖에 가져오지 못 한다.

import requests
from bs4 import BeautifulSoup

url = 'https://pages.coupang.com/p/113731'
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

product_links = soup.find_all('a', href=True)

for link in product_links:
    href = link['href']
    if '/vp/products/' in href:
        product_id = href.split('/vp/products/')[1].split('?')[0]
        item_id = href.split('itemId=')[1].split('&')[0]
        print(f"Product ID: {product_id}, Item ID: {item_id}")



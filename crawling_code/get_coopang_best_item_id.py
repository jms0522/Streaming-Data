import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

# 대상 URL과 현재 날짜 설정
url = 'https://pages.coupang.com/p/113731'
current_date = datetime.now().strftime('%Y%m%d')

# 페이지 요청 및 파싱
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
product_links = soup.find_all('a', href=True)

# 제품 ID, 아이템 ID, 벤더 아이템 ID를 저장할 리스트
product_items = []

for link in product_links:
    href = link['href']
    if '/vp/products/' in href:
        product_id = href.split('/vp/products/')[1].split('?')[0]
        item_id = href.split('itemId=')[1].split('&')[0]
        vendor_item_id = href.split('vendorItemId=')[1].split('&')[0]
        product_items.append((product_id, item_id, vendor_item_id))

# CSV 파일로 저장
filename = f'product-items-{current_date}.csv'
with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Product ID', 'Item ID', 'Vendor Item ID'])  # 헤더 추가
    writer.writerows(product_items)

print(f"Data saved to {filename}")
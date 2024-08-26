from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
from tqdm import tqdm

# Selenium 설정
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # 헤드리스 모드
options.add_argument("--disable-gpu")
options.add_argument("--disable-blink-features=AutomationControlled")  # 봇 감지 차단

# 크롬 드라이버 설정
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# 크롤링할 URL
url = "https://www.coupang.com/vp/products/7440092608?itemId=19344788853&vendorItemId=86458206298&sourceType=cmgoms&omsPageId=113731&omsPageUrl=113731&isAddedCart="
driver.get(url)

# 페이지 로딩 대기
time.sleep(2)

# 스크롤을 끝까지 내리기
for _ in tqdm(range(10), desc="스크롤 진행 중"):
    driver.find_element_by_tag_name('body').send_keys(Keys.END)
    time.sleep(2)  # 각 스크롤 후 대기 시간

# 페이지 소스 가져오기
soup = BeautifulSoup(driver.page_source, 'html.parser')

# 리뷰 가져오기
reviews = soup.find_all("div", class_="sdp-review__article__list__review__content")
for review in reviews:
    print(review.get_text(strip=True))

# 브라우저 닫기
driver.quit()
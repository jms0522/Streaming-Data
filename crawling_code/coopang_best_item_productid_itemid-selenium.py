import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # 서버 환경에서는 Headless 모드 사용 권장
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument('--disable-gpu')
    options.add_argument('--remote-debugging-port=9222')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

    # ChromeDriver를 현재 설치된 Chrome 브라우저 버전에 맞게 자동으로 설치
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

def fetch_item_and_vendor_ids(url):
    driver = setup_driver()
    driver.get(url)

    try:
        # 페이지 로딩 대기
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "discount-product-unit"))
        )

        # 해당 클래스의 모든 링크를 찾기
        product_elements = driver.find_elements(By.CLASS_NAME, "discount-product-unit")

        results = []
        for product in product_elements:
            link = product.find_element(By.TAG_NAME, "a").get_attribute('href')
            try:
                item_id = link.split('itemId=')[1].split('&')[0]
                vendor_item_id = link.split('vendorItemId=')[1].split('&')[0]
                results.append({'item_id': item_id, 'vendor_item_id': vendor_item_id})
                print(f"Item ID: {item_id}, Vendor Item ID: {vendor_item_id}")
            except IndexError:
                continue

        return results
    except Exception as e:
        print(f"크롤링 중 오류가 발생했습니다: {str(e)}")
        return []
    finally:
        driver.quit()

def main():
    url = 'https://pages.coupang.com/p/113731'
    results = fetch_item_and_vendor_ids(url)

    if results:
        print(f"\n총 {len(results)}개의 Item ID와 Vendor Item ID가 추출되었습니다.")
    else:
        print("Item ID와 Vendor Item ID를 추출하는 데 실패했습니다.")

if __name__ == "__main__":
    main()
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# 특정 제품의 URL
url = 'https://www.coupang.com/vp/products/5877809049?itemId=10301141729&vendorItemId=77583447249&sourceType=cmgoms&omsPageId=113731&omsPageUrl=113731&isAddedCart='

# 웹 페이지 요청 및 BeautifulSoup 파싱
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# 모든 리뷰(article 태그)를 찾기ㅋ
review_articles = soup.find_all('article', class_='sdp-review__article__list')

# 리뷰 정보를 저장할 리스트
reviews = []

# 리뷰 파싱 및 진행률 표시
for article in tqdm(review_articles, desc="Processing reviews"):
    # 사용자 이름과 ID
    user_info = article.find('span', class_='sdp-review__article__list__info__user__name')
    user_name = user_info.get_text(strip=True)
    user_id = user_info['data-member-id']

    # 제품 이름
    product_name = article.find('div', class_='sdp-review__article__list__info__product-info__name').get_text(strip=True)

    # 리뷰 내용
    review_content = article.find('div', class_='sdp-review__article__list__review__content').get_text(strip=True)

    # 도움이 된 사람 수
    helpful_count = article.find('span', class_='js_reviewArticleHelpfulCount').get_text(strip=True)

    # 리뷰 정보를 딕셔너리로 저장
    review_data = {
        "user_id": user_id,
        "user_name": user_name,
        "product_name": product_name,
        "review_content": review_content,
        "helpful_count": helpful_count
    }
    reviews.append(review_data)

# 결과 출력
for review in reviews:
    print(review)
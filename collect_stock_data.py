import os
import requests
from datetime import datetime
from airtable import Airtable
import time

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def make_api_request(url, params=None, max_retries=3, delay=1):
    """API 요청 처리 (재시도 로직 포함)"""
    for attempt in range(max_retries):
        try:
            if params:
                response = requests.get(url, params=params)
            else:
                response = requests.get(url)
            
            response.raise_for_status()
            time.sleep(delay)  # API 요청 사이에 딜레이
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"시도 {attempt + 1}/{max_retries} 실패: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 점진적으로 대기 시간 증가
                print(f"{wait_time}초 후 재시도...")
                time.sleep(wait_time)
            else:
                print("최대 재시도 횟수 초과")
                return None

def get_stock_data(category):
    """카테고리별 주식 데이터 가져오기"""
    base_url = "https://financialmodelingprep.com/api/v3"
    
    if category == "전일대비등락률상위":
        url = f"{base_url}/stock-screener"
        params = {
            'priceMoreThan': 5,
            'volumeMoreThan': 1000000,
            'marketCapMoreThan': 100000000,
            'isActivelyTrading': 'true',
            'apikey': FMP_API_KEY
        }
        data = make_api_request(url, params=params)
        
        if data:
            # 등락률 3% 이상 필터링
            data = [stock for stock in data if float(stock.get('changesPercentage', 0)) >= 3]
            data = sorted(data, key=lambda x: float(x.get('changesPercentage', 0)), reverse=True)
            
    elif category == "거래대금상위":
        url = f"{base_url}/stock_market/actives"
        params = {'apikey': FMP_API_KEY}
        data = make_api_request(url, params=params)
        
    elif category == "시가총액상위":
        url = f"{base_url}/stock-screener"
        params = {
            'marketCapMoreThan': 10000000000,
            'limit': 100,
            'apikey': FMP_API_KEY
        }
        data = make_api_request(url, params=params)
        
    elif category == "52주신고가":
        url = f"{base_url}/stock_market/52_week_high"
        params = {'apikey': FMP_API_KEY}
        data = make_api_request(url, params=params)
    
    else:
        print(f"지원하지 않는 카테고리: {category}")
        return None

    if data:
        print(f"{category}: {len(data)}개 종목 데이터 수집")
        return data
    return None

def update_airtable(stock_data, category):
    """Airtable에 데이터 업데이트"""
    if not stock_data:
        return
    
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = {
        'date': datetime.now().strftime("%Y-%m-%d")
    }
    
    for stock in stock_data:
        try:
            record = {
                '티커': stock.get('symbol', ''),
                '종목명': stock.get('companyName', stock.get('name', '')),
                '현재가': float(stock.get('price', 0)),
                '등락률': float(stock.get('changesPercentage', 0)),
                '거래량': int(stock.get('volume', 0)),
                '시가총액': float(stock.get('marketCap', 0)),
                '거래소 정보': stock.get('exchange', ''),
                '업데이트 시간': current_date,
                '분류': category
            }
            
            if not record['티커']:
                print(f"필수 필드 누락: {stock}")
                continue
                
            existing_records = airtable.search('티커', record['티커'])
            
            if existing_records:
                airtable.update(existing_records[0]['id'], record)
                print(f"데이터 업데이트 완료: {record['티커']} ({category})")
            else:
                airtable.insert(record)
                print(f"새 데이터 추가 완료: {record['티커']} ({category})")
                
            time.sleep(0.2)  # Airtable API 요청 사이에 약간의 딜레이
                
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('symbol', 'Unknown')}): {str(e)}")

def main():
    print("데이터 수집 시작...")
    
    categories = [
        "전일대비등락률상위",
        "거래대금상위",
        "시가총액상위",
        "52주신고가"
    ]
    
    for category in categories:
        print(f"\n{category} 데이터 수집 중...")
        stock_data = get_stock_data(category)
        if stock_data:
            update_airtable(stock_data, category)
        else:
            print(f"{category} 데이터 수집 실패")
    
    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

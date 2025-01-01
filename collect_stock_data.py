import os
import requests
from datetime import datetime
from airtable import Airtable

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_stock_data(category):
    """카테고리별 주식 데이터 가져오기"""
    base_url = "https://financialmodelingprep.com/api/v3"
    
    if category == "전일대비등락률상위":
        # 스크리너 조건 설정
        url = f"{base_url}/stock-screener"
        params = {
            'priceMoreThan': 5,  # 현재가 5달러 이상
            'volumeMoreThan': 1000000,  # 거래량 100만주 이상
            'marketCapMoreThan': 100000000,  # 시가총액 1억 달러 이상
            'betaMoreThan': 3,  # 등락률 3% 이상 (베타값 사용)
            'isActivelyTrading': 'true',  # 활발한 거래 중인 종목만
            'apikey': FMP_API_KEY
        }
        response = requests.get(url, params=params)
        
    elif category == "거래대금상위":
        url = f"{base_url}/stock_market/actives?apikey={FMP_API_KEY}"
        response = requests.get(url)
        
    elif category == "시가총액상위":
        url = f"{base_url}/stock-screener?marketCapMoreThan=10000000000&limit=100&apikey={FMP_API_KEY}"
        response = requests.get(url)
        
    elif category == "52주신고가":
        url = f"{base_url}/stock_market/52_week_high?apikey={FMP_API_KEY}"
        response = requests.get(url)
    
    else:
        print(f"지원하지 않는 카테고리: {category}")
        return None
    
    try:
        response.raise_for_status()
        data = response.json()
        
        # 전일대비등락률상위의 경우 추가 필터링
        if category == "전일대비등락률상위":
            data = [stock for stock in data if float(stock.get('changesPercentage', 0)) >= 3]
            # 등락률 기준으로 정렬
            data = sorted(data, key=lambda x: float(x.get('changesPercentage', 0)), reverse=True)
        
        print(f"{category}: {len(data)}개 종목 데이터 수집")
        return data
    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 에러 발생: {str(e)}")
        return None
    except Exception as e:
        print(f"데이터 처리 중 에러 발생: {str(e)}")
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
    
    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

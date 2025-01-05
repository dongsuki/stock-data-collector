import os
import requests
from datetime import datetime
from airtable import Airtable
import time

# API 키와 Airtable 테이블 정보
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"  # Polygon API Key
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def convert_exchange_code(mic):
    """거래소 코드를 읽기 쉬운 형태로 변환"""
    exchange_map = {
        'XNAS': 'NASDAQ',
        'XNYS': 'NYSE',
        'XASE': 'AMEX'
    }
    return exchange_map.get(mic, mic)

def get_all_stocks():
    """모든 주식 데이터 가져오기"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {
        'apiKey': POLYGON_API_KEY,
        'include_otc': False
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            print(f"전체 데이터 샘플:", data['tickers'][0] if data.get('tickers') else 'No data')
            return data.get('tickers', [])
        else:
            print(f"API 요청 실패: {response.status_code}")
            return []
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def calculate_top_market_cap(stocks):
    """시가총액 상위 20개 계산"""
    stocks_with_details = []
    
    for stock in stocks:
        ticker = stock.get('ticker', '')
        details = get_stock_details(ticker)
        
        if details and details.get('market_cap'):
            stock['market_cap'] = float(details.get('market_cap', 0))
            stock['company_name'] = details.get('name', 'Unknown')
            stock['primary_exchange'] = details.get('primary_exchange', '')
            stocks_with_details.append(stock)
        
        time.sleep(0.2)  # API 속도 제한 준수

    # 시가총액 기준으로 상위 20개 선택
    return sorted(stocks_with_details, key=lambda x: x.get('market_cap', 0), reverse=True)[:20]

def get_stock_details(ticker):
    """종목 상세정보 조회"""
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {'apiKey': POLYGON_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('results', {})
        print(f"상세정보 조회 실패 ({ticker}): {response.status_code}")
        return None
    except Exception as e:
        print(f"상세정보 조회 중 에러 발생 ({ticker}): {str(e)}")
        return None

def update_airtable(stock_data, category):
    """Airtable에 데이터 추가"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            day_data = stock.get('day', {})
            ticker = stock.get('ticker', '')

            record = {
                '티커': ticker,
                '종목명': stock.get('company_name', 'Unknown'),
                '현재가': float(day_data.get('c', 0)),
                '등락률': float(stock.get('todaysChangePerc', 0)),
                '거래량': int(day_data.get('v', 0)),
                '거래대금': float(day_data.get('c', 0)) * float(day_data.get('v', 0)),
                '시가총액': float(stock.get('market_cap', 0)),
                '거래소 정보': convert_exchange_code(stock.get('primary_exchange', '')),
                '업데이트 시간': current_date,
                '분류': category
            }
            
            airtable.insert(record)
            print(f"새 데이터 추가 완료: {record['티커']} ({category})")
            
            time.sleep(0.2)
            
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
    print("데이터 수집 시작...")
    
    all_stocks = get_all_stocks()
    if not all_stocks:
        print("데이터 수집 실패")
        return
    
    print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")

    # 시가총액 상위 20개 계산
    top_market_cap = calculate_top_market_cap(all_stocks)
    print(f"\n시가총액 상위 20개 데이터 선정 완료")
    update_airtable(top_market_cap, "시가총액 상위")

    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

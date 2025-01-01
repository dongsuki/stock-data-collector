import os
import requests
from datetime import datetime
from airtable import Airtable
import time

POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_ticker_details(ticker):
    """종목 상세정보 조회"""
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {'apiKey': POLYGON_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('results', {})
        return None
    except:
        return None

def filter_stocks(stocks):
    """주식 데이터 필터링"""
    filtered = []
    for stock in stocks:
        if 'W' in stock.get('ticker', ''):  # 워런트 제외
            continue
            
        day_data = stock.get('day', {})
        price = float(day_data.get('c', 0))
        volume = int(day_data.get('v', 0))
        
        if price >= 5 and volume >= 1000000:  # 현재가 5달러 이상, 거래량 100만주 이상
            filtered.append(stock)
    
    return filtered

def get_gainers_data():
    """전일대비등락률상위 데이터"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
    params = {'apiKey': POLYGON_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            all_data = response.json().get('tickers', [])
            filtered_data = filter_stocks(all_data)
            
            # 상세 정보 추가
            enriched_data = []
            for stock in filtered_data:
                details = get_ticker_details(stock['ticker'])
                if details:
                    stock['name'] = details.get('name', '')
                    stock['market_cap'] = details.get('market_cap', 0)
                    stock['primary_exchange'] = details.get('primary_exchange', '')
                enriched_data.append(stock)
            
            if enriched_data:
                print("샘플 데이터:", enriched_data[0])
            return enriched_data
        
        print(f"API 요청 실패: {response.status_code}")
        return []
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def update_airtable(stock_data, category):
    """Airtable에 데이터 업데이트"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            day_data = stock.get('day', {})
            
            record = {
                '티커': stock.get('ticker', ''),
                '종목명': stock.get('name', ''),
                '현재가': float(day_data.get('c', 0)),
                '등락률': float(stock.get('todaysChangePerc', 0)),
                '거래량': int(day_data.get('v', 0)),
                '시가총액': float(stock.get('market_cap', 0)),
                '업데이트 시간': current_date,
                '분류': category
            }
            
            # 거래소 정보가 있는 경우에만 추가
            if stock.get('primary_exchange'):
                record['거래소 정보'] = stock['primary_exchange']
            
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
            
            time.sleep(0.2)
            
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
    print("데이터 수집 시작...")
    
    print("\n전일대비등락률상위 데이터 수집 중...")
    stock_data = get_gainers_data()
    
    if stock_data:
        print(f"{len(stock_data)}개 종목 데이터 수집됨")
        update_airtable(stock_data, "전일대비등락률상위")
    else:
        print("데이터 수집 실패")

    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

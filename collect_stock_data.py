import os
import requests
from datetime import datetime
from airtable import Airtable
import time

POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_gainers_data():
    """전일대비등락률상위 데이터"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
    params = {
        'apiKey': POLYGON_API_KEY
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        print("샘플 데이터:", response.json()['tickers'][0] if response.json().get('tickers') else 'No data')
        return response.json().get('tickers', [])
    else:
        print(f"API 요청 실패: {response.status_code}, {response.text}")
        return []

def update_airtable(stock_data, category):
    """Airtable에 데이터 업데이트"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            day_data = stock.get('day', {})
            last_trade = stock.get('lastTrade', {})
            
            record = {
                '티커': stock.get('ticker', ''),
                '종목명': stock.get('name', ''),
                '현재가': float(last_trade.get('p', 0)),
                '등락률': float(stock.get('todaysChangePerc', 0)),
                '거래량': int(day_data.get('v', 0)),
                '시가총액': float(stock.get('marketCap', 0)),
                '거래소 정보': stock.get('market', ''),
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

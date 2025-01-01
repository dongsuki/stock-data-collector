import os
import requests
from datetime import datetime
from airtable import Airtable

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_stock_data():
    """FMP API로부터 주식 데이터 가져오기"""
    url = f"https://financialmodelingprep.com/api/v3/stock_market/actives?apikey={FMP_API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print(f"First stock data sample: {data[0] if data else 'No data'}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 에러 발생: {str(e)}")
        return None

def update_airtable(stock_data):
    """Airtable에 데이터 업데이트"""
    if not stock_data:
        return
    
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    # Airtable 형식에 맞는 날짜 문자열 생성 (YYYY-MM-DD)
    current_time = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            record = {
                '티커': stock.get('symbol', ''),
                '종목명': stock.get('name', ''),
                '현재가': float(stock.get('price', 0)),
                '등락률': float(stock.get('changesPercentage', 0)),
                '거래량': int(stock.get('volume', 0)),
                '시가총액': float(stock.get('marketCap', 0)),
                '거래소 정보': stock.get('exchange', ''),
                '업데이트 시간': current_time
            }
            
            # 필수 필드 확인
            if not record['티커']:
                print(f"필수 필드 누락: {stock}")
                continue
                
            # 기존 레코드 검색
            existing_records = airtable.search('티커', record['티커'])
            
            if existing_records:
                # 기존 레코드 업데이트
                airtable.update(existing_records[0]['id'], record)
                print(f"데이터 업데이트 완료: {record['티커']}")
            else:
                # 새 레코드 생성
                airtable.insert(record)
                print(f"새 데이터 추가 완료: {record['티커']}")
                
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('symbol', 'Unknown')}): {str(e)}")

def main():
    print("데이터 수집 시작...")
    stock_data = get_stock_data()
    
    if stock_data:
        print(f"{len(stock_data)}개의 종목 데이터 수집됨")
        update_airtable(stock_data)
        print("모든 데이터 처리 완료!")
    else:
        print("데이터 수집 실패")

if __name__ == "__main__":
    main()

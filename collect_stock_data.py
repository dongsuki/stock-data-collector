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

def get_all_stock_data():
    """FMP API로부터 모든 주식 데이터 가져오기"""
    # 모든 상장 주식 목록 가져오기
    url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={FMP_API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        stock_list = response.json()
        
        # 미국 주식만 필터링 (symbol이 영문자로만 구성된 종목)
        us_stocks = [stock for stock in stock_list if stock['symbol'].isalpha()]
        print(f"총 {len(us_stocks)}개의 미국 주식 발견")
        
        # 각 주식의 상세 정보 가져오기
        detailed_stocks = []
        for i, stock in enumerate(us_stocks, 1):
            quote_url = f"https://financialmodelingprep.com/api/v3/quote/{stock['symbol']}?apikey={FMP_API_KEY}"
            try:
                quote_response = requests.get(quote_url)
                quote_response.raise_for_status()
                quote_data = quote_response.json()
                
                if quote_data:  # 데이터가 있는 경우만 추가
                    detailed_stocks.append(quote_data[0])
                    print(f"진행 중: {i}/{len(us_stocks)} - {stock['symbol']}")
                
                # API 속도 제한 준수
                if i % 10 == 0:  # 10개 요청마다
                    time.sleep(1)  # 1초 대기
                    
            except Exception as e:
                print(f"종목 {stock['symbol']} 데이터 가져오기 실패: {str(e)}")
                continue
        
        return detailed_stocks
        
    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 에러 발생: {str(e)}")
        return None

def update_airtable(stock_data):
    """Airtable에 데이터 업데이트"""
    if not stock_data:
        return
    
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    
    # Airtable Date 형식에 맞춰 날짜 문자열 생성
    current_date = {
        'date': datetime.now().strftime("%Y-%m-%d")
    }
    
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
                '업데이트 시간': current_date
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
    stock_data = get_all_stock_data()
    
    if stock_data:
        print(f"{len(stock_data)}개의 종목 데이터 수집됨")
        update_airtable(stock_data)
        print("모든 데이터 처리 완료!")
    else:
        print("데이터 수집 실패")

if __name__ == "__main__":
    main()

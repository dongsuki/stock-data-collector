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

def calculate_top_traded_value(stocks):
    """거래대금 상위 20개 계산"""
    for stock in stocks:
        day_data = stock.get('day', {})
        close_price = float(day_data.get('c', 0))  # 현재가
        volume = int(day_data.get('v', 0))  # 거래량
        traded_value = close_price * volume  # 거래대금
        stock['traded_value'] = traded_value

    # 거래대금을 기준으로 상위 20개 선택
    return sorted(stocks, key=lambda x: x.get('traded_value', 0), reverse=True)[:20]

def update_airtable(stock_data, category):
    """Airtable에 데이터 추가"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            day_data = stock.get('day', {})
            
            # 기본 데이터
            close_price = float(day_data.get('c', 0))
            volume = int(day_data.get('v', 0))
            traded_value = stock.get('traded_value', 0)
            todays_change_perc = stock.get('todaysChangePerc', 0)

            # 기본 데이터만으로 저장
            record = {
                '티커': stock.get('ticker', ''),
                '종목명': stock.get('name', 'Unknown'),  # 기본값 'Unknown'
                '현재가': close_price,
                '등락률': todays_change_perc,
                '거래량': volume,
                '거래대금': traded_value,
                '시가총액': stock.get('market_cap', 0),  # market_cap 추가
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

    # 거래대금 상위 20개 계산
    top_traded_value = calculate_top_traded_value(all_stocks)
    print(f"\n거래대금 상위 20개 데이터 선정 완료")
    update_airtable(top_traded_value, "거래대금 상위")

    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

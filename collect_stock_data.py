import requests
from pyairtable import Table
from datetime import datetime
import time

POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def fetch_all_tickers():
    """전체 스냅샷 데이터 가져오기"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {'apiKey': POLYGON_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "OK":
            return data.get("tickers", [])
        else:
            print("API 에러:", data.get("message", "알 수 없는 오류"))
            return []
    except Exception as e:
        print(f"데이터 수집 에러: {e}")
        return []

def screen_stocks(stocks, price=5, volume=1_000_000, change=3):
    """스크리닝 조건에 맞는 주식 필터링"""
    screened_stocks = []
    for stock in stocks:
        day_data = stock.get('day', {})
        stock_price = float(day_data.get('c', 0))  # 종가
        stock_volume = int(day_data.get('v', 0))  # 거래량
        stock_change = stock.get('todaysChangePerc', 0)
        
        if stock_price >= price and stock_volume >= volume and stock_change >= change:
            screened_stocks.append({
                "ticker": stock['ticker'],
                "price": stock_price,
                "volume": stock_volume,
                "change": stock_change,
                "exchange": stock.get('primaryExchange', ''),
                "name": stock.get('name', '')
            })
    # 등락률 기준 내림차순 정렬
    screened_stocks = sorted(screened_stocks, key=lambda x: x['change'], reverse=True)
    return screened_stocks

def update_airtable(screened_stocks):
    """Airtable에 데이터 업데이트"""
    table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, TABLE_NAME)
    
    for stock in screened_stocks:
        try:
            record = {
                '티커': stock['ticker'],
                '현재가': stock['price'],
                '거래량': stock['volume'],
                '등락률': stock['change'],
                '거래소 정보': stock['exchange'],
                '종목명': stock['name'],
                '업데이트 시간': datetime.now().isoformat(),  # ISO 8601 형식으로 변환
                '분류': "스크리닝"
            }
            
            # 모든 레코드 가져오기
            records = table.all(view='Grid view')
            existing_record = next((r for r in records if r['fields'].get('티커') == record['티커']), None)
            
            if existing_record:
                table.update(existing_record['id'], record)
                print(f"데이터 업데이트 완료: {record['티커']}")
            else:
                table.create(record)
                print(f"새 데이터 추가 완료: {record['티커']}")
            
            time.sleep(0.2)  # API 호출 제한 방지
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
    print("전체 주식 데이터 가져오는 중...")
    all_tickers = fetch_all_tickers()
    
    if all_tickers:
        print(f"총 {len(all_tickers)}개의 종목 데이터 가져옴.")
        
        print("스크리닝 조건: 종가 >= 5달러, 거래량 >= 100만, 등락률 >= 3%")
        screened = screen_stocks(all_tickers, price=5, volume=1_000_000, change=3)
        
        print(f"조건에 맞는 종목: {len(screened)}개")
        update_airtable(screened)
    else:
        print("데이터를 가져오지 못했습니다.")

if __name__ == "__main__":
    main()

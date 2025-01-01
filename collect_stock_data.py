import requests
from airtable import Airtable
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

def screen_stocks(stocks, min_price=5, min_volume=1_000_000):
    """스크리닝 조건에 맞는 주식 필터링"""
    screened_stocks = []
    for stock in stocks:
        day_data = stock.get('day', {})
        price = float(day_data.get('c', 0))  # 종가
        volume = int(day_data.get('v', 0))  # 거래량
        
        if price >= min_price and volume >= min_volume:
            screened_stocks.append({
                "ticker": stock['ticker'],
                "price": price,
                "volume": volume
            })
    return screened_stocks

def update_airtable(screened_stocks):
    """Airtable에 데이터 업데이트"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    
    for stock in screened_stocks:
        try:
            record = {
                '티커': stock['ticker'],
                '현재가': stock['price'],
                '거래량': stock['volume']
            }
            
            existing_records = airtable.search('티커', record['티커'])
            
            if existing_records:
                airtable.update(existing_records[0]['id'], record)
                print(f"데이터 업데이트 완료: {record['티커']}")
            else:
                airtable.insert(record)
                print(f"새 데이터 추가 완료: {record['티커']}")
            
            time.sleep(0.2)  # API 호출 제한 방지
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
    print("전체 주식 데이터 가져오는 중...")
    all_tickers = fetch_all_tickers()
    
    if all_tickers:
        print(f"총 {len(all_tickers)}개의 종목 데이터 가져옴.")
        
        print("스크리닝 조건: 종가 >= 5달러, 거래량 >= 100만")
        screened = screen_stocks(all_tickers, min_price=5, min_volume=1_000_000)
        
        print(f"조건에 맞는 종목: {len(screened)}개")
        update_airtable(screened)
    else:
        print("데이터를 가져오지 못했습니다.")

if __name__ == "__main__":
    main()

import os
import requests
from datetime import datetime, timedelta
from airtable import Airtable
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# API 설정
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
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

def get_52_week_high(ticker):
   """Polygon.io Aggregates API를 사용하여 52주 신고가와 저가 계산"""
   end_date = datetime.now()
   start_date = end_date - timedelta(weeks=52)
   url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
   params = {
       'adjusted': 'true',
       'sort': 'desc',
       'limit': 365,
       'apiKey': POLYGON_API_KEY
   }

   try:
       response = requests.get(url, params=params)
       response.raise_for_status()
       data = response.json()

       if data.get('status') != 'OK':
           print(f"API 응답 에러 - {ticker}: {data.get('message', 'Unknown error')}")
           return {'high': 0.0, 'avg_volume': 0}

       if not data.get('results'):
           return {'high': 0.0, 'avg_volume': 0}

       high_prices = [day['h'] for day in data['results']]
       volume = sum(day['v'] for day in data['results'][-30:]) / 30  # 30일 평균 거래량
       
       return {'high': max(high_prices), 'avg_volume': volume}
   except requests.exceptions.RequestException as e:
       print(f"API 요청 실패 - {ticker}: {str(e)}")
       return {'high': 0.0, 'avg_volume': 0}

def calculate_52_week_high_parallel(tickers):
   """병렬 처리로 52주 신고가 계산"""
   results = {}
   with ThreadPoolExecutor(max_workers=10) as executor:
       future_to_ticker = {executor.submit(get_52_week_high, ticker): ticker for ticker in tickers}
       for future in as_completed(future_to_ticker):
           ticker = future_to_ticker[future]
           try:
               results[ticker] = future.result()
               print(f"{ticker}: 52주 신고가 계산 완료")
           except Exception as e:
               print(f"{ticker}: 계산 실패 - {e}")
               results[ticker] = {'high': 0.0, 'avg_volume': 0}
   return results

def filter_52_week_high_stocks(stocks):
   """52주 신고가 근접 주식 필터링"""
   filtered = []
   min_price = 10.0  # 최소 주가 10달러
   min_volume = 1000000  # 최소 거래량 100만주 
   high_threshold = 0.95  # 신고가의 95% 이상
   min_market_cap = 500000000  # 최소 시가총액 5억달러

   tickers = [stock['ticker'] for stock in stocks]
   high_values = calculate_52_week_high_parallel(tickers)

   for stock in stocks:
       ticker = stock['ticker']
       day_data = stock.get('day', {})
       current_price = float(day_data.get('c', 0))
       volume = float(day_data.get('v', 0))
       market_cap = float(stock.get('market_cap', 0))

       high_data = high_values.get(ticker, {'high': 0.0, 'avg_volume': 0})
       high_52_week = high_data['high']
       avg_volume = high_data['avg_volume']

       if (high_52_week > 0 and 
           current_price >= high_52_week * high_threshold and
           current_price >= min_price and
           volume >= min_volume and
           avg_volume >= min_volume and
           market_cap >= min_market_cap):

           stock.update({
               '52_week_high': high_52_week,
               'price_to_high_ratio': current_price / high_52_week if high_52_week > 0 else 0,
               'avg_volume': avg_volume
           })
           filtered.append(stock)

   return sorted(filtered, key=lambda x: x['price_to_high_ratio'], reverse=True)

def format_number(value):
   """숫자 포맷팅 (K, M, B 단위 변환)"""
   if value >= 1_000_000_000:
       return f"{value/1_000_000_000:.2f}B"
   elif value >= 1_000_000:
       return f"{value/1_000_000:.2f}M"
   elif value >= 1_000:
       return f"{value/1_000:.2f}K"
   return f"{value:.2f}"

def update_airtable(stock_data, category):
   """Airtable에 데이터 추가 및 업데이트"""
   airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
   current_date = datetime.now().strftime("%Y-%m-%d")

   existing_records = {record['fields'].get('티커'): record['id'] 
                      for record in airtable.get_all()}

   for stock in stock_data:
       try:
           day_data = stock.get('day', {})
           record = {
               '티커': stock.get('ticker', ''),
               '종목명': stock.get('name', ''),
               '현재가': float(day_data.get('c', 0)),
               '52주 신고가': float(stock.get('52_week_high', 0)),
               '신고가대비': f"{stock.get('price_to_high_ratio', 0) * 100:.1f}%",
               '거래량': format_number(int(day_data.get('v', 0))),
               '평균거래량': format_number(int(stock.get('avg_volume', 0))),
               '시가총액': format_number(float(stock.get('market_cap', 0))),
               '업데이트 시간': current_date,
               '분류': category,
               '거래소': convert_exchange_code(stock.get('primary_exchange', ''))
           }

           if stock['ticker'] in existing_records:
               airtable.update(existing_records[stock['ticker']], record)
               print(f"데이터 업데이트: {record['티커']}")
           else:
               airtable.insert(record)
               print(f"새 데이터 추가: {record['티커']}")
           
           time.sleep(0.2)

       except Exception as e:
           print(f"레코드 처리 오류 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
   print("데이터 수집 시작...")

   url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
   params = {
       'apiKey': POLYGON_API_KEY, 
       'include_otc': False,
       'market': 'stocks',
       'active': True
   }

   try:
       response = requests.get(url, params=params)
       response.raise_for_status()
       all_stocks = response.json().get('tickers', [])

       print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")

       filtered_stocks = filter_52_week_high_stocks(all_stocks)
       print(f"\n조건을 만족하는 종목 수: {len(filtered_stocks)}개")

       if filtered_stocks:
           update_airtable(filtered_stocks, "52주 신고가 상위")

       print("\n데이터 처리 완료!")

   except Exception as e:
       print(f"데이터 수집 중 에러 발생: {e}")

if __name__ == "__main__":
   main()

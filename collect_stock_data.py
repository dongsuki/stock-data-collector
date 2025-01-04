import os
import requests
from datetime import datetime
from airtable import Airtable
import time

FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_all_us_stocks():
    """
    모든 거래 가능 종목 중 (NASDAQ, NYSE, AMEX) 종목만 필터링해서 반환
    """
    url = "https://financialmodelingprep.com/api/v3/available-traded/list"
    params = {'apikey': FMP_API_KEY}
    try:
        response = requests.get(url, params=params)
        print(f"[get_all_us_stocks] 전체 종목 데이터 수집: {response.status_code}")
        if response.status_code == 200:
            stocks = response.json()
            # 미국 주식만 필터링 (NASDAQ, NYSE, AMEX)
            us_stocks = [stock for stock in stocks if stock.get('exchangeShortName') in ['NASDAQ', 'NYSE', 'AMEX']]
            print(f"[get_all_us_stocks] 미국 주식 종목 수: {len(us_stocks)}")
            return us_stocks
    except Exception as e:
        print(f"[get_all_us_stocks] 전체 종목 데이터 수집 중 에러: {str(e)}")
    return []

def get_quotes_in_batch(symbols, chunk_size=100):
    """
    심볼 리스트를 일정량(chunk_size)으로 나눈 뒤,
    'Batch Quote API'를 통해 한 번에 시세를 가져옴.
    
    :param symbols: ['AAPL', 'MSFT', 'GOOGL', ...]
    :param chunk_size: 한 번에 조회할 종목 수
    :return: { "AAPL": { ...quote... }, "MSFT": { ...quote... } ... } 형태의 딕셔너리
    """
    all_quotes = []
    
    # 심볼을 chunk_size 만큼 잘라가며 조회
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i+chunk_size]
        chunk_str = ",".join(chunk)
        
        url = f"https://financialmodelingprep.com/api/v3/quote/{chunk_str}"
        params = {'apikey': FMP_API_KEY}
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                all_quotes.extend(data)
            else:
                print(f"[get_quotes_in_batch] 상태코드: {response.status_code}, 심볼: {chunk_str}")
            # 너무 빠르게 연속 호출을 하지 않도록 sleep (필요 시 조정)
            time.sleep(0.2)
        except Exception as e:
            print(f"[get_quotes_in_batch] 배치 조회 중 에러 (심볼: {chunk_str}): {str(e)}")
    
    # 종목별로 접근할 수 있도록 {symbol: quote_dict} 형태로 변환
    quote_map = {}
    for quote in all_quotes:
        symbol = quote.get('symbol')
        if symbol:
            quote_map[symbol] = quote

    return quote_map

def filter_gainers(quote_map):
    """
    전일대비등락률 상위 (등락률 5%↑, 현재가 $5↑, 거래량 100만주↑) - 갯수 제한 없음
    """
    results = []
    for symbol, quote in quote_map.items():
        price = float(quote.get('price', 0))
        volume = float(quote.get('volume', 0))
        changes_percentage = float(quote.get('changesPercentage', 0))
        
        if price >= 5 and volume >= 1_000_000 and changes_percentage >= 5:
            results.append(quote)
    # 등락률 내림차순 정렬
    results.sort(key=lambda x: float(x.get('changesPercentage', 0)), reverse=True)
    return results

def filter_volume_leaders(quote_map):
    """
    거래대금상위 (현재가 $5↑, 상위 20개)
    거래대금 = price * volume
    """
    valid_stocks = []
    for symbol, quote in quote_map.items():
        price = float(quote.get('price', 0))
        volume = float(quote.get('volume', 0))
        if price >= 5:
            trading_value = price * volume
            quote['trading_value'] = trading_value
            valid_stocks.append(quote)
    # 거래대금 내림차순 정렬 후 상위 20개
    valid_stocks.sort(key=lambda x: x.get('trading_value', 0), reverse=True)
    return valid_stocks[:20]

def filter_market_cap_leaders(quote_map):
    """
    시가총액 상위 (현재가 $5↑, 거래량 100만주↑, 상위 20개)
    """
    valid_stocks = []
    for symbol, quote in quote_map.items():
        price = float(quote.get('price', 0))
        volume = float(quote.get('volume', 0))
        market_cap = float(quote.get('marketCap', 0))
        
        if price >= 5 and volume >= 1_000_000:
            valid_stocks.append(quote)
    # 시총 내림차순 정렬 후 상위 20개
    valid_stocks.sort(key=lambda x: float(x.get('marketCap', 0)), reverse=True)
    return valid_stocks[:20]

def filter_52_week_high(quote_map):
    """
    52주 신고가 (현재가 $5↑, 거래량 100만주↑, 현재가가 52주 고가의 95% 이상, 상위 20개)
    """
    high_stocks = []
    for symbol, quote in quote_map.items():
        try:
            price = float(quote.get('price', 0))
            year_high = float(quote.get('yearHigh', 0))
            volume = float(quote.get('volume', 0))
            
            if (price >= 5 and
                volume >= 1_000_000 and
                year_high > 0 and
                price >= year_high * 0.95):
                high_stocks.append(quote)
        except:
            continue
    
    # 52주 고가 대비 현재가 비율이 높은 순(= 신고가 가까운 순)
    high_stocks.sort(key=lambda x: (float(x.get('price', 0)) / float(x.get('yearHigh', 1))), reverse=True)
    return high_stocks[:20]

def update_airtable(stock_data, category):
    """
    Airtable에 데이터 추가
    """
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n[update_airtable] {category} 데이터 Airtable 업데이트 시작")
    print(f"업데이트할 종목 수: {len(stock_data)}")

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
                '업데이트 시간': current_date,
                '분류': category
            }

            print(f" - {record['티커']} ({category}) 업로드...")
            airtable.insert(record)
            time.sleep(0.05)  # Airtable API 너무 과도 호출 안 되도록 조정
        except Exception as e:
            print(f"[update_airtable] 레코드 처리 중 에러: {stock.get('symbol', 'Unknown')} / {str(e)}")

def main():
    print("\n=== 미국 주식 데이터 수집 시작 ===")

    # 1) 전체 US 상장 주식 목록
    us_stocks = get_all_us_stocks()
    symbols = [s.get('symbol') for s in us_stocks if 'symbol' in s]

    # 2) Batch Quote API로 전체 심볼 시세 조회
    print("\n[Batch 시세 조회]")
    quote_map = get_quotes_in_batch(symbols, chunk_size=100)
    print(f"[main] 시세 데이터 수집 완료. 총 {len(quote_map)}건")

    # 3) 전일대비등락률상위
    print("\n[전일대비등락률상위]")
    gainers = filter_gainers(quote_map)
    update_airtable(gainers, "전일대비등락률상위")

    # 4) 거래대금상위
    print("\n[거래대금상위]")
    volume_leaders = filter_volume_leaders(quote_map)
    update_airtable(volume_leaders, "거래대금상위")

    # 5) 시가총액상위
    print("\n[시가총액상위]")
    market_cap_leaders = filter_market_cap_leaders(quote_map)
    update_airtable(market_cap_leaders, "시가총액상위")

    # 6) 52주 신고가
    print("\n[52주 신고가]")
    high_52 = filter_52_week_high(quote_map)
    update_airtable(high_52, "52주신고가")

    print("\n=== 모든 데이터 처리 완료 ===")

if __name__ == "__main__":
    main()

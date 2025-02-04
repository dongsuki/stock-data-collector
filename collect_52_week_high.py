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


def safe_float(value, default=0.0):
    """안전하게 float로 변환"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def get_delisted_stocks():
    """FMP API에서 상장폐지된 종목 목록 가져오기"""
    url = f"https://financialmodelingprep.com/api/v3/delisted-companies?apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return {item['symbol'] for item in response.json()}
    except Exception as e:
        print(f"❌ 상장폐지 종목 데이터 가져오기 실패: {str(e)}")
    return set()


def get_tradable_stocks():
    """현재 거래 가능한 종목 목록 가져오기"""
    url = f"https://financialmodelingprep.com/api/v3/available-traded/list?apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return {item['symbol'] for item in response.json()}
    except Exception as e:
        print(f"❌ 거래 가능한 종목 데이터 가져오기 실패: {str(e)}")
    return set()


def get_quotes():
    """미국 주식 데이터 가져오기"""
    print("📡 데이터 수집 시작...")

    # NASDAQ 데이터 수집
    url_nasdaq = f"https://financialmodelingprep.com/api/v3/quotes/nasdaq?apikey={FMP_API_KEY}"
    try:
        response = requests.get(url_nasdaq, timeout=30)
        nasdaq_stocks = response.json() if response.status_code == 200 else []
        print(f"📌 NASDAQ 종목 수집 완료: {len(nasdaq_stocks)}개")
    except Exception as e:
        print(f"❌ NASDAQ 데이터 수집 실패: {str(e)}")
        nasdaq_stocks = []

    # NYSE 데이터 수집
    url_nyse = f"https://financialmodelingprep.com/api/v3/quotes/nyse?apikey={FMP_API_KEY}"
    try:
        response = requests.get(url_nyse, timeout=30)
        nyse_stocks = response.json() if response.status_code == 200 else []
        print(f"📌 NYSE 종목 수집 완료: {len(nyse_stocks)}개")
    except Exception as e:
        print(f"❌ NYSE 데이터 수집 실패: {str(e)}")
        nyse_stocks = []

    all_stocks = nasdaq_stocks + nyse_stocks
    print(f"✅ 총 수집 종목 수: {len(all_stocks)}개")

    return all_stocks


def is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
    """실제 거래 가능한 미국 주식인지 확인"""
    symbol = stock.get('symbol', '')
    exchange = stock.get('exchange', '')
    type = stock.get('type', '').lower()
    name = stock.get('name', '') or ''  # None 방지
    name = name.lower()
    volume = safe_float(stock.get('volume'))
    price = safe_float(stock.get('price'))

    # ✅ 1. ETF 제외 (ETF 관련 키워드 포함된 경우)
    etf_keywords = ['etf', 'trust', 'fund']
    if 'etf' in type or any(keyword in name for keyword in etf_keywords):
        return False

    # ✅ 2. NYSE/NASDAQ이 아닌 경우 제외
    if exchange not in {'NYSE', 'NASDAQ'}:
        return False

    # ✅ 3. 상장폐지 종목 필터링
    if symbol in delisted_stocks:
        return False

    # ✅ 4. 현재 거래 가능한 종목 필터링
    if symbol not in tradable_stocks:
        return False

    # ✅ 5. 특수 증권 관련 키워드 체크
    invalid_keywords = [
        'warrant', 'warrants', 'adr', 'preferred', 'acquisition',
        'right', 'rights', 'merger', 'spac', 'unit', 'notes',
        'bond', 'series', 'class', 'holding', 'holdings', 'partners', 'management'
    ]
    if any(keyword in name for keyword in invalid_keywords):
        return False

    # ✅ 6. 거래 활성도 체크
    min_daily_dollar_volume = 1000000  # 최소 100만 달러 거래대금
    if price * volume < min_daily_dollar_volume:
        return False

    return True


def filter_stocks(stocks):
    """주식 필터링"""
    print("\n🔎 필터링 시작...")
    print(f"📌 필터링 전 종목 수: {len(stocks)}개")

    delisted_stocks = get_delisted_stocks()
    tradable_stocks = get_tradable_stocks()
    print(f"✅ 상장폐지 종목 수: {len(delisted_stocks)}개")
    print(f"✅ 현재 거래 가능한 종목 수: {len(tradable_stocks)}개")

    filtered = []
    for stock in stocks:
        if not is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
            continue

        price = safe_float(stock.get('price'))
        volume = safe_float(stock.get('volume'))
        yearHigh = safe_float(stock.get('yearHigh'))
        marketCap = safe_float(stock.get('marketCap'))

        price_to_high_ratio = (price / yearHigh) * 100
        change_percent = ((price - safe_float(stock.get('previousClose'))) / safe_float(stock.get('previousClose'))) * 100 if safe_float(stock.get('previousClose')) > 0 else 0

        if price >= 10 and volume >= 1000000 and marketCap >= 500000000 and price_to_high_ratio >= 95:
            filtered.append({
                'symbol': stock['symbol'],
                'price': price,
                'volume': volume,
                'yearHigh': yearHigh,
                'marketCap': marketCap,
                'name': stock['name'],
                'exchange': stock['exchange'],
                'price_to_high_ratio': price_to_high_ratio,
                'change_percent': change_percent
            })

    print(f"✅ 조건 만족 종목: {len(filtered)}개")
    return sorted(filtered, key=lambda x: x['price_to_high_ratio'], reverse=True)


def update_airtable(stocks):
    """Airtable에 새 레코드 추가"""
    print("\n📡 Airtable 업데이트 시작...")
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")

    for stock in stocks:
        record = {
            '티커': stock['symbol'],
            '종목명': stock['name'],
            '현재가': stock['price'],
            '등락률': stock['change_percent'],
            '거래량': stock['volume'],
            '시가총액': stock['marketCap'],
            '업데이트 시간': current_date,
            '분류': "52주_신고가_근접",
            '거래소 정보': stock['exchange'],
            '신고가 비율(%)': stock['price_to_high_ratio']
        }
        airtable.insert(record)
    print("✅ Airtable 업데이트 완료!")


def main():
    stocks = get_quotes()
    if stocks:
        filtered_stocks = filter_stocks(stocks)
        if filtered_stocks:
            update_airtable(filtered_stocks)


if __name__ == "__main__":
    main()

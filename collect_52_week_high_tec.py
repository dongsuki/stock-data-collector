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


# --- 기술적 필터링 관련 함수 추가 --- #

def get_moving_average(symbol, period):
    """
    주어진 종목(symbol)의 단순 이동평균(SMA) 값을 가져온다.
    일봉 데이터를 사용하며, 가장 최신의 이동평균 값을 반환한다.
    """
    url = f"https://financialmodelingprep.com/api/v3/technical_indicator/daily/{symbol}?type=sma&period={period}&apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data:
                # data는 최신순(내림차순)으로 정렬되어 있다고 가정
                return safe_float(data[0].get('value'))
        return None
    except Exception as e:
        print(f"Error fetching MA{period} for {symbol}: {e}")
        return None


def get_moving_average_history(symbol, period, days=30):
    """
    주어진 종목(symbol)의 단순 이동평균(SMA) 히스토리(최근 days일치)를 리스트로 반환한다.
    반환되는 리스트는 최신 데이터가 첫번째, 가장 오래된 데이터가 마지막에 위치.
    """
    url = f"https://financialmodelingprep.com/api/v3/technical_indicator/daily/{symbol}?type=sma&period={period}&limit={days}&apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data:
                return [safe_float(item.get('value')) for item in data if item.get('value') is not None]
        return []
    except Exception as e:
        print(f"Error fetching MA{period} history for {symbol}: {e}")
        return []


def passes_technical_filters(stock):
    """
    아래 기술적 필터링 조건을 모두 만족하는지 확인한다.
    1. 주가가 150일 및 200일 이평선 위에 있다.
    2. 150일 이평선이 200일 이평선 위에 있다.
    3. 200일 이평선은 최소 1개월 이상 상승 추세에 있다.
    4. 50일 이평선이 150일 이평선(또는 200일 이평선) 위에 있다.
    5. 현 주가가 50일 이평선 위에 있다.
    6. 현 주가가 52주 신저가보다 최소 30% 이상 높다.
    7. 현 주가가 52주 신고가 대비 75% 이상 위치한다. (즉, 52주 구간의 75% 이상에 거래되고 있다.)
    """
    symbol = stock.get('symbol')
    price = safe_float(stock.get('price'))
    yearHigh = safe_float(stock.get('yearHigh'))
    yearLow = safe_float(stock.get('yearLow'))
    if yearLow <= 0:
        return False

    # 조건 6: 현재 주가가 52주 신저가의 130% 미만이면 필터 탈락
    if price < 1.3 * yearLow:
        return False

    # 조건 7: (price - yearLow) / (yearHigh - yearLow) >= 0.75 인지 확인
    if yearHigh > yearLow:
        if (price - yearLow) / (yearHigh - yearLow) < 0.75:
            return False
    else:
        return False

    # 50, 150, 200일 이동평균 값 가져오기
    ma50 = get_moving_average(symbol, 50)
    ma150 = get_moving_average(symbol, 150)
    ma200 = get_moving_average(symbol, 200)

    if ma50 is None or ma150 is None or ma200 is None:
        return False

    # 조건 1: 주가가 150일 및 200일 이평선 위에 있어야 함
    if price <= ma150 or price <= ma200:
        return False

    # 조건 2: 150일 이평선 > 200일 이평선
    if ma150 <= ma200:
        return False

    # 조건 4: 50일 이평선이 150일 이평선보다 위에 있어야 함
    if ma50 <= ma150:
        return False

    # 조건 5: 주가가 50일 이평선 위에 있어야 함
    if price <= ma50:
        return False

    # 조건 3: 200일 이평선이 최소 1개월(약 30일) 이상 상승 추세인지 확인
    # 최근 30일간의 200일 이평선 데이터를 가져와서, 최신 값이 가장 오래된 값보다 높은지 확인
    ma200_history = get_moving_average_history(symbol, 200, days=30)
    if not ma200_history or len(ma200_history) < 2:
        return False
    # data는 최신순으로 정렬되어 있다고 가정: 인덱스 0은 최신, -1은 가장 오래된 값
    if ma200_history[0] <= ma200_history[-1]:
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

        price_to_high_ratio = (price / yearHigh) * 100 if yearHigh > 0 else 0
        change_percent = ((price - safe_float(stock.get('previousClose'))) / safe_float(stock.get('previousClose'))) * 100 if safe_float(stock.get('previousClose')) > 0 else 0

        if price >= 10 and volume >= 1000000 and marketCap >= 500000000 and price_to_high_ratio >= 95:
            # 기술적 필터링 조건 추가
            if not passes_technical_filters(stock):
                continue

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

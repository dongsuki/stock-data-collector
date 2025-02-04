import os
import requests
from datetime import datetime
from airtable import Airtable
import time

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"  # Airtable 테이블명

# Airtable 객체 생성
airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)


def safe_float(value, default=0.0):
    """안전하게 float로 변환"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


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


def get_moving_averages(symbol):
    """FMP API에서 이동평균선 데이터 가져오기"""
    url = f"https://financialmodelingprep.com/api/v3/technical_indicator/daily/{symbol}?type=sma&period=50,150,200&apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if len(data) > 0:
                return {
                    'sma50': safe_float(data[0].get('sma50')),
                    'sma150': safe_float(data[0].get('sma150')),
                    'sma200': safe_float(data[0].get('sma200'))
                }
    except Exception as e:
        print(f"❌ 이동평균 데이터 가져오기 실패 ({symbol}): {str(e)}")
    return None


def is_moving_average_uptrend(symbol, ma_type, days=30):
    """이동평균선이 일정 기간 동안 상승 추세인지 확인"""
    url = f"https://financialmodelingprep.com/api/v3/technical_indicator/daily/{symbol}?type={ma_type}&limit={days}&apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if len(data) >= days:
                first_value = safe_float(data[-1].get(ma_type))
                last_value = safe_float(data[0].get(ma_type))
                return last_value > first_value  # 최근 값이 과거 값보다 크면 상승 추세
    except Exception as e:
        print(f"❌ {ma_type} 상승 추세 확인 실패 ({symbol}): {str(e)}")
    return False


def filter_stocks(stocks):
    """주식 필터링"""
    print("\n🔎 필터링 시작...")
    delisted_stocks = get_delisted_stocks()
    tradable_stocks = get_tradable_stocks()

    filtered = [
        stock for stock in stocks if is_valid_us_stock(stock, delisted_stocks, tradable_stocks)
    ]
    print(f"✅ 조건 만족 종목: {len(filtered)}개")
    return filtered


def update_airtable(stocks):
    """Airtable에 새 레코드 추가"""
    print("\n📡 Airtable 업데이트 시작...")
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
            '분류': "52주_신고가_기술분석_포함",
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

import os
import requests
from datetime import datetime
from airtable import Airtable
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_session():
    """Retry를 설정한 세션 생성"""
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

def get_batch_stock_details(symbols):
    """Batch로 여러 종목의 세부 정보를 가져오기"""
    url = f"https://financialmodelingprep.com/api/v3/quote/{','.join(symbols)}"
    params = {'apikey': FMP_API_KEY}

    session = get_session()
    response = session.get(url, params=params)
    if response.status_code != 200:
        print(f"Batch 요청 실패: {response.status_code}")
        return []

    data = response.json()
    print(f"Batch 응답 데이터: {data[:5]}")  # 디버깅용 첫 5개 데이터 출력
    return data

def get_52week_high_stocks():
    """52주 신고가 근처에 있는 주식 데이터 가져오기"""
    url = "https://financialmodelingprep.com/api/v3/stock-screener"
    params = {
        'marketCapMoreThan': 500000000,
        'priceMoreThan': 5,
        'volumeMoreThan': 1000000,
        'apikey': FMP_API_KEY
    }

    session = get_session()
    response = session.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"API 호출 실패: {response.status_code}")

    stocks = response.json()
    print(f"응답된 주식 데이터 개수: {len(stocks)}")
    if stocks:
        print(f"첫 번째 주식 데이터 샘플: {stocks[0]}")

    # 거래소 필터링
    filtered_by_exchange = [
        stock for stock in stocks 
        if stock.get('exchangeShortName') in ['NYSE', 'NASDAQ']
    ]
    print(f"필터링된 거래소 종목 수: {len(filtered_by_exchange)}")

    # 심볼 목록 생성
    symbols = [stock['symbol'] for stock in filtered_by_exchange if stock.get('symbol')]
    print(f"심볼 수집 완료: {len(symbols)}개")

    # Batch 요청으로 상세 데이터 가져오기
    batch_size = 100
    detailed_stocks = []

    for i in range(0, len(symbols), batch_size):
        batch_symbols = symbols[i:i + batch_size]
        details = get_batch_stock_details(batch_symbols)
        detailed_stocks.extend(details)

    # 필터링
    filtered_stocks = []
    for stock in detailed_stocks:
        if not stock.get('price') or not stock.get('yearHigh'):
            print(f"필수 필드 누락: {stock.get('symbol')} - 응답 데이터: {stock}")
            continue
        high_ratio = (stock['price'] / stock['yearHigh']) * 100
        stock['highRatio'] = high_ratio

        # 100% 이상인 종목 명시적 출력
        if high_ratio >= 100:
            print(f"신고가 비율 100% 이상: {stock['symbol']} - 현재가: {stock['price']} - 52주 신고가: {stock['yearHigh']} - 비율: {high_ratio:.2f}%")

        # 95% 이상 포함
        if high_ratio >= 95:
            filtered_stocks.append(stock)

    return sorted(filtered_stocks, key=lambda x: x['highRatio'], reverse=True)[:20]

def update_airtable(stock_data):
    """Airtable에 데이터 추가"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")

    records = []
    for stock in stock_data:
        record = {
            'fields': {
                '티커': stock.get('symbol', ''),
                '종목명': stock.get('name', ''),
                '현재가': float(stock.get('price', 0)),
                '52주 신고가': float(stock.get('yearHigh', 0)),
                '신고가 비율(%)': round(stock.get('highRatio', 0), 2),
                '거래량': int(stock.get('volume', 0)),
                '시가총액': float(stock.get('marketCap', 0)),
                '업데이트 시간': current_date,
                '분류': '52주신고가상위',
                '거래소 정보': stock.get('exchange', '')
            }
        }
        records.append(record)

    try:
        response = airtable.batch_insert(records)
        print(f"{len(records)}개의 데이터가 Airtable에 추가되었습니다.")
    except Exception as e:
        print(f"Airtable 업로드 중 오류 발생: {str(e)}")

def main():
    print("데이터 수집 시작...")
    print("필터링 조건:")
    print("- 현재가 >= $5")
    print("- 거래량 >= 1,000,000주")
    print("- 시가총액 >= $500,000,000")
    print("- 현재가가 52주 신고가의 95% 이상")

    try:
        stocks = get_52week_high_stocks()
        if not stocks:
            print("조건에 맞는 종목이 없습니다.")
            return

        print(f"\n조건을 만족하는 종목 수: {len(stocks)}개")
        update_airtable(stocks)
        print("\n모든 데이터 처리 완료!")

    except Exception as e:
        print(f"프로그램 실행 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()

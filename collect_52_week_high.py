import os
import requests
from datetime import datetime, timedelta
import time
from typing import List, Dict, Optional
from airtable import Airtable

# API 설정
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_stock_details(ticker: str) -> Optional[Dict]:
    """Polygon API를 사용하여 개별 종목의 상세정보(시가총액, 회사명, 거래소 등)를 조회합니다."""
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {'apiKey': POLYGON_API_KEY}
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('results', {})
        else:
            print(f"종목 상세정보 조회 실패 ({ticker}): {response.status_code}")
            print(f"응답 내용: {response.text}")
            return None
    except Exception as e:
        print(f"종목 상세정보 조회 중 에러 발생 ({ticker}): {str(e)}")
        return None

def get_all_stocks() -> List[Dict]:
    """Polygon API의 스냅샷 엔드포인트를 사용하여 모든 주식 데이터를 조회합니다."""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {
        'apiKey': POLYGON_API_KEY,
        'include_otc': 'false'
    }
    
    try:
        print(f"API 요청 시작: {url}")
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return data.get('tickers', [])
        else:
            print(f"API 요청 실패: {response.status_code}")
            print(f"응답 내용: {response.text}")
            return []
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def filter_stocks(stocks: List[Dict]) -> List[Dict]:
    """
    1차 필터링: 스냅샷 데이터에서 아래 조건을 만족하는 종목만 선택합니다.
       - 종가 (day['c']) >= $10
       - 거래량 (day['v']) >= 1,000,000주
       - 상세정보 조회 후 시가총액 (market_cap) >= $500M
    """
    filtered = []
    total = len(stocks)
    print(f"총 {total}개 종목 필터링 시작...")
    
    for i, stock in enumerate(stocks, 1):
        try:
            day_data = stock.get('day', {})
            price = float(day_data.get('c', 0))  # 'c'는 종가
            volume = int(day_data.get('v', 0))
            
            if i % 100 == 0:
                print(f"진행 중... {i}/{total}")
            
            if price >= 10 and volume >= 1_000_000:
                details = get_stock_details(stock.get('ticker', ''))
                if details:
                    market_cap = float(details.get('market_cap', 0))
                    if market_cap >= 500_000_000:
                        stock['name'] = details.get('name', '')
                        stock['market_cap'] = market_cap
                        stock['primary_exchange'] = details.get('primary_exchange', '')
                        filtered.append(stock)
                        print(f"조건 만족: {stock.get('ticker','')} (가격: ${price}, 거래량: {volume:,}, 시가총액: ${market_cap:,.2f})")
                        time.sleep(0.5)  # API 호출 속도 제한 고려
        except Exception as e:
            print(f"종목 필터링 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")
            continue

    return filtered

def get_52_week_high(ticker: str) -> Dict:
    """
    주어진 티커의 52주(1년) 동안의 일별 데이터에서 최고 'h' 값(52주 최고가)를 계산합니다.
    """
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
        data = response.json()
        if data.get('status') != 'OK' or not data.get('results'):
            return {'high': 0.0}
        results = data['results']
        high_prices = [day['h'] for day in results if 'h' in day]
        return {'high': max(high_prices) if high_prices else 0.0}
    except Exception as e:
        print(f"Error fetching 52주 데이터 for {ticker}: {str(e)}")
        return {'high': 0.0}

def process_high_stocks(filtered_stocks: List[Dict]) -> List[Dict]:
    """
    최종 필터링: 1차 필터링된 종목들 중에서
       - 현재 종가(day['c'])가 해당 종목의 52주 최고가의 95% 이상인 종목만 최종 선정합니다.
    """
    high_threshold = 0.95
    result = []
    
    for stock in filtered_stocks:
        ticker = stock.get('ticker', '')
        high_data = get_52_week_high(ticker)
        current_price = float(stock.get('day', {}).get('c', 0))
        if high_data['high'] > 0 and current_price >= high_data['high'] * high_threshold:
            stock.update({
                '52_week_high': high_data['high'],
                'price_to_high_ratio': current_price / high_data['high']
            })
            result.append(stock)
            print(f"52주 신고가 조건 통과: {ticker} - 현재가 ${current_price:.2f} / 52주 최고가 ${high_data['high']:.2f}")
        else:
            print(f"52주 신고가 조건 미달: {ticker} - 현재가 ${current_price:.2f} / 52주 최고가 ${high_data['high']:.2f}")
    
    # 신고가 대비 현재가 비율 내림차순 정렬 (비율이 높을수록 신고가에 근접)
    return sorted(result, key=lambda x: x.get('price_to_high_ratio', 0), reverse=True)

def convert_exchange_code(mic: str) -> str:
    """거래소 MIC 코드를 변환합니다."""
    exchange_map = {
        'XNAS': 'NASDAQ',
        'XNYS': 'NYSE',
        'XASE': 'AMEX'
    }
    return exchange_map.get(mic, mic)

def update_airtable(stock_data: List[Dict], category: str):
    """
    Airtable에 데이터를 **무조건 새 레코드로 추가**합니다.
    """
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            day_data = stock.get('day', {})
            price = float(day_data.get('c', 0))
            record = {
                '티커': stock.get('ticker', ''),
                '종목명': stock.get('name', ''),
                '현재가': price,
                '52주 신고가': float(stock.get('52_week_high', 0)),
                '신고가대비': f"{stock.get('price_to_high_ratio', 0) * 100:.1f}%",
                '시가총액': f"${float(stock.get('market_cap', 0)):,.2f}",
                '업데이트 시간': current_date,
                '분류': category,
                '거래소': convert_exchange_code(stock.get('primary_exchange', 'N/A'))
            }
            airtable.insert(record)
            print(f"Airtable에 추가: {record['티커']}")
            time.sleep(0.2)  # Airtable API 호출 속도 조절
        except Exception as e:
            print(f"Airtable 데이터 추가 중 오류 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
    try:
        print("데이터 수집 시작...")
        print("1차 필터링 조건: 종가 >= $10, 거래량 >= 1,000,000, 시가총액 >= $500M")
        
        # 1. 전체 스냅샷 데이터 조회
        all_stocks = get_all_stocks()
        if not all_stocks:
            print("데이터 수집 실패")
            return
        print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")
        
        # 2. 1차 필터링: 기본 조건 + 상세정보(시가총액) 체크
        filtered_stocks = filter_stocks(all_stocks)
        print(f"\n1차 필터링 조건을 만족하는 종목 수: {len(filtered_stocks)}개")
        
        # 3. 52주 신고가 조건 필터링: 현재가가 52주 최고가의 95% 이상인 종목만 선택
        high_stocks = process_high_stocks(filtered_stocks)
        print(f"\n최종 52주 신고가 상위 종목 수: {len(high_stocks)}개")
        
        # 4. Airtable에 결과 삽입 (무조건 새 레코드 추가)
        if high_stocks:
            update_airtable(high_stocks, "52주 신고가 상위")
            print("Airtable 업데이트 완료")
        else:
            print("선정된 종목이 없습니다.")
        
        print("\n모든 데이터 처리 완료!")
        
    except Exception as e:
        print(f"프로그램 실행 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()

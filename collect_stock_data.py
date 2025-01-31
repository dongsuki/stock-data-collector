import os
import requests
from datetime import datetime
from airtable import Airtable
import time
from typing import Dict, List, Optional

POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_stock_details(ticker: str) -> Dict:
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {'apiKey': POLYGON_API_KEY}
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('results', {})
        return None
    except Exception as e:
        print(f"종목 상세정보 조회 중 에러 발생 ({ticker}): {str(e)}")
        return None

def filter_recent_financials(financials: List, max_quarters=12) -> List:
    if not financials:
        return []
    return sorted(financials, key=lambda x: x.get('period_of_report_date', ''), reverse=True)[:max_quarters]

def get_financials(ticker: str) -> List:
    url = f"https://api.polygon.io/vX/reference/financials"
    params = {
        'apiKey': POLYGON_API_KEY,
        'ticker': ticker,
        'limit': 12,
        'timeframe': 'quarterly',
        'order': 'desc',
        'sort': 'period_of_report_date'
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            financials = response.json().get('results', [])
            return filter_recent_financials(financials)
        return []
    except Exception as e:
        print(f"재무데이터 조회 중 에러 발생 ({ticker}): {str(e)}")
        return []

def safe_growth_rate(current: float, previous: float) -> Optional[float]:
    if current is not None and previous is not None and previous != 0:
        return ((current - previous) / abs(previous)) * 100
    return None

def calculate_growth_rates(financials: List) -> Dict:
    growth_rates = {
        'eps_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None},
        'operating_income_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None},
        'revenue_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None}
    }
    
    if not financials:
        return growth_rates
        
    try:
        # 분기 성장률 계산
        for i in range(min(len(financials)-1, 3)):
            current = financials[i].get('financials', {})
            previous = financials[i+1].get('financials', {})
            
            current_eps = current.get('income_statement', {}).get('diluted_earnings_per_share', {}).get('value')
            previous_eps = previous.get('income_statement', {}).get('diluted_earnings_per_share', {}).get('value')
            
            current_op = current.get('income_statement', {}).get('operating_income_loss', {}).get('value')
            previous_op = previous.get('income_statement', {}).get('operating_income_loss', {}).get('value')
            
            current_rev = current.get('income_statement', {}).get('revenues', {}).get('value')
            previous_rev = previous.get('income_statement', {}).get('revenues', {}).get('value')
            
            quarter_key = f'q{i+1}'
            growth_rates['eps_growth'][quarter_key] = safe_growth_rate(current_eps, previous_eps)
            growth_rates['operating_income_growth'][quarter_key] = safe_growth_rate(current_op, previous_op)
            growth_rates['revenue_growth'][quarter_key] = safe_growth_rate(current_rev, previous_rev)
        
        # 연간 성장률 계산
        for year in range(1, 4):
            if len(financials) >= year * 4:
                current = financials[0].get('financials', {})
                previous = financials[year * 4 - 1].get('financials', {})
                
                current_eps = current.get('income_statement', {}).get('diluted_earnings_per_share', {}).get('value')
                previous_eps = previous.get('income_statement', {}).get('diluted_earnings_per_share', {}).get('value')
                
                current_op = current.get('income_statement', {}).get('operating_income_loss', {}).get('value')
                previous_op = previous.get('income_statement', {}).get('operating_income_loss', {}).get('value')
                
                current_rev = current.get('income_statement', {}).get('revenues', {}).get('value')
                previous_rev = previous.get('income_statement', {}).get('revenues', {}).get('value')
                
                year_key = f'y{year}'
                growth_rates['eps_growth'][year_key] = safe_growth_rate(current_eps, previous_eps)
                growth_rates['operating_income_growth'][year_key] = safe_growth_rate(current_op, previous_op)
                growth_rates['revenue_growth'][year_key] = safe_growth_rate(current_rev, previous_rev)
                    
    except Exception as e:
        print(f"성장률 계산 중 에러 발생: {str(e)}")
    
    return growth_rates

def update_airtable(stock_data: List, category: str):
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            financials = get_financials(stock['ticker'])
            growth_rates = calculate_growth_rates(financials)
            
            record = {
                '티커': stock.get('ticker', ''),
                '종목명': stock.get('name', ''),
                '현재가': float(stock.get('day', {}).get('c', 0)),
                '등락률': float(stock.get('todaysChangePerc', 0)),
                '거래량': int(stock.get('day', {}).get('v', 0)),
                '시가총액': float(stock.get('market_cap', 0)),
                '업데이트 시간': current_date,
                '분류': category,
                'EPS성장률_최신분기': growth_rates['eps_growth']['q1'],
                'EPS성장률_전분기': growth_rates['eps_growth']['q2'],
                'EPS성장률_전전분기': growth_rates['eps_growth']['q3'],
                'EPS성장률_1년': growth_rates['eps_growth']['y1'],
                'EPS성장률_2년': growth_rates['eps_growth']['y2'],
                'EPS성장률_3년': growth_rates['eps_growth']['y3'],
                '영업이익성장률_최신분기': growth_rates['operating_income_growth']['q1'],
                '영업이익성장률_전분기': growth_rates['operating_income_growth']['q2'],
                '영업이익성장률_전전분기': growth_rates['operating_income_growth']['q3'],
                '영업이익성장률_1년': growth_rates['operating_income_growth']['y1'],
                '영업이익성장률_2년': growth_rates['operating_income_growth']['y2'],
                '영업이익성장률_3년': growth_rates['operating_income_growth']['y3'],
                '매출액성장률_최신분기': growth_rates['revenue_growth']['q1'],
                '매출액성장률_전분기': growth_rates['revenue_growth']['q2'],
                '매출액성장률_전전분기': growth_rates['revenue_growth']['q3'],
                '매출액성장률_1년': growth_rates['revenue_growth']['y1'],
                '매출액성장률_2년': growth_rates['revenue_growth']['y2'],
                '매출액성장률_3년': growth_rates['revenue_growth']['y3'],
            }
            
            if stock.get('primary_exchange'):
                record['거래소 정보'] = convert_exchange_code(stock['primary_exchange'])
            
            airtable.insert(record)
            print(f"데이터 추가 완료: {record['티커']}")
            time.sleep(0.2)
                
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def get_all_stocks():
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {
        'apiKey': POLYGON_API_KEY,
        'include_otc': False
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return data.get('tickers', [])
        else:
            print(f"API 요청 실패: {response.status_code}")
            return []
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def filter_stocks(stocks: List) -> List:
    filtered = []
    total = len(stocks)
    
    print(f"총 {total}개 종목 필터링 시작...")
    
    for i, stock in enumerate(stocks, 1):
        day_data = stock.get('day', {})
        price = float(day_data.get('c', 0))
        volume = int(day_data.get('v', 0))
        change = float(stock.get('todaysChangePerc', 0))
        
        if i % 100 == 0:
            print(f"진행 중... {i}/{total}")
        
        if price >= 5 and volume >= 1000000 and change >= 5:
            details = get_stock_details(stock['ticker'])
            if details:
                market_cap = float(details.get('market_cap', 0))
                if market_cap >= 500000000:
                    stock['name'] = details.get('name', '')
                    stock['market_cap'] = market_cap
                    stock['primary_exchange'] = details.get('primary_exchange', '')
                    filtered.append(stock)
                    print(f"조건 만족: {stock['ticker']} (시가총액: ${market_cap:,.2f})")
    
    return sorted(filtered, key=lambda x: x.get('todaysChangePerc', 0), reverse=True)

def convert_exchange_code(mic: str) -> str:
    exchange_map = {
        'XNAS': 'NASDAQ',
        'XNYS': 'NYSE',
        'XASE': 'AMEX'
    }
    return exchange_map.get(mic, mic)

def main():
    print("데이터 수집 시작...")
    print("필터링 조건: 현재가 >= $5, 거래량 >= 1M주, 등락률 >= 5%, 시가총액 >= $500M")
    
    all_stocks = get_all_stocks()
    if not all_stocks:
        print("데이터 수집 실패")
        return
        
    print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")
    filtered_stocks = filter_stocks(all_stocks)
    print(f"\n조건을 만족하는 종목 수: {len(filtered_stocks)}개")
    
    if filtered_stocks:
        update_airtable(filtered_stocks, "전일대비등락률상위")
    
    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

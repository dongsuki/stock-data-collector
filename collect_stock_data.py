import os
import requests
from datetime import datetime
from airtable import Airtable
import time
from typing import Dict, List, Optional

POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def safe_growth_rate(current: float, previous: float) -> Optional[float]:
    try:
        if not current or not previous:
            return None
        current = float(current)
        previous = float(previous)
        if previous > 0:  # 양수인 경우만 계산
            return ((current - previous) / abs(previous)) * 100
        return None
    except (ValueError, TypeError):
        return None

def get_financials_fmp(ticker: str) -> List:
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
    params = {'apikey': FMP_API_KEY, 'period': 'quarter', 'limit': 20}  # 5년치 분기 데이터 (20개)
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            financials = response.json()
            return sorted(financials, key=lambda x: x.get('date', ''), reverse=True)
        return []
    except Exception as e:
        print(f"재무데이터 조회 중 에러 발생 ({ticker}): {str(e)}")
        return []

def calculate_growth_rates_fmp(financials: List) -> Dict:
    growth_rates = {
        'eps_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None},
        'operating_income_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None},
        'revenue_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None}
    }
    
    if not financials or len(financials) < 5:  # 최소 5개 분기 데이터 필요
        return growth_rates

    try:
        # 분기별 성장률 계산 (전년 동기 대비)
        for i in range(3):  # 최근 3개 분기
            if i + 4 < len(financials):  # 전년 동기 데이터 확인
                current = financials[i]
                year_ago = financials[i + 4]  # 전년 동기 데이터
                
                growth_rates['eps_growth'][f'q{i+1}'] = safe_growth_rate(current.get('eps', 0), year_ago.get('eps', 0))
                growth_rates['operating_income_growth'][f'q{i+1}'] = safe_growth_rate(
                    current.get('operatingIncome', 0), year_ago.get('operatingIncome', 0)
                )
                growth_rates['revenue_growth'][f'q{i+1}'] = safe_growth_rate(
                    current.get('revenue', 0), year_ago.get('revenue', 0)
                )

        # 연간 데이터 집계
        annual_data = []
        for year in range(4):  # 최근 4년
            if year * 4 + 3 < len(financials):
                year_financials = financials[year * 4 : year * 4 + 4]
                annual_data.append({
                    'eps': sum(float(q.get('eps', 0)) for q in year_financials),
                    'operatingIncome': sum(float(q.get('operatingIncome', 0)) for q in year_financials),
                    'revenue': sum(float(q.get('revenue', 0)) for q in year_financials)
                })
        
        # 연간 성장률 계산 (전년 대비)
        for i in range(3):
            if i + 1 < len(annual_data):
                current = annual_data[i]
                previous = annual_data[i + 1]
                
                growth_rates['eps_growth'][f'y{i+1}'] = safe_growth_rate(current['eps'], previous['eps'])
                growth_rates['operating_income_growth'][f'y{i+1}'] = safe_growth_rate(
                    current['operatingIncome'], previous['operatingIncome']
                )
                growth_rates['revenue_growth'][f'y{i+1}'] = safe_growth_rate(current['revenue'], previous['revenue'])
                
    except Exception as e:
        print(f"성장률 계산 중 에러 발생: {str(e)}")
    
    return growth_rates

def update_airtable(stock_data: List, category: str):
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            financials = get_financials_fmp(stock['ticker'])
            growth_rates = calculate_growth_rates_fmp(financials)
            
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
            
            airtable.insert(record)
            print(f"데이터 추가 완료: {record['티커']}")
            time.sleep(0.2)
                
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

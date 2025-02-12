import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone
from typing import Dict, List
from airtable import Airtable

class StockAnalyzer:
    def __init__(self):
        self.kst = timezone('Asia/Seoul')
        # Airtable 설정
        self.base_id = 'appJFk54sIT9oSiZy'
        self.table_name = '마크미너비니'
        self.api_key = 'patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748'
        self.airtable = Airtable(self.base_id, self.table_name, self.api_key)

    def get_krx_tickers(self) -> List[str]:
        """코스피, 코스닥 티커 가져오기 (ETF 제외)"""
        kospi = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt')[0]
        kosdaq = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt')[0]
        
        # 종목코드 포맷팅
        kospi['종목코드'] = kospi['종목코드'].apply(lambda x: f"{x:06d}.KS")
        kosdaq['종목코드'] = kosdaq['종목코드'].apply(lambda x: f"{x:06d}.KQ")
        
        # ETF 제외
        kospi = kospi[~kospi['주식종류'].str.contains('ETF', na=False)]
        kosdaq = kosdaq[~kosdaq['주식종류'].str.contains('ETF', na=False)]
        
        return list(kospi['종목코드']) + list(kosdaq['종목코드'])

    def calculate_weighted_return(self, historical: pd.DataFrame) -> float:
        """가중 수익률 계산"""
        if len(historical) < 252:
            return None
            
        try:
            prices = historical['Close'].iloc[[0, 63, 126, 189, 251]].values
            q1 = (prices[0] / prices[1] - 1) * 100
            q2 = (prices[1] / prices[2] - 1) * 100
            q3 = (prices[2] / prices[3] - 1) * 100
            q4 = (prices[3] / prices[4] - 1) * 100
            
            return q1 * 0.4 + q2 * 0.2 + q3 * 0.2 + q4 * 0.2
        except Exception:
            return None

    def calculate_rs_rating(self, returns_dict: Dict[str, float]) -> Dict[str, float]:
        """RS 등급 계산"""
        valid_returns = {k: v for k, v in returns_dict.items() if v is not None}
        symbols = list(valid_returns.keys())
        rets = list(valid_returns.values())
        
        s = pd.Series(rets)
        ranks = s.rank(ascending=False)
        n = len(rets)
        rs_values = ((n - ranks) / (n - 1) * 98) + 1
        
        return {sym: rs for sym, rs in zip(symbols, rs_values)}

    def check_technical_conditions(self, stock_data: pd.DataFrame) -> bool:
        """기술적 조건 확인"""
        if len(stock_data) < 220:
            return False

        closes = stock_data['Close'].values
        current_price = closes[0]
        
        # 이동평균선 계산
        ma50 = np.mean(closes[:50])
        ma150 = np.mean(closes[:150])
        ma200 = np.mean(closes[:200])
        ma200_prev = np.mean(closes[20:220])
        
        # 조건 확인
        conditions = {
            'price_above_ma50': current_price > ma50,
            'price_above_ma150': current_price > ma150,
            'price_above_ma200': current_price > ma200,
            'ma50_above_ma150': ma50 > ma150,
            'ma150_above_ma200': ma150 > ma200,
            'ma200_trend': ma200 > ma200_prev
        }
        
        return all(conditions.values())

    def analyze_stocks(self):
        """주식 분석 실행"""
        tickers = self.get_krx_tickers()
        returns_dict = {}
        stock_data = {}
        
        # 데이터 수집 및 가중 수익률 계산
        for ticker in tickers:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="2y")
            if len(hist) > 0:
                returns_dict[ticker] = self.calculate_weighted_return(hist)
                stock_data[ticker] = hist

        # RS 등급 계산
        rs_ratings = self.calculate_rs_rating(returns_dict)
        
        # 조건에 맞는 종목 선별
        selected_stocks = []
        for ticker, rs_value in rs_ratings.items():
            if rs_value >= 70:  # RS 임계값
                hist = stock_data[ticker]
                if self.check_technical_conditions(hist):
                    current_price = hist['Close'].iloc[0]
                    year_high = hist['High'].max()
                    if current_price >= year_high * 0.75:  # 신고가 대비 조건
                        selected_stocks.append({
                            'ticker': ticker,
                            'name': yf.Ticker(ticker).info.get('longName', ''),
                            'current_price': current_price,
                            'change_percent': ((current_price / hist['Close'].iloc[1]) - 1) * 100,
                            'volume': hist['Volume'].iloc[0],
                            'year_high': year_high,
                            'rs_rating': rs_value,
                            'market_cap': yf.Ticker(ticker).info.get('marketCap', 0)
                        })

        return selected_stocks

    def update_airtable(self, selected_stocks: List[Dict]):
        """Airtable 업데이트"""
        # 기존 레코드 삭제
        existing_records = self.airtable.get_all()
        for record in existing_records:
            self.airtable.delete(record['id'])

        # 새로운 데이터 추가
        for stock in selected_stocks:
            self.airtable.insert({
                '종목명': stock['name'],
                '업데이트 날짜': datetime.now(self.kst).strftime('%Y-%m-%d'),
                '현재가': stock['current_price'],
                '등락률': stock['change_percent'],
                '거래대금': stock['volume'],
                '52주 신고가': stock['year_high'],
                'RS순위': stock['rs_rating'],
                '시가총액': stock['market_cap']
            })

def main():
    analyzer = StockAnalyzer()
    selected_stocks = analyzer.analyze_stocks()
    analyzer.update_airtable(selected_stocks)

if __name__ == "__main__":
    main()

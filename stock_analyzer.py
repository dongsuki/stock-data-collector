import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone
from typing import Dict, List
from airtable import Airtable
import os
import requests
import FinanceDataReader as fdr

class StockAnalyzer:
    def __init__(self):
        self.kst = timezone('Asia/Seoul')
        # Airtable 설정
        self.base_id = 'appJFk54sIT9oSiZy'
        self.table_name = '마크미너비니'
        self.api_key = os.environ['AIRTABLE_API_KEY']
        self.airtable = Airtable(self.base_id, self.table_name, self.api_key)

    def get_krx_tickers(self) -> List[str]:
        """코스피, 코스닥 티커 가져오기 (ETF 제외)"""
        try:
            # KOSPI 종목 가져오기
            kospi = fdr.StockListing('KOSPI')
            # KOSDAQ 종목 가져오기
            kosdaq = fdr.StockListing('KOSDAQ')
            
            # 컬럼명 확인 및 출력
            print("KOSPI columns:", kospi.columns)
            print("KOSDAQ columns:", kosdaq.columns)
            
            # 종목코드 컬럼이 'Code'인 경우
            code_column = 'Code' if 'Code' in kospi.columns else 'code'
            name_column = 'Name' if 'Name' in kospi.columns else 'name'
            
            # 종목코드 포맷팅
            kospi_tickers = kospi[code_column].astype(str).apply(lambda x: f"{x.zfill(6)}.KS")
            kosdaq_tickers = kosdaq[code_column].astype(str).apply(lambda x: f"{x.zfill(6)}.KQ")
            
            # ETF 제외 (Name 컬럼에 'ETF'가 포함된 경우)
            kospi_tickers = kospi_tickers[~kospi[name_column].str.contains('ETF', na=False)]
            kosdaq_tickers = kosdaq_tickers[~kosdaq[name_column].str.contains('ETF', na=False)]
            
            return list(kospi_tickers) + list(kosdaq_tickers)
        except Exception as e:
            print(f"티커 가져오기 중 오류 발생: {e}")
            print(f"KOSPI data sample:\n{kospi.head()}")
            print(f"KOSDAQ data sample:\n{kosdaq.head()}")
            raise

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
        except Exception as e:
            print(f"수익률 계산 중 오류 발생: {e}")
            return None

    def calculate_rs_rating(self, returns_dict: Dict[str, float]) -> Dict[str, float]:
        """RS 등급 계산"""
        valid_returns = {k: v for k, v in returns_dict.items() if v is not None}
        symbols = list(valid_returns.keys())
        rets = list(valid_returns.values())
        
        if not rets:  # 유효한 수익률이 없는 경우
            return {}
            
        s = pd.Series(rets)
        ranks = s.rank(ascending=False)
        n = len(rets)
        rs_values = ((n - ranks) / (n - 1) * 98) + 1
        
        return {sym: rs for sym, rs in zip(symbols, rs_values)}

    def check_technical_conditions(self, stock_data: pd.DataFrame) -> bool:
        """기술적 조건 확인"""
        if len(stock_data) < 220:
            return False

        try:
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
        except Exception as e:
            print(f"기술적 조건 확인 중 오류 발생: {e}")
            return False

    def is_trading_day(self) -> bool:
        """오늘이 거래일인지 확인"""
        today = datetime.now(self.kst)
        
        # 주말 체크
        if today.weekday() >= 5:  # 5: 토요일, 6: 일요일
            return False
            
        try:
            # KRX 거래일 확인
            krx = fdr.StockListing('KRX')
            if krx.empty:
                # API 오류 시 주말만 체크
                return today.weekday() < 5
            return True
        except Exception as e:
            print(f"거래일 확인 중 오류 발생: {e}")
            # API 오류 시 주말만 체크
            return today.weekday() < 5

    def analyze_stocks(self):
        """주식 분석 실행"""
        tickers = self.get_krx_tickers()
        returns_dict = {}
        stock_data = {}
        
        print(f"총 {len(tickers)}개 종목 분석 시작...")
        
        for i, ticker in enumerate(tickers):
            try:
                if i % 100 == 0:
                    print(f"{i}/{len(tickers)} 종목 처리 중...")
                    
                stock = yf.Ticker(ticker)
                hist = stock.history(period="2y")
                if len(hist) > 0:
                    returns_dict[ticker] = self.calculate_weighted_return(hist)
                    stock_data[ticker] = hist
            except Exception as e:
                print(f"{ticker} 처리 중 오류 발생: {e}")
                continue

        print("RS 등급 계산 중...")
        rs_ratings = self.calculate_rs_rating(returns_dict)
        
        print("조건에 맞는 종목 선별 중...")
        selected_stocks = []
        for ticker, rs_value in rs_ratings.items():
            if rs_value >= 70:  # RS 임계값
                hist = stock_data[ticker]
                if self.check_technical_conditions(hist):
                    current_price = hist['Close'].iloc[0]
                    year_high = hist['High'].max()
                    if current_price >= year_high * 0.75:  # 신고가 대비 조건
                        try:
                            stock_info = yf.Ticker(ticker).info
                            selected_stocks.append({
                                'ticker': ticker,
                                'name': stock_info.get('longName', ''),
                                'current_price': current_price,
                                'change_percent': ((current_price / hist['Close'].iloc[1]) - 1) * 100,
                                'volume': hist['Volume'].iloc[0] * current_price,  # 거래대금으로 변환
                                'year_high': year_high,
                                'rs_rating': rs_value,
                                'market_cap': stock_info.get('marketCap', 0)
                            })
                        except Exception as e:
                            print(f"{ticker} 정보 가져오기 중 오류 발생: {e}")
                            continue

        print(f"총 {len(selected_stocks)}개 종목 선정 완료")
        return selected_stocks

    def update_airtable(self, selected_stocks: List[Dict]):
        """Airtable 업데이트"""
        print("Airtable 업데이트 시작...")
        try:
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
                    '등락률': round(stock['change_percent'], 2),
                    '거래대금': round(stock['volume'] / 1_000_000, 2),  # 백만 단위로 변환
                    '52주 신고가': stock['year_high'],
                    'RS순위': round(stock['rs_rating'], 2),
                    '시가총액': round(stock['market_cap'] / 1_000_000_000, 2)  # 10억 단위로 변환
                })
            print("Airtable 업데이트 완료")
        except Exception as e:
            print(f"Airtable 업데이트 중 오류 발생: {e}")
            raise

def main():
    analyzer = StockAnalyzer()
    
    if not analyzer.is_trading_day():
        print("오늘은 거래일이 아닙니다.")
        return
        
    print("주식 분석을 시작합니다...")
    selected_stocks = analyzer.analyze_stocks()
    analyzer.update_airtable(selected_stocks)
    print("분석 완료!")

if __name__ == "__main__":
    main()

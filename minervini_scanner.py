import concurrent.futures
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone
from typing import Dict, List, Tuple
from airtable import Airtable
import os
import requests
import FinanceDataReader as fdr
import time
from random import uniform

class StockAnalyzer:
    def __init__(self):
        self.kst = timezone('Asia/Seoul')
        self.base_id = 'appJFk54sIT9oSiZy'
        self.table_name = '마크미너비니'
        self.api_key = os.environ['AIRTABLE_API_KEY']
        self.airtable = Airtable(self.base_id, self.table_name, self.api_key)
        
        self.today = datetime.now(self.kst)
        self.start_date = (self.today - timedelta(days=280)).strftime('%Y-%m-%d')
        self.end_date = self.today.strftime('%Y-%m-%d')
        self.max_workers = 5

    def get_krx_tickers(self) -> List[str]:
        """코스피, 코스닥 티커 가져오기 (ETF 제외)"""
        try:
            kospi = fdr.StockListing('KOSPI')
            kosdaq = fdr.StockListing('KOSDAQ')
            
            code_column = 'Code' if 'Code' in kospi.columns else 'code'
            name_column = 'Name' if 'Name' in kospi.columns else 'name'
            
            kospi_tickers = kospi[code_column].astype(str).apply(lambda x: f"{x.zfill(6)}.KS")
            kosdaq_tickers = kosdaq[code_column].astype(str).apply(lambda x: f"{x.zfill(6)}.KQ")
            
            kospi_tickers = kospi_tickers[~kospi[name_column].str.contains('ETF', na=False)]
            kosdaq_tickers = kosdaq_tickers[~kosdaq[name_column].str.contains('ETF', na=False)]
            
            return list(kospi_tickers) + list(kosdaq_tickers)
        except Exception as e:
            print(f"티커 가져오기 중 오류 발생: {e}")
            raise

    def calculate_weighted_return(self, df: pd.DataFrame) -> float:
        """가중 수익률 계산"""
        if len(df) < 252:
            return None
            
        try:
            required_idx = [0, 63, 126, 189, 251]
            prices = df['Close'].iloc[required_idx].values
            
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
        
        if not rets:
            return {}
            
        s = pd.Series(rets)
        ranks = s.rank(ascending=False)
        n = len(rets)
        rs_values = ((n - ranks) / (n - 1) * 98) + 1
        
        return {sym: rs for sym, rs in zip(symbols, rs_values)}

    def check_technical_conditions(self, df: pd.DataFrame) -> bool:
        """기술적 조건 확인"""
        if len(df) < 220:
            return False

        try:
            closes = df['Close']
            current_price = closes.iloc[0]
            
            ma50 = closes.rolling(window=50).mean().iloc[-1]
            ma150 = closes.rolling(window=150).mean().iloc[-1]
            ma200 = closes.rolling(window=200).mean().iloc[-1]
            ma200_prev = closes.iloc[20:220].mean()
            
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

    def get_stock_data_with_return(self, ticker: str) -> Tuple[str, pd.DataFrame, float]:
        """병렬 처리를 위한 데이터 및 수익률 계산 함수"""
        retry_count = 0
        while retry_count < 3:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(start=self.start_date, end=self.end_date)
                
                if len(hist) >= 252:
                    weighted_return = self.calculate_weighted_return(hist)
                    time.sleep(uniform(0.5, 1))
                    return ticker, hist, weighted_return
                return ticker, pd.DataFrame(), None
                
            except Exception as e:
                retry_count += 1
                if "Too Many Requests" in str(e):
                    wait_time = 5 * retry_count
                    print(f"{ticker} 처리 중 요청 제한 발생. {wait_time}초 후 재시도...")
                    time.sleep(wait_time)
                else:
                    print(f"{ticker} 처리 중 오류 발생: {e}")
                    return ticker, pd.DataFrame(), None
        
        return ticker, pd.DataFrame(), None

    def analyze_stocks(self):
        """주식 분석 실행 - 병렬 처리 적용"""
        tickers = self.get_krx_tickers()
        returns_dict = {}
        stock_data = {}
        
        print(f"총 {len(tickers)}개 종목 분석 시작...")
        
        # 1단계: 병렬 처리로 전체 종목의 수익률 계산
        print("전체 종목 수익률 계산 중...")
        processed_count = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_ticker = {executor.submit(self.get_stock_data_with_return, ticker): ticker for ticker in tickers}
            
            for future in concurrent.futures.as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    ticker, hist, weighted_return = future.result()
                    if weighted_return is not None:
                        returns_dict[ticker] = weighted_return
                        stock_data[ticker] = hist
                    
                    processed_count += 1
                    if processed_count % 50 == 0:
                        print(f"{processed_count}/{len(tickers)} 종목 처리 완료...")
                        
                except Exception as e:
                    print(f"{ticker} 처리 중 오류 발생: {e}")
        
        # 2단계: RS 등급 계산
        print("RS 등급 계산 중...")
        rs_ratings = self.calculate_rs_rating(returns_dict)
        
        # 3단계: RS 70 이상 종목에 대해 기술적 조건 확인
        print("조건에 맞는 종목 선별 중...")
        selected_stocks = []
        
        for ticker, rs_value in rs_ratings.items():
            if rs_value >= 70:
                hist = stock_data[ticker]
                if self.check_technical_conditions(hist):
                    current_price = hist['Close'].iloc[0]
                    year_high = hist['High'].max()
                    
                    if current_price >= year_high * 0.75:
                        try:
                            stock_info = yf.Ticker(ticker).info
                            time.sleep(uniform(0.5, 1))
                            
                            selected_stocks.append({
                                'ticker': ticker,
                                'name': stock_info.get('longName', ''),
                                'current_price': current_price,
                                'change_percent': ((current_price / hist['Close'].iloc[1]) - 1) * 100,
                                'volume': hist['Volume'].iloc[0] * current_price,
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
            existing_records = self.airtable.get_all()
            for record in existing_records:
                self.airtable.delete(record['id'])

            for stock in selected_stocks:
                self.airtable.insert({
                    '종목명': stock['name'],
                    '업데이트 날짜': self.today.strftime('%Y-%m-%d'),
                    '현재가': stock['current_price'],
                    '등락률': round(stock['change_percent'], 2),
                    '거래대금': round(stock['volume'] / 1_000_000, 2),
                    '52주 신고가': stock['year_high'],
                    'RS순위': round(stock['rs_rating'], 2),
                    '시가총액': round(stock['market_cap'] / 1_000_000_000, 2)
                })
            print("Airtable 업데이트 완료")
        except Exception as e:
            print(f"Airtable 업데이트 중 오류 발생: {e}")
            raise

def main():
    print("마크 미너비니 스캐너 시작...")
    analyzer = StockAnalyzer()
    selected_stocks = analyzer.analyze_stocks()
    analyzer.update_airtable(selected_stocks)
    print("분석 완료!")

if __name__ == "__main__":
    main()

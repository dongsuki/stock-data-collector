import yfinance as yf
import pandas as pd

def get_eps_trend_data(symbol):
    try:
        # 티커 객체 생성
        ticker = yf.Ticker(symbol)
        print(f"Fetching data for {symbol}...")
        
        # 데이터 가져오기 시도
        try:
            # EPS 트렌드 데이터 수집 시도
            earnings_trend = ticker.earnings_trend
            if earnings_trend is not None:
                print("Earnings trend data found:")
                print(earnings_trend)
            else:
                print("No earnings trend data available")
            
            # 애널리스트 예측 데이터 수집 시도
            analyst_data = ticker.analyst_price_target
            if analyst_data is not None:
                print("Analyst price target data found:")
                print(analyst_data)
            else:
                print("No analyst price target data available")
                
        except Exception as e:
            print(f"Error fetching trend data: {e}")
        
        return True
        
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return False

def main():
    symbol = 'INOD'
    success = get_eps_trend_data(symbol)
    print(f"Data collection for {symbol}: {'successful' if success else 'failed'}")

if __name__ == "__main__":
    main()

import yfinance as yf
import pandas as pd

def get_eps_trend_data(symbol):
    try:
        # 티커 객체 생성
        ticker = yf.Ticker(symbol)
        print(f"Fetching data for {symbol}...")
        
        try:
            # 분석가 추정치 데이터 가져오기
            print("\nAnalyst Recommendations:")
            recommendations = ticker.recommendations
            if recommendations is not None:
                print(recommendations)
            else:
                print("No recommendations data available")

            # 실적 데이터 가져오기
            print("\nEarnings Data:")
            earnings = ticker.earnings
            if earnings is not None:
                print(earnings)
            else:
                print("No earnings data available")

            # financials 데이터 확인
            print("\nFinancials Data:")
            financials = ticker.financials
            if financials is not None:
                print(financials)
            else:
                print("No financials data available")
                
        except Exception as e:
            print(f"Error fetching data: {e}")
        
        return True
        
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return False

def main():
    symbol = 'INOD'
    success = get_eps_trend_data(symbol)
    print(f"\nData collection for {symbol}: {'successful' if success else 'failed'}")

if __name__ == "__main__":
    main()

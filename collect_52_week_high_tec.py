import requests
import time
from datetime import datetime

FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"

def get_historical_data(symbol):
    """히스토리 데이터 가져오기"""
    try:
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?timeseries=252&apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if 'historical' in data:
                historical_data = sorted(data['historical'], key=lambda x: x['date'], reverse=True)
                print(f"수집된 데이터 개수: {len(historical_data)}일")
                return historical_data
            else:
                print("⚠️ historical 데이터 없음")
                return None
    except Exception as e:
        print(f"⚠️ 요청 실패: {str(e)}")
        return None

def get_quote(symbol):
    """현재 시세 데이터 가져오기"""
    try:
        url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data:
                return data[0]
    except Exception as e:
        print(f"⚠️ 시세 데이터 요청 실패: {str(e)}")
    return None

def calculate_rs(historical_data):
    """RS 계산"""
    try:
        print("\n=== 데이터 확인 ===")
        print(f"전체 데이터 수: {len(historical_data)}")
        print(f"첫 번째 데이터: {historical_data[0]}")
        print(f"마지막 데이터: {historical_data[-1]}")
        
        closes = [float(day['close']) for day in historical_data]
        print(f"\n=== 종가 데이터 ===")
        print(f"시작일: {historical_data[0]['date']}, 종가: {closes[0]}")
        print(f"63일전: {historical_data[63]['date']}, 종가: {closes[63]}")
        print(f"126일전: {historical_data[126]['date']}, 종가: {closes[126]}")
        print(f"189일전: {historical_data[189]['date']}, 종가: {closes[189]}")
        print(f"252일전: {historical_data[251]['date']}, 종가: {closes[251]}")
        
        # 분기별 수익률 계산 추가
        quarters = [
            ((closes[0] - closes[63]) / closes[63]) * 100,  # 최근 3개월
            ((closes[63] - closes[126]) / closes[126]) * 100,  # 2분기
            ((closes[126] - closes[189]) / closes[189]) * 100,  # 3분기
            ((closes[189] - closes[251]) / closes[251]) * 100,  # 4분기
        ]
        
        weighted_return = (
            quarters[0] * 0.4 +  # 최근 3개월: 40%
            quarters[1] * 0.2 +  # 2분기: 20%
            quarters[2] * 0.2 +  # 3분기: 20%
            quarters[3] * 0.2    # 4분기: 20%
        )
        
        print("\n=== RS 계산 결과 ===")
        print(f"최근 3개월 수익률: {quarters[0]:.1f}%")
        print(f"2분기 수익률: {quarters[1]:.1f}%")
        print(f"3분기 수익률: {quarters[2]:.1f}%")
        print(f"4분기 수익률: {quarters[3]:.1f}%")
        print(f"가중 평균 수익률: {weighted_return:.1f}%")
        
        return weighted_return
        
    except Exception as e:
        print(f"⚠️ RS 계산 중 오류 발생: {str(e)}")
        return None
        
def check_conditions(quote_data, historical_data):
    """52주 신고가 + 기술적 조건 확인"""
    try:
        # 현재가, 거래량, 시가총액 체크
        price = float(quote_data['price'])
        volume = float(quote_data['volume'])
        marketCap = float(quote_data['marketCap'])
        yearHigh = float(quote_data['yearHigh'])
        
        print("\n=== 기본 조건 체크 ===")
        print(f"현재가: ${price:.2f}")
        print(f"거래량: {volume:,.0f}")
        print(f"시가총액: ${marketCap:,.0f}")
        print(f"52주 고가: ${yearHigh:.2f}")
        
        # 52주 신고가 비율 계산
        price_to_high_ratio = (price / yearHigh) * 100
        print(f"52주 고가 대비: {price_to_high_ratio:.1f}%")
        
        # 이동평균선 계산
        closes = [float(day['close']) for day in historical_data]
        ma50 = sum(closes[:50]) / 50
        ma150 = sum(closes[:150]) / 150
        ma200 = sum(closes[:200]) / 200
        ma200_prev = sum(closes[20:220]) / 200
        
        print("\n=== 이동평균선 체크 ===")
        print(f"현재가: ${price:.2f}")
        print(f"50일선: ${ma50:.2f}")
        print(f"150일선: ${ma150:.2f}")
        print(f"200일선: ${ma200:.2f}")
        
        conditions = {
            '가격 $10 이상': price >= 10,
            '거래량 100만주 이상': volume >= 1000000,
            '시가총액 5억달러 이상': marketCap >= 500000000,
            '52주 고가의 75% 이상': price_to_high_ratio >= 75,
            '현재가 > MA150': price > ma150,
            '현재가 > MA200': price > ma200,
            'MA150 > MA200': ma150 > ma200,
            'MA200 상승추세': ma200 > ma200_prev,
            'MA50 > MA150': ma50 > ma150,
            'MA50 > MA200': ma50 > ma200,
            '현재가 > MA50': price > ma50
        }
        
        print("\n=== 조건 만족 여부 ===")
        for condition, result in conditions.items():
            print(f"{condition}: {'✅' if result else '❌'}")
            
        return all(conditions.values())
        
    except Exception as e:
        print(f"⚠️ 조건 체크 중 오류 발생: {str(e)}")
        return False

def main():
    symbol = 'HIMS'
    print(f"\n🔍 {symbol} 분석 시작...")
    
    # 1. 히스토리 데이터 가져오기
    historical_data = get_historical_data(symbol)
    if not historical_data:
        return
        
    # 2. 현재 시세 데이터 가져오기
    quote_data = get_quote(symbol)
    if not quote_data:
        return
        
    # 3. RS 계산
    rs_value = calculate_rs(historical_data)
    
    # 4. 조건 체크
    passed = check_conditions(quote_data, historical_data)
    
    print(f"\n=== 최종 결과 ===")
    print(f"RS 값: {rs_value:.1f}" if rs_value else "RS 계산 실패")
    print(f"조건 만족 여부: {'✅' if passed else '❌'}")

if __name__ == "__main__":
    main()

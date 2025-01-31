import requests

POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"

def get_financials_debug(ticker: str):
    url = f"https://api.polygon.io/vX/reference/financials"
    params = {
        'apiKey': POLYGON_API_KEY,
        'ticker': ticker,
        'limit': 12,
        'timeframe': 'quarterly',
        'order': 'desc',
        'sort': 'period_of_report_date',
        'include_sources': True
    }
    
    try:
        response = requests.get(url, params=params)
        print(f"\n=== {ticker} 재무데이터 분석 ===")
        print(f"상태코드: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            print(f"데이터 개수: {len(results)}")
            
            if results:
                print("\n재무제표 구조 분석:")
                first_result = results[0]
                financials = first_result.get('financials', {})
                
                print("\n=== 주요 재무지표 존재 여부 ===")
                income_stmt = financials.get('income_statement', {})
                print("EPS:", 'diluted_earnings_per_share' in income_stmt)
                print("영업이익:", 'operating_income_loss' in income_stmt)
                print("매출액:", 'revenues' in income_stmt)
                
                print("\n=== 분기별 데이터 ===")
                for r in results:
                    period = r.get('period_of_report_date', 'N/A')
                    fin = r.get('financials', {}).get('income_statement', {})
                    eps = fin.get('diluted_earnings_per_share', {}).get('value', 'N/A')
                    rev = fin.get('revenues', {}).get('value', 'N/A')
                    print(f"Period: {period}, EPS: {eps}, Revenue: {rev}")
            
            return data
    except Exception as e:
        print(f"에러 발생: {str(e)}")
        return None

# CLS 테스트
get_financials_debug('CLS')

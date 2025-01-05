import requests
from datetime import datetime
from airtable import Airtable
import time

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

# 종목 데이터를 가져올 때 사용할 분할 구간
SYMBOL_RANGES = [('A', 'F'), ('G', 'L'), ('M', 'R'), ('S', 'Z')]

def fetch_all_stocks():
    """알파벳 범위로 데이터를 분할 요청하여 모든 종목 데이터 수집"""
    all_data = []
    url = "https://financialmodelingprep.com/api/v3/stock-screener"

    for start, end in SYMBOL_RANGES:
        params = {
            'apikey': FMP_API_KEY,
            'exchange': 'NYSE,NASDAQ',
            'limit': 1000,  # 한 번에 최대 1000개 요청 가능
            'symbol': f'{start}-{end}'
        }

        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                all_data.extend(response.json())
                print(f"{start}-{end} 구간 데이터 수집 완료")
            else:
                print(f"{start}-{end} 구간 데이터 수집 실패: {response.status_code}")
            time.sleep(0.2)  # API 호출 제한을 고려한 딜레이
        except Exception as e:
            print(f"데이터 수집 중 에러 발생 ({start}-{end}): {str(e)}")

    return all_data

def get_52_week_high_companies(all_data):
    """52주 신고가 상위 기업 데이터 필터링"""
    # 52주 신고가 비율 계산
    for company in all_data:
        price = float(company.get('price', 0) or 0)
        year_high = float(company.get('yearHigh', 1) or 1)  # 기본값 1로 설정 (0으로 나누기 방지)
        company['high_ratio'] = (price / year_high) * 100 if year_high > 0 else 0

    # 52주 신고가 비율 기준 정렬 후 상위 20개 선택
    sorted_data = sorted(all_data, key=lambda x: x['high_ratio'], reverse=True)
    return sorted_data[:20]

def update_airtable(companies):
    """Airtable에 데이터 업데이트"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")

    for company in companies:
        try:
            record = {
                '티커': company['symbol'],
                '종목명': company.get('companyName', ''),
                '현재가': float(company.get('price', 0) or 0),
                '52주 신고가': float(company.get('yearHigh', 0) or 0),
                '신고가 비율(%)': float(company.get('high_ratio', 0) or 0),
                '등락률': float(company.get('changesPercentage', 0) or 0),
                '거래량': int(company.get('volume', 0) or 0),
                '시가총액': float(company.get('marketCap', 0) or 0),
                '거래소 정보': company.get('exchange', ''),
                '업데이트 시간': current_date,
                '분류': "52주 신고가 상위"
            }

            airtable.insert(record)
            print(f"데이터 추가 완료: {company['symbol']}")
            time.sleep(0.2)  # Airtable API 제한 고려

        except Exception as e:
            print(f"Airtable 업데이트 중 에러 ({company['symbol']}): {str(e)}")

def main():
    print("52주 신고가 상위 데이터 수집 시작...")

    # 모든 종목 데이터 수집
    start_time = time.time()
    all_data = fetch_all_stocks()
    if not all_data:
        print("데이터 수집 실패")
        return

    print(f"\n전체 종목 데이터 수집 완료: {len(all_data)}개")

    # 상위 20개 데이터 필터링
    top_companies = get_52_week_high_companies(all_data)
    print(f"\n상위 20개 52주 신고가 기업 데이터 필터링 완료")

    # Airtable 업데이트
    print("\nAirtable 업데이트 시작...")
    update_airtable(top_companies)

    elapsed_time = time.time() - start_time
    print(f"\n모든 작업 완료! 총 소요 시간: {elapsed_time / 60:.2f}분")

if __name__ == "__main__":
    main()

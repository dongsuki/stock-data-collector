import requests

API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"

# Gainers API 테스트
gainers_url = f"https://financialmodelingprep.com/api/v3/stock_market/gainers?apiKey={API_KEY}"
response = requests.get(gainers_url)
print("Gainers API 응답:", response.status_code)
if response.status_code == 200:
    print("데이터 샘플:", response.json()[:2])  # 첫 2개 데이터만 출력

# Actives API 테스트
actives_url = f"https://financialmodelingprep.com/api/v3/stock_market/actives?apiKey={API_KEY}"
response = requests.get(actives_url)
print("\nActives API 응답:", response.status_code)
if response.status_code == 200:
    print("데이터 샘플:", response.json()[:2])

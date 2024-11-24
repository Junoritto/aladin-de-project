import requests
import json
import os
from datetime import datetime

# 알라딘 API 키 설정
TTBKey = "ttbdlwnsgh1071623001"
BASE_URL = "http://www.aladin.co.kr/ttb/api/ItemList.aspx"

# 현재 파일 위치를 기준으로 경로 설정
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(CURRENT_DIR, "../data/raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_books(ttb_key, query_type="ItemNewSpecial", max_results=50, start=1, output_format="JS"):
    """
    알라딘 API를 호출하여 서적 리스트를 가져오는 함수.
    """
    params = {
        "ttbkey": ttb_key,
        "QueryType": query_type,
        "MaxResults": max_results,
        "start": start,
        "SearchTarget": "Book",
        "output": output_format,
        "Version": "20131101"
    }
    
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json() if output_format == "JS" else response.text
    else:
        print(f"API 호출 실패: {response.status_code}")
        return None
    
def save_to_file(data, output_dir, filename):
    """
    데이터를 JSON 파일로 저장하는 함수.
    """
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"파일 저장 완료: {filepath}")

def main():
    all_books = []  # 모든 서적 데이터를 저장할 리스트
    start = 1       # 초기 start 값
    max_results = 50
    batch = 1       # 배치 번호
    max_batches = 5 # 최대 배치 수 설정
    output_format = "JS"  # JSON 형식으로 데이터 가져오기
    
    while batch <= max_batches:
        print(f"API 호출: batch={batch}/{max_batches}")
        books = fetch_books(TTBKey, max_results=max_results, start=batch, output_format=output_format)
        
        # API 호출 실패 처리
        if not books or "item" not in books:
            print("더 이상 데이터가 없습니다. 호출 종료.")
            break
        
        # 응답 데이터 추가
        all_books.extend(books["item"])
        
        # 데이터 저장 (저장 필요한 경우에 주석 해제하고 사용)
        # filename = f"books_batch_{batch}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        # save_to_file(books["item"], OUTPUT_DIR, filename)
        
        # 다음 호출 준비
        batch += 1
    
    # 전체 데이터 저장 (저장 필요한 경우에 주석 해제하고 사용)
    # full_data_file = f"all_books_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    # save_to_file(all_books, OUTPUT_DIR, full_data_file)

if __name__ == "__main__":
    main()
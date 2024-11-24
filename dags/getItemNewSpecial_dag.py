from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import os
import json

# 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 알라딘 API 설정
TTBKey = "ttbdlwnsgh1071623001"
BASE_URL = "http://www.aladin.co.kr/ttb/api/ItemList.aspx"
RAW_DATA_DIR = "../data/raw"
S3_BUCKET = "your-s3-bucket-name"  # S3 버킷 이름

@dag(
    dag_id="getItemNewSpecial_dag",
    default_args=default_args,
    description="Extract data from Aladin API and upload to S3",
    schedule_interval="@daily",  # 매일 자정 실행
    start_date=datetime(2023, 11, 22),
    catchup=False,
    tags=["extract", "api", "s3"],
)
def extract():
    @task
    def fetch_books(ttb_key, max_results=50, max_batches=5):
        """
        알라딘 API를 호출하여 데이터를 JSON 파일로 저장.
        """
        all_books = []
        os.makedirs(RAW_DATA_DIR, exist_ok=True)  # 로컬 저장 경로 생성
        for batch in range(1, max_batches + 1):
            print(f"API 호출: batch={batch}/{max_batches}")
            params = {
                "ttbkey": ttb_key,
                "QueryType": "ItemNewSpecial",
                "MaxResults": max_results,
                "start": batch,
                "SearchTarget": "Book",
                "output": "JS",
                "Version": "20131101"
            }
            response = requests.get(BASE_URL, params=params)
            if response.status_code != 200 or "item" not in response.json():
                print("데이터 수집이 중단되었습니다.")
                break
            books = response.json()["item"]
            all_books.extend(books)

            # 배치별 JSON 저장
            filename = f"books_batch_{batch}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
            with open(os.path.join(RAW_DATA_DIR, filename), "w", encoding="utf-8") as f:
                json.dump(books, f, ensure_ascii=False, indent=4)
            print(f"로컬에 저장 완료: {filename}")

        return all_books  # 전체 데이터를 다음 Task로 전달

    @task
    def upload_to_s3(book_data):
        """
        수집된 데이터를 S3에 업로드.
        """
        hook = S3Hook(aws_conn_id="aws_default")
        filename = f"all_books_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        local_path = os.path.join(RAW_DATA_DIR, filename)

        # 데이터를 로컬 파일로 저장
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(book_data, f, ensure_ascii=False, indent=4)

        # S3 업로드
        s3_key = f"raw/{filename}"
        hook.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )
        print(f"S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")

    # Task 연결
    books = fetch_books(TTBKey)  # 데이터를 API에서 가져옴
    # upload_to_s3(books)          # S3로 업로드 (향후 활성화)

extract()
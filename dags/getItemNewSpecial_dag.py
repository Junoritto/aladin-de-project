from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
from airflow.models import Variable
import requests
import os
import pytz
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
TTBKey = Variable.get("TTBKey")
BASE_URL = "http://www.aladin.co.kr/ttb/api/ItemList.aspx"
RAW_DATA_DIR = "../data/raw"
BUCKET_NAME = "de3-aladin-bucket"  # S3 버킷 이름
S3_KEY_PREFIX = "new/"          # S3 저장 경로


# Snowflake 테이블 정보
SNOWFLAKE_TABLE = "item_new_special"
SNOWFLAKE_SCHEMA = "raw_data"
SNOWFLAKE_DATABASE = "dev"

# 필요한 컬럼 정의
REQUIRED_COLUMNS = [
    "title", "link", "author", "pubDate", "itemId",
    "priceSales", "priceStandard", "cover", "categoryName",
    "publisher", "salesPoint", "customerReviewRank"
]

@dag(
    dag_id="getItemNewSpecial_dag",
    default_args=default_args,
    description="Extract data from Aladin API and upload to S3",
    schedule_interval="@daily",  # 매일 자정 실행
    start_date=datetime(2023, 11, 22),
    catchup=False,
    tags=["extract", "transform", "load", "api", "s3", "snowflake"],
)
def pipeline():
    @task
    def fetch_books(ttb_key, max_results=50, max_batches=20):
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

        return all_books  # 전체 데이터를 다음 Task로 전달

    @task
    def transform_books(book_data):
        """
        JSON 데이터에서 필요한 컬럼만 필터링.
        """
        # 필터링된 데이터를 저장
        transformed_books = []

        for book in book_data:
            # 필요한 컬럼만 추출
            filtered_book = {col: book.get(col) for col in REQUIRED_COLUMNS}
            transformed_books.append(filtered_book)

        print(f"변환된 데이터 개수: {len(transformed_books)}")
        return transformed_books
    
    @task
    def process_category(book_data):
        """
        categoryName에서 두 번째 부분만 추출
        """
        processed_books = []

        for book in book_data:
            # CATEGORYNAME 컬럼에서 '>'로 나누고 두 번째 부분만 추출
            category_full = book.get("categoryName", "")
            category_parts = category_full.split(">")
            if len(category_parts) > 1:
                book["categoryName"] = category_parts[1].strip()  # 두 번째 부분으로 대체
            else:
                book["categoryName"] = "Unknown"  # 데이터가 없을 경우 기본값 설정

            processed_books.append(book)

        print(f"카테고리 전처리 완료된 데이터 개수: {len(processed_books)}")
        return processed_books

    @task
    def upload_to_s3(book_data):
        """
        수집된 데이터를 S3에 업로드.
        """
        # Airflow Connection ID 설정
        aws_conn_id = "aws_default"

        # S3 Hook 인스턴스 생성
        hook = S3Hook(aws_conn_id=aws_conn_id)

        # 한국 시간 설정
        kst = pytz.timezone("Asia/Seoul")
        now_kst = datetime.now(kst)
        
        # 파일 이름 설정
        filename_date = now_kst.strftime("%Y%m%d")
        filename = f"transformed_books_{filename_date}.json"

        s3_key = f"{S3_KEY_PREFIX}{filename}"

        # S3 업로드
        hook.load_string(
            string_data=json.dumps(book_data, ensure_ascii=False),  # 데이터를 JSON으로 직렬화
            key=f"{S3_KEY_PREFIX}{filename}",  # S3 내 경로
            bucket_name=BUCKET_NAME,  # S3 버킷 이름
            replace=True  # 기존 파일 덮어쓰기
        )

        print(f"S3 업로드 완료: s3://{BUCKET_NAME}/{S3_KEY_PREFIX}{filename}")

        # 업로드된 S3 파일의 Key를 반환
        return s3_key
    
    @task
    def load_to_snowflake(s3_key):
        """
        S3에서 데이터를 읽어 Snowflake에 적재
        """
        # S3에서 데이터 다운로드
        aws_conn_id = "aws_default"
        hook = S3Hook(aws_conn_id=aws_conn_id)

        print(f"S3에서 다운로드: {s3_key}")
        data = hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)

        # JSON 데이터 파싱
        book_data = json.loads(data)
        print(f"다운로드된 데이터 개수: {len(book_data)}")

        snowflake_conn_id = "SNOWFLAKE_CONN_ID"

        # Snowflake Hook 생성
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        # Snowflake 테이블 초기화
        truncate_sql = "TRUNCATE TABLE RAW_DATA.ITEM_NEW_SPECIAL"
        snowflake_hook.run(truncate_sql)

        # AWS 자격 증명 가져오기
        aws_credentials = hook.get_credentials()
        aws_access_key = aws_credentials.access_key
        aws_secret_key = aws_credentials.secret_key

        # INSERT 쿼리 생성
        copy_sql = f"""
        COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
        FROM s3://{BUCKET_NAME}/{s3_key}
        CREDENTIALS = (AWS_KEY_ID='{aws_access_key}' AWS_SECRET_KEY='{aws_secret_key}')
        FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE'
        """

        # 데이터 적재
        try:
            snowflake_hook.run(copy_sql)
            print("Snowflake 데이터 적재 완료")
        except Exception as e:
            print(f"Snowflake 데이터 적재 중 오류 발생: {e}")
            raise


    # Task 연결
    books = fetch_books(TTBKey)  # 데이터를 API에서 가져옴
    transformed_books = transform_books(books)  # 데이터를 변환
    processed_books = process_category(transformed_books)
    s3_key = upload_to_s3(processed_books)  # 변환된 데이터를 S3로 업로드
    load_to_snowflake(s3_key)  # 변환된 데이터를 Snowflake에 적재

pipeline()
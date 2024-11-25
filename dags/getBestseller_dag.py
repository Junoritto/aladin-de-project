from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import boto3
import json

# 환경 변수에서 API 키와 AWS S3 정보 가져오기
API_KEY = os.getenv("AIRFLOW_VAR_API_KEY")
BUCKET_NAME = "de3-aladin-bucket" # 버킷 이름 수정
S3_KEY_PREFIX = "bestsellers/"


def fetch_bestseller_data():
    """
    Fetch bestseller data from Aladin API.
    """
    all_data = []
    max_results = 50  # API에서 한 번에 가져올 최대 데이터 수
    total_pages = 20  # API 매뉴얼에 따라 가져올 페이지 수

    for page in range(1, total_pages + 1):
        # API 호출 파라미터 설정
        params = {
            "ttbkey": API_KEY,
            "QueryType": "Bestseller",
            "SearchTarget": "Book",
            "MaxResults": max_results,
            "start": page,
            "Output": "JS",
            "Version": "20131101",
        }
        response = requests.get("http://www.aladin.co.kr/ttb/api/ItemList.aspx", params=params)
        response.raise_for_status()
        data = response.json().get("item", [])

        if not data:
            print(f"No more data found at page {page}. Stopping fetch.")
            break

        all_data.extend(data)
        print(f"Fetched {len(data)} items from page {page}. Total fetched so far: {len(all_data)}")

    print(f"Total items fetched: {len(all_data)}")
    return all_data


def save_to_s3(data, **kwargs):
    """Save fetched data to AWS S3."""
    aws_conn_env = os.getenv("AIRFLOW_CONN_AWS_CONN_ID")
    if not aws_conn_env:
        raise ValueError("환경 변수 'AIRFLOW_CONN_AWS_CONN_ID'가 설정되지 않았습니다.")

    # AWS 연결 정보 파싱
    try:
        conn_info = aws_conn_env.split("//")[1]
        aws_access_key, remaining = conn_info.split(":", 1)
        aws_secret_key, extra = remaining.split("@", 1)
        region_name = "us-west-2"  # 필요 시 수정 가능
    except Exception as e:
        raise ValueError(f"'AIRFLOW_CONN_AWS_CONN_ID'의 포맷이 올바르지 않습니다: {aws_conn_env}") from e
    
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name,
    )
    file_name = f"{S3_KEY_PREFIX}bestseller.json" # 파일명 고정
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(data, ensure_ascii=False),
    )
    print(f"Data saved to S3 bucket {BUCKET_NAME}/{file_name}")


# Airflow DAG 정의
with DAG(
    dag_id="aladin_bestseller_to_s3",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Fetch Aladin bestseller data and save to S3",
    schedule_interval="@daily",
    start_date=datetime(2023, 11, 26),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_bestseller_data",
        python_callable=fetch_bestseller_data,
    )

    save_task = PythonOperator(
        task_id="save_to_s3",
        python_callable=save_to_s3,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_bestseller_data') }}"],
    )

    fetch_task >> save_task

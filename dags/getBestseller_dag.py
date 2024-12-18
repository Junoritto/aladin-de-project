from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import boto3
import json
from snowflake.connector import connect
from urllib.parse import urlparse, parse_qs
import html


# 환경 변수에서 API 키와 Snowflake 연결 정보 가져오기
API_KEY = os.getenv("AIRFLOW_VAR_API_KEY")
BUCKET_NAME = "de3-aladin-bucket"  # S3 버킷 이름
S3_KEY_PREFIX = "bestsellers/"  # S3 키 경로
SNOWFLAKE_CONN_STR = os.getenv("AIRFLOW_CONN_SNOWFLAKE_CONN_ID", "").strip()

def parse_snowflake_connection(uri):
    """
    Parse Snowflake URI and extract connection parameters.
    """
    if not uri.startswith("snowflake://"):
        raise ValueError(f"Invalid Snowflake URI: {uri}")

    parsed_url = urlparse(uri)

    # 사용자 및 비밀번호
    user = parsed_url.username
    password = parsed_url.password
    if not user or not password:
        raise ValueError("Snowflake URI parsing error: Missing user or password.")

    # 계정(account) 및 스키마(schema) 처리
    path = parsed_url.path.lstrip("/")  # "/raw_data" -> "raw_data"
    account = parse_qs(parsed_url.query).get("account", [None])[0]
    schema = path or parse_qs(parsed_url.query).get("schema", [None])[0]

    # 추가 쿼리 파라미터
    query_params = parse_qs(parsed_url.query)
    database = query_params.get("database", [None])[0]
    warehouse = query_params.get("warehouse", [None])[0]

    return {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
    }

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

        # JSON 데이터 전처리
        for item in data:
            # 모든 value를 안전하게 변환
            processed_item = {
                key.upper(): (
                    html.escape(str(value)).replace('"', '') if isinstance(value, str) else value
                )
                for key, value in item.items()
            }

            # CATEGORYNAME 처리
            category_name = processed_item.get("CATEGORYNAME", "")
            processed_category_name = category_name.split("&gt;")[-3] if "&gt;" in category_name else category_name

            # 필요한 컬럼만 필터링하여 추가
            all_data.append({
                "TITLE": processed_item.get("TITLE", ""),
                "LINK": processed_item.get("LINK", ""),
                "AUTHOR": processed_item.get("AUTHOR", ""),
                "PUBDATE": processed_item.get("PUBDATE", ""),
                "ITEMID": processed_item.get("ITEMID", 0),
                "PRICESALES": processed_item.get("PRICESALES", 0),
                "PRICESTANDARD": processed_item.get("PRICESTANDARD", 0),
                "COVER": processed_item.get("COVER", ""),
                "CATEGORYNAME": processed_category_name,  # 수정된 CATEGORYNAME 저장
                "PUBLISHER": processed_item.get("PUBLISHER", ""),
                "SALESPOINT": processed_item.get("SALESPOINT", 0),
                "CUSTOMERREVIEWRANK": processed_item.get("CUSTOMERREVIEWRANK", 0),
                "BESTRANK": processed_item.get("BESTRANK", 0),
            })

        print(f"Fetched {len(data)} items from page {page}. Total fetched so far: {len(all_data)}")

    print(f"Total items fetched: {len(all_data)}")
    return all_data




def save_to_s3(data, **kwargs):
    """Save fetched data to AWS S3 in a valid JSON format."""
    aws_conn_env = os.getenv("AIRFLOW_CONN_AWS_CONN_ID")
    if not aws_conn_env:
        raise ValueError("환경 변수 'AIRFLOW_CONN_AWS_CONN_ID'가 설정되지 않았습니다.")

    try:
        conn_info = aws_conn_env.split("//")[1]
        aws_access_key, remaining = conn_info.split(":", 1)
        aws_secret_key, extra = remaining.split("@", 1)
        region_name = "ap-northeast-2"
    except Exception as e:
        raise ValueError(f"'AIRFLOW_CONN_AWS_CONN_ID'의 포맷이 올바르지 않습니다: {aws_conn_env}") from e

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name,
    )

    # 데이터가 리스트인지 확인하고 JSON으로 변환
    if isinstance(data, list):
        # 리스트를 Line-delimited JSON으로 변환
        transformed_data = "\n".join(json.dumps(record, ensure_ascii=False) for record in data)
    elif isinstance(data, str):
        # 문자열이 작은따옴표를 사용한 경우 큰따옴표로 변환
        transformed_data = data.replace("'", '"')  # 작은따옴표를 큰따옴표로 변환
    else:
        raise ValueError("데이터가 잘못된 형식입니다. JSON 배열 또는 문자열이어야 합니다.")

    file_name = f"{S3_KEY_PREFIX}bestseller.json"

    # S3에 데이터 저장
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=transformed_data,
    )
    print(f"Data saved to S3 bucket {BUCKET_NAME}/{file_name}")





def load_to_snowflake(s3_key, **kwargs):
    """
    Load JSON data from S3 to Snowflake without using a stage.
    """
    connection_params = parse_snowflake_connection(SNOWFLAKE_CONN_STR)

    aws_conn_env = os.getenv("AIRFLOW_CONN_AWS_CONN_ID")
    if not aws_conn_env:
        raise ValueError("환경 변수 'AIRFLOW_CONN_AWS_CONN_ID'가 설정되지 않았습니다.")
    
    conn_info = aws_conn_env.split("//")[1]
    aws_access_key, remaining = conn_info.split(":", 1)
    aws_secret_key, _ = remaining.split("@", 1)

    conn = connect(
        user=connection_params["user"],
        password=connection_params["password"],
        account=connection_params["account"],
        database=connection_params["database"],
        schema=connection_params["schema"],
        warehouse=connection_params["warehouse"],
    )
    cursor = conn.cursor()

    cursor.execute(f"USE DATABASE {connection_params['database']};")
    cursor.execute(f"USE SCHEMA {connection_params['schema']};")

    truncate_sql = "TRUNCATE TABLE RAW_DATA.BESTSELLER;"
    cursor.execute(truncate_sql)
    print("Table RAW_DATA.BESTSELLER truncated.")

    copy_sql = f"""
    COPY INTO RAW_DATA.BESTSELLER
    FROM 's3://{BUCKET_NAME}/{s3_key}'
    CREDENTIALS = (AWS_KEY_ID='{aws_access_key}' AWS_SECRET_KEY='{aws_secret_key}')
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE';
    """
    try:
        cursor.execute(copy_sql)
        conn.commit()
        print("Data loaded into Snowflake table RAW_DATA.BESTSELLER.")
    except Exception as e:
        print(f"Error during COPY INTO: {e}")
    finally:
        cursor.close()
        conn.close()




# Airflow DAG 정의
with DAG(
    dag_id="aladin_bestseller_pipeline",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Fetch Aladin bestseller data, save to S3, and load into Snowflake",
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
        op_args=["{{ ti.xcom_pull(task_ids='fetch_bestseller_data') }}"],  # XCom으로 데이터 전달
    )

    load_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
        op_args=["bestsellers/bestseller.json"],  # S3 파일 키 전달
    )

    fetch_task >> save_task >> load_task
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.utils.dates import days_ago

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='test_aws_connection',
    default_args=default_args,
    description='Test AWS connection using S3',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['test', 'aws'],
) as dag:

    # S3 버킷에서 객체 리스트 가져오기
    list_s3_objects = S3ListOperator(
        task_id='list_s3_objects',
        aws_conn_id='AWS_CONN_ID',  # 생성한 AWS 커넥션 ID
        bucket='de3-aladin-bucket',  # 테스트할 S3 버킷 이름
    )

    list_s3_objects

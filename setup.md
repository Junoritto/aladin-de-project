# Airflow 및 Docker 환경 세팅, S3 및 Snowflake 연결 가이드


1. 해당 브랜치의 repo를 clone
```bash
git clone --branch feature/airflow-docker-setup --single-branch https://github.com/Junoritto/aladin-de-project.git
```

2. clone한 repo 폴더로 진입
```bash
cd aladin-de-project
```

4. 해당 폴더에 .env 파일을 추가(따로 공유드릴 예정)

5. docker compose 실행
```bash
docker-compose up
```

6. https://localhost:8080 으로 웹 UI에 접속, 또는 Dag 테스트

### S3 접속 가이드
- s3 관련 오퍼레이터 작성 시 다음과 같이 파라미터 설정

1. PythonOperator 사용 시
```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# task가 될 함수 내에 아래 내용 작성
hook = S3Hook(aws_conn_id="AWS_CONN_ID")
json_file= s3_hook.read_key(key="json의 파일 경로", bucket_name="de3-aladin-bucket")
data = json.loads(json_file)
```

2. S3 관련 Operator 사용 시
```python
# Operator 내 매개변수로 전달
aws_conn_id='AWS_CONN_ID',  # AWS 커넥션 ID
bucket='de3-aladin-bucket' # 버킷 이름
```

### 알라딘 API KEY 사용 가이드
- .env 파일에 이미 내장되어 있으며 별도로 설정할 필요 없음

```python
from airflow.models import Variable
api_key = Variable.get("API_KEY") # 이 값을 url에 파라미터로 넘겨 호출하는 식으로 활용
```

### Snowflake 접속 가이드
- snowflake 관련 오퍼레이터 작성 시 다음과 같이 파라미터 설정

1. PythonOperator 사용 시
```python
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# task가 될 함수 내에 아래 내용 작성
hook = SnowflakeHook(snowflake_conn_id="SNOWFLAKE_CONN_ID")
conn = hook.get_conn()
cursor = conn.cursor()
cursor.excute("실행할 sql 쿼리문")
```

2. SnowflakeOperator 사용 시
```python
task = SnowflakeOperator(
    task_id="load_task",
    snowflake_conn_id="SNOWFLAKE_CONN_ID", #다른 snowflake 관련 operator에도 해당 매개변수를 적용
    sql="실행할 sql 쿼리문",
)
```
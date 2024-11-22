# Airflow 및 Docker 환경 세팅, S3 및 Snowflake 연결 가이드

1. 해당 브랜치의 repo를 clone
```bash
git clone --branch feature/airflow-docker-setup --single-branch https://github.com/Junoritto/aladin-de-project.git
```

2. clone한 repo 폴더로 진압
```bash
cd aladin-de-project
```

4. 해당 폴더에 .env 파일을 추가(따로 공유드릴 예정)

5. docker compose 실행
```bash
docker-compose up
```

6. https://localhost:8080 으로 웹 UI에 접속, 또는 Dag 테스트

# S3 접속 가이드
- s3 관련 오퍼레이터 작성 시 다음과 같이 파라미터 설정
```python
aws_conn_id='AWS_CONN_ID',  # AWS 커넥션 ID
bucket='de3-aladin-bucket' # 버킷 이름
```

# 알라딘 API KEY 사용 가이드
- .env 파일에 이미 내장되어 있으며 별도로 설정할 필요 없음

```python
from airflow.models import Variable
api_key = Variable.get("API_KEY") # 이 값을 url에 파라미터로 넘겨 호출하는 식으로 활용
```

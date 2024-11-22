# 알라딘 서점 api를 통한 중고서적 트렌드 분석 및 정보 시각화

### 프로젝트 폴더 구조
```
project/
├── dags/
├── data/
│   ├── raw/
│   ├── processed/
├── logs/
├── plugins/
├── scripts/
└── docker-compose.yml
```

#### 폴더 상세 설명
1. dags/ (DAG 스크립트 폴더)
- 용도 :
    - Airflow에서 실행할 DAG(Pipeline)을 작성하는 Python 스크립트를 저장
    - 각 DAG는 워크플로우(Task 간의 의존성, 실행 순서 등)를 정의

2. data/ (데이터 저장 폴더)
- 용도:
    - 로컬 환경에서 데이터를 저장하고 관리
    - raw/와 processed(?)/로 나누어 데이터 상태를 구분

3. logs/ (로그 저장 폴더)
- 용도:
    - Airflow 실행 시 발생하는 로그 파일 저장
    - DAG 실행 성공/실패 내역과 디버깅에 필요한 정보 포함

4. plugins/ (Airflow 확장용 플러그인 폴더)
- 용도:
    - Airflow의 기본 제공 기능 외에 사용자 정의 플러그인, 훅(Hook), 오퍼레이터 저장
    - 복잡한 작업에 대해 재사용 가능한 코드 작성

5. scripts/ (독립 실행 스크립트 폴더)
- 용도:
    - Airflow 외부에서 테스트하거나 독립적으로 실행 가능한 Python 스크립트 저장
    - 초기 개발 단계에서 DAG 없이 직접 실행하며 기능을 테스트할 때 사용

6. docker-compose.yml (Docker Compose 설정 파일)
- 용도:
    - Airflow와 관련 구성 요소를 Docker로 실행하기 위한 설정
    - Airflow, 웹 서버, 스케줄러, 워커 등 모든 컨테이너를 한 번에 실행 가능

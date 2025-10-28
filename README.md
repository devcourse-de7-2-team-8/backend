# 전기차 충전소 데이터 시각화 대시보드

이 프로젝트는 전기차 충전소 데이터를 시각화하여 사용자에게 유용한 정보를 제공하는 대시보드를 개발하는 것을 목표로 합니다.  
대시보드는 충전소 위치, 사용 현황, 충전 속도 등 다양한 데이터를 시각적으로 표현하여 사용자 경험을 향상시킵니다.

## Setup DATA

```bash
uv sync
uv run devcourse-project2
```

### dependencies

- **uv:** for package management

### Project Structure

```
project
├── README.md
├── data
│   ├── sessions
│   └── stations
├── pyproject.toml
├── src
│   ├── adhoc
│   ├── common
│   ├── devcourse_project2 -- main package
│   │   └── __init__.py
│   ├── pipelines
│   ├── tasks
│   └── utils
└── uv.lock
```

### .env file Structure (located in /)

```
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
S3_BUCKET_NAME=
```


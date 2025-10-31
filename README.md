# ⚡서울시 소유 전기차 충전소 데이터 시각화 대시보드 

<p align="center">
  <img src="https://github.com/Lepus0T/report/blob/main/dashboard_visual.png?raw=true" 
       width="400" 
       alt="Dashboard Visualization">
</p>

## <p align="center">💡프로젝트 목표 </p>
이 프로젝트는 서울시 소유 전기차 충전소 데이터를 시각화하여 사용자에게 유용한 정보를 제공하는 대시보드를 개발하는 것을 목표로 합니다.  
대시보드는 충전소 위치, 사용 현황, 충전 이용 등 다양한 데이터를 시각적으로 표현하여 사용자 경험을 향상시킵니다.

## <p align="center">⚙️기술 및 프레임워크 </p>
### Data Collection
전기차 충전 기록정보

[서울시 소유 전기차충전기 일별 시간별 충전현황(2024년).xlsx](https://github.com/user-attachments/files/23251371/2024.xlsx)

[서울시 소유 전기차 충전기 일별 시간별 충전현황(8월말까지) (1).xlsx](https://github.com/user-attachments/files/23251373/8.1.xlsx)

전기차 충전소 위치정보

[서울시 전기차 충전소 정보(8월말 기준).xlsx](https://github.com/user-attachments/files/23251375/8.xlsx)

### Data Preprocessing
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
<img src="https://img.shields.io/badge/ApacheParquet-50ABF1?style=for-the-badge&logo=ApachParquet&logoColor=black">

### Data Lake
![Amazon S3](https://img.shields.io/badge/Amazon%20S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white)

### Warehouse
![Snowflake](https://img.shields.io/badge/snowflake-%2329B5E8.svg?style=for-the-badge&logo=snowflake&logoColor=white)

### Visualization
<img src="https://img.shields.io/badge/ApacheSuperset-20A6C9?style=for-the-badge&logo=ApacheSuperset&logoColor=black">

### Team Management
![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)
![Notion](https://img.shields.io/badge/Notion-%23000000.svg?style=for-the-badge&logo=notion&logoColor=white)

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
SNOWFLAKE_SCHEMA_RAW=
SNOWFLAKE_SCHEMA_ANALYTICS=
SNOWFLAKE_SCHEMA_PUBLIC=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
S3_BUCKET_NAME=
```


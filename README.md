# <p align="center">⚡서울시 소유 전기차 충전소 데이터 시각화 대시보드 </p>

<p align="center">
  <img src="https://github.com/Lepus0T/report/blob/main/dashboard_visual.png?raw=true" 
       width="400"
       height="300"
       alt="Dashboard Visualization">
</p>

## ⚙️ Setup DATA
> 프로젝트 실행 및 환경 세팅을 위한 명령어입니다.

```bash
uv sync            # 의존성 설치
uv run devcourse-project2   # 메인 파이프라인 실행
```
### 🌿 .env file Structure (located in project root)

```bash
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
## <p align="center">💡프로젝트 목표 </p>
- 이 프로젝트는 서울시 소유 전기차 충전소 데이터를 시각화하여 사용자에게 유용한 정보를 제공하는 대시보드를 개발하는 것을 목표로 합니다.  
- 대시보드는 충전소 위치, 사용 현황, 충전 이용 등 다양한 데이터를 시각적으로 표현하여 사용자 경험을 향상시킵니다.

## <p align="center">⚙️기술 및 프레임워크 </p>
### 📂Data Collection
- **서울시 전기차 충전 기록 정보**
  - [서울시 소유 전기차충전기 일별 시간별 충전현황(2024년).xlsx](https://github.com/user-attachments/files/XXXXX/2024.xlsx)
  - [서울시 소유 전기차 충전기 일별 시간별 충전현황(8월말까지).xlsx](https://github.com/user-attachments/files/YYYY/8m.xlsx)
- **서울시 전기차 충전소 위치 정보**
  - [서울시 전기차 충전소 정보(8월말 기준).xlsx](https://github.com/user-attachments/files/ZZZZ/station.xlsx)
> 📊 서울 열린데이터광장에서 수집한 원본 데이터로, 이후 Pandas를 통해 전처리 및 Parquet 변환


### 🧹Data Preprocessing
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
<img src="https://img.shields.io/badge/ApacheParquet-50ABF1?style=for-the-badge&logo=ApachParquet&logoColor=black">

### ☁️Data Lake
![Amazon S3](https://img.shields.io/badge/Amazon%20S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white)

### 🧊Data Warehouse
![Snowflake](https://img.shields.io/badge/snowflake-%2329B5E8.svg?style=for-the-badge&logo=snowflake&logoColor=white)

### 📊Visualization
<img src="https://img.shields.io/badge/ApacheSuperset-20A6C9?style=for-the-badge&logo=ApacheSuperset&logoColor=black">

### Team Management
![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)
![Notion](https://img.shields.io/badge/Notion-%23000000.svg?style=for-the-badge&logo=notion&logoColor=white)
<img src="https://img.shields.io/badge/uv-DE5FE9?style=for-the-badge&logo=uv&logoColor=black">

## <p align="center">📝기술 구조도 및 ERD</p>

<div align="center" style="display: flex; justify-content: center; gap: 10px;">
  <img src="https://github.com/Lepus0T/report/blob/main/stack_structure.png?raw=true" width="48%" alt="기술 구조도">
  <img src="https://github.com/Lepus0T/report/blob/main/image%20(2).png?raw=true" width="48%" alt="ERD">
</div>

## <p align="center"> 👨‍💻프로젝트 세부 내용 및 구조</p>
## 데이터 수집 및 전처리

## S3 Structure
```
data/
├── stations/ # 충전소별 위치 정보
│ └── seoul_station.parquet
└── sessions/ # 충전소별 충전 기록 정보
├── year=2024/
│ └── ev_charging_session.parquet
└── year=2025/
└── ev_charging_session.parquet
```
- 서울시 전기차 충전소 및 충전 이력 데이터를 연도별·유형별로 분류해 S3에 저장.
- Parquet 포맷으로 변환되어 Snowflake Stage와 연동되어 효율적인 ETL이 가능.

## Medallion 아키텍처
```
[Raw Data] (S3)
├── stations/seoul_station.parquet
├── sessions/year=2024/session.parquet
└── sessions/year=2025/session.parquet
↓
[Bronze Layer] (Snowflake)
├── station_stg -- *_station.csv 테이블로 저장
└── session_stg -- Extenal_Table로 연결
↓
[Silver Layer] (Snowflake)
├── region_info
├── ev_charging_stations 
├── ev_charging_sessions
↓
[Gold Layer] - 시각화(대시보드) 테이블 (Snowflake)
├── REGION_MONTHLY_SESSION_SUMMARY
├── STATION_BY_GU
├── STATION_UTILIZATION_GU
├── AVG_KWH_BY_GU
```
- 원본 데이터(xlsx)를 수집 후 Pandas로 전처리하여 Parguet 형식으로 변환, AWS S3에 저장.
- Snowflake의 Stage를 통해 데이터를 적재.
- Bronze(S3 원시 데이터) -> Silver(정제/조인된 데이터) -> Gold(집계·시각화용 데이터) 순으로 변환되는 ETL Flow를 구성.
- 최종적으로 Superset에서 Gold Layer 데이터를 시각화 대시보드로 표현.
  
## 디렉토리 구조
```/src
├── common # 코딩 컨벤션 통일을 위한 Frame
│   └── base_tasks.py
├── devcourse_project2 # 파이프라인 EndPoint
│   ├── __init__.py 
├── pipelines # 순차적으로 실행되며 Task를 연결한 파이프라인
│   ├── analytics_station_utilization.py
│   ├── ...
│   └── station_stg_pipeline.py
├── tasks # 파이프라인 코드 가독성을 위하여 분리한 Tasks
│   └── convert_xlsx_to_parquet.py
└── utils # File Path, conn등 전역으로 사용되는 Utils
    ├── conn_utils.py
    └── file_utils.py
```
- 디렉토리는 ETL/ELT 파이프라인 구조에 맞춰 모듈화되어 있음
- **common**에서 Task 기반 프레임워크를 정의하고, **piplines**와 **task**에서 단계별 데이터 흐름을 관리합니다.

## <p align="center"> 📊프로젝트 결과</p>

### 서울시 소유 전기차 충전소 충전 기록 대시보드
- 시/구, 월별 충전 수, 충전 수 변화
- 구별 평균 충전량, 시간대별 충전 이용량
<img src="https://github.com/Lepus0T/report/blob/main/screencapture-localhost-8088-superset-dashboard-17-2025-10-31-13_56_04.png?raw=true">

### 서울시 소유 전기차 충전소 데이터 대시보드
- 전기차 충전소 위치
- 구별 충전소 수
- 구별 전기차 충전소 가동률
<img src="https://github.com/Lepus0T/report/blob/main/screencapture-localhost-8088-superset-dashboard-17-2025-10-31-13_56_15.png?raw=true">




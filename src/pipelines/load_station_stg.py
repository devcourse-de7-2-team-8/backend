import sys
import os
import logging
from dotenv import load_dotenv
from typing import List

# Python이 src/common, utils 모듈 찾도록 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.base_tasks import Task
from utils.conn_utils import get_snowflake_conn, get_s3_conn
from utils.file_utils import get_path

# .env 파일 로드
load_dotenv()

# S3 환경변수
s3_bucket = os.getenv("S3_BUCKET_NAME")
aws_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

# Snowflake 연결 (utils 사용)
conn = get_snowflake_conn()
cur = conn.cursor()

# station_stg 테이블 생성 (원본 보존, 컬럼 타입 명확히)
cur.execute("""
CREATE OR REPLACE TABLE RAW_DATA.station_stg (
    station_name VARCHAR,
    address VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    capacity_kw FLOAT,
    total_chargers INT,
    slow_chargers INT,
    fast_chargers INT,
    outlet_chargers INT
);
""")

# S3 → Snowflake COPY INTO
cur.execute(f"""
COPY INTO raw_data.station_stg
FROM 's3://{s3_bucket}/data/stations/seoul_station.parquet'
CREDENTIALS=(AWS_KEY_ID='{aws_key}' AWS_SECRET_KEY='{aws_secret}')
FILE_FORMAT=(TYPE = PARQUET);
""")

print("raw_data.station_stg 테이블 생성 및 S3 데이터 로드 완료!")
cur.close()
conn.close()
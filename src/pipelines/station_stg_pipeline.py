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


# --------------------------------------------------------------
# IDEMPOTENT TEST 
# 1-1. 테이블 1번만 생성, COPY INTO 할 때 중복 제거
# --------------------------------------------------------------
# station_stg 테이블 생성 (원본 보존, 컬럼 타입 명확히)
cur.execute(f"""
CREATE TABLE IF NOT EXISTS RAW_DATA.station_stg (
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
# ---------------------------------------------------------
# 1-2. TEMP 테이블 생성
cur.execute("""
CREATE OR REPLACE TEMP TABLE RAW_DATA.tmp_station_stg LIKE RAW_DATA.station_stg;
""")

# S3 데이터를 TEMP 테이블로 COPY
cur.execute(f"""
COPY INTO RAW_DATA.tmp_station_stg
FROM 's3://{s3_bucket}/data/stations/seoul_station.csv'
CREDENTIALS=(AWS_KEY_ID='{aws_key}' AWS_SECRET_KEY='{aws_secret}')
FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
""")


# tmp 테이블과 station_stg의 merge 테이블 생성 (원본 보존, 컬럼 타입 명확히)
cur.execute(f"""
MERGE INTO RAW_DATA.station_stg t
USING RAW_DATA.tmp_station_stg s
ON t.station_name = s.station_name AND t.address = s.address
WHEN MATCHED AND (
       t.latitude     <> s.latitude OR
       t.longitude    <> s.longitude OR
       t.capacity_kw  <> s.capacity_kw OR
       t.total_chargers <> s.total_chargers OR
       t.slow_chargers  <> s.slow_chargers OR
       t.fast_chargers  <> s.fast_chargers OR
       t.outlet_chargers <> s.outlet_chargers
) THEN
  UPDATE SET
    latitude = s.latitude,
    longitude = s.longitude,
    capacity_kw = s.capacity_kw,
    total_chargers = s.total_chargers,
    slow_chargers = s.slow_chargers,
    fast_chargers = s.fast_chargers,
    outlet_chargers = s.outlet_chargers
WHEN NOT MATCHED THEN
  INSERT (station_name, address, latitude, longitude, capacity_kw,
          total_chargers, slow_chargers, fast_chargers, outlet_chargers)
  VALUES (s.station_name, s.address, s.latitude, s.longitude, s.capacity_kw,
          s.total_chargers, s.slow_chargers, s.fast_chargers, s.outlet_chargers);
""")

# IDEMPOTENT TEST END
# ---------------------------------------------------------
# ---------------------------------------------------------




# ---test 실패 대비 보존코드------------------
# station_stg 테이블 생성 (원본 보존, 컬럼 타입 명확히)
# cur.execute("""
# CREATE OR REPLACE TABLE RAW_DATA.station_stg (
#     station_name VARCHAR,
#     address VARCHAR,
#     latitude FLOAT,
#     longitude FLOAT,
#     capacity_kw FLOAT,
#     total_chargers INT,
#     slow_chargers INT,
#     fast_chargers INT,
#     outlet_chargers INT
# );
# """)
#----------------------------------------
# ---test 실패 대비 보존코드------------------
# S3 → Snowflake COPY INTO
# cur.execute(f"""
# COPY INTO RAW_DATA.station_stg
# FROM 's3://{s3_bucket}/data/stations/seoul_station.csv'
# CREDENTIALS=(AWS_KEY_ID='{aws_key}' AWS_SECRET_KEY='{aws_secret}')
# FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
# """)
#-----------------------------------------


print("RAW.station_stg 테이블 생성 및 S3 데이터 로드 완료!")
cur.close()
conn.close()

# def run_station_stg_pipeline():
#    convert_station_task.run()
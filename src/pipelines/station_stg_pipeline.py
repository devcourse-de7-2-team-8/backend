import os
import sys

# Python이 src/common, utils 모듈 찾도록 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

from common.base_tasks import Task
from utils.conn_utils import get_snowflake_conn

load_dotenv()

# from
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")

# to
DB = os.getenv("SNOWFLAKE_DATABASE").upper()
RAW_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_RAW")
TEMP_TABLE_NAME = "TMP_STATION_STG"
TABLE_NAME = "STATION_STG"

conn = get_snowflake_conn()


def create_station_stg():
    with conn.cursor() as cur:
        
        cur.execute(f"""DROP TABLE IF EXISTS {DB}.{RAW_SCHEMA}.{TABLE_NAME}""")
        cur.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {DB}.{RAW_SCHEMA}.{TABLE_NAME} (
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
        """
        )

        cur.execute(f"""
        COPY INTO {DB}.{RAW_SCHEMA}.{TABLE_NAME}
        FROM 's3://{S3_BUCKET}/data/stations/'
        CREDENTIALS=(AWS_KEY_ID='{AWS_KEY}' AWS_SECRET_KEY='{AWS_SECRET}')
        FILE_FORMAT=(TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)


create_station_stg_task = Task("station_stg 생성", create_station_stg)


# 파이프라인 실행 함수 정의
def run_station_stg_pipeline():
    create_station_stg_task.run()

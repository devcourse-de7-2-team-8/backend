import os
from dotenv import load_dotenv
from common.base_tasks import Task
from utils.conn_utils import get_snowflake_conn

def create_session_stage_external_table():
    load_dotenv()
    db     = os.getenv("SNOWFLAKE_DATABASE").upper()
    schema = os.getenv("SNOWFLAKE_SCHEMA").upper()
    bucket = os.getenv("S3_BUCKET_NAME")
    aws_k  = os.getenv("AWS_ACCESS_KEY_ID")
    aws_s  = os.getenv("AWS_SECRET_ACCESS_KEY")

    sqls = [
        f"USE DATABASE {db}",
        f"USE SCHEMA {schema}",
        f"""
        CREATE OR REPLACE STAGE {db}.{schema}.EV_SESSIONS_STAGE
          URL = 's3://{bucket}/data/sessions/'
          CREDENTIALS = (AWS_KEY_ID='{aws_k}' AWS_SECRET_KEY='{aws_s}')
        """,
        f"""
        CREATE OR REPLACE EXTERNAL TABLE {db}.{schema}.SESSION_STG (
          station_name VARCHAR   AS (VALUE:c1::VARCHAR),
          start_time   TIMESTAMP AS (VALUE:c2::TIMESTAMP),
          end_time     TIMESTAMP AS (VALUE:c3::TIMESTAMP),
          charged_kwh  DOUBLE    AS (VALUE:c4::DOUBLE)

        )
        WITH LOCATION = @{db}.{schema}.EV_SESSIONS_STAGE
        FILE_FORMAT = (
          TYPE = CSV,
          FIELD_OPTIONALLY_ENCLOSED_BY = '\"',
          SKIP_HEADER = 1,
          FIELD_DELIMITER = ','
        )
        """
    ]

    conn = get_snowflake_conn()
    with conn.cursor() as cur:
        for q in sqls:
            cur.execute(q)
    print("EV_SESSIONS_STAGE + SESSION_STG 생성 완료")
    conn.close()

t = Task("create_session_stage_ext_table", create_session_stage_external_table)

def run_session_stage_pipeline():
    t.run()

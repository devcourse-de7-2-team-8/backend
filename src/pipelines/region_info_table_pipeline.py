import os
from dotenv import load_dotenv
from common.base_tasks import Task
from utils.conn_utils import get_snowflake_conn

def create_region_info_table():
    load_dotenv()
    db     = os.getenv("SNOWFLAKE_DATABASE").upper()
    schema = os.getenv("SNOWFLAKE_SCHEMA").upper()

    sqls = [
        f"USE DATABASE {db}",
        f"USE SCHEMA {schema}",
        f"DROP TABLE IF EXISTS {db}.{schema}.REGION_INFO",
        f"""
        CREATE OR REPLACE TABLE {db}.{schema}.REGION_INFO AS
        SELECT
            DISTINCT
            SPLIT_PART(address, ' ', 1) AS sido,   -- 시/도
            SPLIT_PART(address, ' ', 2) AS gu,     -- 구/군
        FROM {db}.{schema}.STATION_STG
        WHERE address IS NOT NULL
        ORDER BY sido, gu
        """
    ]

    conn = get_snowflake_conn()
    with conn.cursor() as cur:
        for q in sqls:
            cur.execute(q)
    print("REGION_INFO 테이블 생성 완료")
    conn.close()

t = Task("create_region_info_table", create_region_info_table)

def run_region_info_pipeline():
    t.run()

import os
from dotenv import load_dotenv
from common.base_tasks import Task
from utils.conn_utils import get_snowflake_conn

def create_region_info_table():
    load_dotenv()
    db     = os.getenv("SNOWFLAKE_DATABASE").upper()
    schema_raw = os.getenv("SNOWFLAKE_SCHEMA_RAW").upper()
    schema_pub   = os.getenv("SNOWFLAKE_SCHEMA_PUBLIC").upper()

    sqls = [
        f"USE DATABASE {db}",
        f"USE SCHEMA {schema_pub}",
        f"DROP TABLE IF EXISTS {db}.{schema_pub}.REGION_INFO",
        f"""
        CREATE OR REPLACE TABLE {db}.{schema_pub}.REGION_INFO AS
            SELECT
                CAST(ROW_NUMBER() OVER (ORDER BY SIDO, GU) AS INT) AS REGION_ID,
                SIDO,
                GU
            FROM (
                SELECT DISTINCT
                    CASE
                        WHEN SPLIT_PART(address, ' ', 1) = '서울시' THEN '서울'
                        ELSE SPLIT_PART(address, ' ', 1)
                    END AS SIDO,                        -- '서울시'를 '서울'로 통일
                    SPLIT_PART(address, ' ', 2) AS GU     
                FROM {db}.{schema_raw}.STATION_STG
                WHERE address IS NOT NULL
            ) D
            ORDER BY SIDO, GU
        """
    ]

    conn = get_snowflake_conn()
    with conn.cursor() as cur:
        for q in sqls:
            cur.execute(q)
    print("PUBLIC 스키마에 REGION_INFO 테이블 생성 완료")
    conn.close()

t = Task("create_region_info_table", create_region_info_table)

def run_region_info_pipeline():
    t.run()

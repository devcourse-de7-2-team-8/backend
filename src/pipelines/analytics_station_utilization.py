import os

from dotenv import load_dotenv

from common.base_tasks import Task
from utils.conn_utils import get_snowflake_conn

load_dotenv()
conn = get_snowflake_conn()
DB            = os.getenv("SNOWFLAKE_DATABASE").upper()
PUBLIC_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_PUBLIC")
ANALYTICS_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_ANALYTICS")


def create_analytics_station_count_table():
    load_dotenv()

    sqls = [
        f"USE DATABASE {DB}",
        f"CREATE SCHEMA IF NOT EXISTS {ANALYTICS_SCHEMA}",

        f"""
        CREATE OR REPLACE TABLE {DB}.{ANALYTICS_SCHEMA}.STATION_BY_GU AS
        SELECT 
            "GU_SHORT" AS "GU_SHORT",
            "STATION_COUNT" AS "STATION_COUNT"
        FROM (
            SELECT
                SPLIT_PART(address, ' ', 2) AS gu_short,
                COUNT(*) AS station_count
            FROM {DB}.{PUBLIC_SCHEMA}.EV_CHARGING_STATIONS
            WHERE address LIKE '서울%%'
            GROUP BY gu_short
            ORDER BY station_count DESC
        ) AS virtual_table
        LIMIT 1000
        """
    ]

    conn = get_snowflake_conn()
    with conn.cursor() as cur:
        for q in sqls:
            cur.execute(q)

    print(f"{ANALYTICS_SCHEMA}.STATION_COUNT_BY_GU 테이블 생성 완료!")
    conn.close()


# Task등록
t = Task("create_analytics_station_count_table", create_analytics_station_count_table)

def run_analytics_station_utilization_pipeline():
    analytics_station_utilization_task.run()
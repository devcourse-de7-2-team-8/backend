import logging
import os

from dotenv import load_dotenv

from common.base_tasks import Task
from utils.conn_utils import get_snowflake_conn

"""
저장된 PUBLIC 스키마의 테이블을 기준으로 시각화용 테이블을 생성하는 파이프라인

"""

load_dotenv()
conn = get_snowflake_conn()
PUBLIC_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_PUBLIC")
ANALYTICS_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_ANALYTICS")

def create_region_monthly_session_summary_table():
    table_name = "REGION_MONTHLY_SESSION_SUMMARY"
    
    with conn.cursor() as cur:
        cur.execute(f"""
                    DROP TABLE if EXISTS {ANALYTICS_SCHEMA}.{table_name};
                    """)
        
        cur.execute(f"""
        CREATE TABLE {ANALYTICS_SCHEMA}.{table_name}
        AS (
        SELECT
        r.region_id,
        MAX(r.sido)                                                     AS sido,
        MAX(r.gu)                                                       AS gu,
        LEFT(s.start_time::string, 7)                                   AS year_month,
        COUNT(1)                                                        AS current_session_count,
        LAG(current_session_count, 1)
           OVER (PARTITION BY r.region_id ORDER BY year_month)          AS prev_session_count,
           (CASE
               WHEN
                   prev_session_count IS NOT NULL AND
                   current_session_count IS NOT NULL
                   THEN current_session_count - prev_session_count
               ELSE 0
            END
           )                                                            AS diff_session_count,
           SUM(current_session_count) OVER (PARTITION BY r.region_id)   AS total_region_session_count,
           ROUND(current_session_count / total_region_session_count * 100, 2)
                                                                        AS region_session_percentage
        FROM {PUBLIC_SCHEMA}.ev_charging_sessions AS s
                 JOIN {PUBLIC_SCHEMA}.ev_charging_stations_temp AS st
                      ON s.station_name = st.station_name
                 JOIN {PUBLIC_SCHEMA}.region_info AS r
                      ON r.region_id = st.region_id
        GROUP BY r.region_id, year_month
        ORDER BY r.region_id, year_month
        )
        """)
    logging.log(logging.INFO, f"Table {ANALYTICS_SCHEMA}.{table_name} created successfully.")



region_monthly_session_summary_task = Task("create_region_monthly_session_summary_table",
                                           lambda : create_region_monthly_session_summary_table())


def  run_visualization_table_pipeline():  
    region_monthly_session_summary_task.run()
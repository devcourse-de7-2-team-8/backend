import logging
import os

from dotenv import load_dotenv

from common.base_tasks import Task
from utils.conn_utils import get_snowflake_conn

load_dotenv()
conn = get_snowflake_conn()
DB            = os.getenv("SNOWFLAKE_DATABASE").upper()
PUBLIC_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_PUBLIC")
ANALYTICS_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_ANALYTICS")

def create_analytics_station_utilization_table():
    load_dotenv()

    sqls = [
        f"USE DATABASE {DB}",
        f"CREATE SCHEMA IF NOT EXISTS {ANALYTICS_SCHEMA}",

        f"""
        CREATE OR REPLACE TABLE {DB}.{ANALYTICS_SCHEMA}.STATION_UTILIZATION_GU AS
        WITH
        -- 세션별 충전시간(시)과 충전량(kWh)
        session_hours AS (
          SELECT
              e.station_id,
              GREATEST(TIMESTAMPDIFF(SECOND, e.start_time, e.end_time) / 3600, 0) AS hours,
              e.charged_kwh
          FROM {DB}.{PUBLIC_SCHEMA}.EV_CHARGING_SESSIONS e
          WHERE e.end_time IS NOT NULL
            AND e.start_time IS NOT NULL
            AND e.end_time > e.start_time
            AND e.charged_kwh IS NOT NULL
        ),

        -- 충전소별 총 충전시간 / 총 충전량 / 평균전력(kW)
        station_agg AS (
          SELECT
              sh.station_id,
              SUM(sh.charged_kwh) AS total_kwh,
              SUM(sh.hours)       AS total_hours,
              CASE WHEN SUM(sh.hours) > 0
                   THEN SUM(sh.charged_kwh) / SUM(sh.hours)
                   ELSE 0 END AS avg_power_kw
          FROM session_hours sh
          GROUP BY sh.station_id
        )

        -- REGION_INFO와 조인해 구(gu) 포함 + STATIONS의 capacity_kw 사용
        SELECT
            s.region_id,
            r.gu,
            s.station_id,
            s.station_name,
            s.capacity_kw,
            ROUND(a.avg_power_kw, 3) AS avg_power_kw,
            ROUND((a.avg_power_kw / s.capacity_kw) * 100, 2) AS utilization_pct,
            a.total_kwh,
            a.total_hours
        FROM {DB}.{PUBLIC_SCHEMA}.EV_CHARGING_STATIONS s
        LEFT JOIN station_agg a
               ON a.station_id = s.station_id
        LEFT JOIN {DB}.{PUBLIC_SCHEMA}.REGION_INFO r
               ON r.region_id = s.region_id
        WHERE r.gu IS NOT NULL
          AND s.capacity_kw IS NOT NULL
          AND s.capacity_kw > 0
          AND a.avg_power_kw IS NOT NULL
          AND a.avg_power_kw > 0
        """
    ]

    with conn.cursor() as cur:
        for q in sqls:
            cur.execute(q)

    print(f"{ANALYTICS_SCHEMA}.STATION_UTILIZATION_GU 테이블 생성 완료")


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
                 JOIN {PUBLIC_SCHEMA}.EV_CHARGING_STATIONS AS st
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


analytics_station_utilization_task = Task("create_analytics_station_utilization_table", create_analytics_station_utilization_table)

analytics_station_utilization_task >> region_monthly_session_summary_task


def run_analytics_station_utilization_pipeline():
    analytics_station_utilization_task.run()
    
    conn.close()


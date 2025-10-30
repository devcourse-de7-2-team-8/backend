import os
from dotenv import load_dotenv
from common.base_tasks import Task
from utils.conn_utils import get_snowflake_conn


def create_analytics_station_utilization_table():
    load_dotenv()

    db            = os.getenv("SNOWFLAKE_DATABASE").upper()
    schema_public = os.getenv("SNOWFLAKE_SCHEMA_PUBLIC").upper()  # 원본 스키마 (예: PUBLIC)
    schema_target = "ANALYTICS"                                   # 결과 스키마(고정 생성)

    sqls = [
        f"USE DATABASE {db}",
        f"CREATE SCHEMA IF NOT EXISTS {schema_target}",

        f"""
        CREATE OR REPLACE TABLE {db}.{schema_target}.STATION_UTILIZATION_GU AS
        WITH
        -- 세션별 충전시간(시)과 충전량(kWh)
        session_hours AS (
          SELECT
              e.station_id,
              GREATEST(TIMESTAMPDIFF(SECOND, e.start_time, e.end_time) / 3600, 0) AS hours,
              e.charged_kwh
          FROM {db}.{schema_public}.EV_CHARGING_SESSIONS e
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
        FROM {db}.{schema_public}.EV_CHARGING_STATIONS s
        LEFT JOIN station_agg a
               ON a.station_id = s.station_id
        LEFT JOIN {db}.{schema_public}.REGION_INFO r
               ON r.region_id = s.region_id
        WHERE r.gu IS NOT NULL
          AND s.capacity_kw IS NOT NULL
          AND s.capacity_kw > 0
          AND a.avg_power_kw IS NOT NULL
          AND a.avg_power_kw > 0
        """
    ]

    conn = get_snowflake_conn()
    with conn.cursor() as cur:
        for q in sqls:
            cur.execute(q)

    print(f"{schema_target}.STATION_UTILIZATION_GU 테이블 생성 완료")
    conn.close()


# 파이프라인 Task 등록
t = Task("create_analytics_station_utilization_table", create_analytics_station_utilization_table)


def run_analytics_station_utilization_pipeline():
    t.run()


import logging
import os

from dotenv import load_dotenv

from common.base_tasks import Task
from utils.conn_utils import get_snowflake_conn

"""

Station_id(충전소 고유번호)를 가지고 있는 FACT TABLE (ev_charging_sessions)를
생성하는 파이프라인
 
Related Tables: RAW_DATA.session_stg, PUBLIC.ev_charging_station
@author: Seonggil Jeong
"""

load_dotenv()
conn = get_snowflake_conn()
PUBLIC_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_PUBLIC")
RAW_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_RAW")



def is_exists_ev_charging_session_table():
    with conn.cursor() as cur:
        cur.execute(f"""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = '{PUBLIC_SCHEMA}'
	                  AND table_name = 'EV_CHARGING_SESSIONS'
                    """)
        result = cur.fetchone()[0]
        return result is not None and result > 0


def create_if_not_exists_session_table(exists_table_flag: bool = False):
    if exists_table_flag:
        logging.log(logging.INFO, "Table EV_CHARGING_SESSIONS already exists.")
        return

    with conn.cursor() as cur:
        cur.execute(f"""
                CREATE TABLE PUBLIC.EV_CHARGING_SESSIONS
                            AS (SELECT st.station_id,
                                       st.station_name,
                                       s.start_time,
                                       s.end_time,
                                       s.charged_kwh
                                FROM {PUBLIC_SCHEMA}.ev_charging_stations AS st
	                                     LEFT JOIN {RAW_SCHEMA}.session_stg AS s
	                                               ON s.station_name = st.station_name
                            )
	                                              
        """)


def validation_ev_charging_session_table():
    with conn.cursor() as cur:
        expected_zero = cur.execute(f"""
		                            SELECT COUNT(1)
		                            FROM {PUBLIC_SCHEMA}.ev_charging_sessions AS s
		                            WHERE s.station_id IS NULL""").fetchone()
        if expected_zero is not None and expected_zero[0] == 0:
            logging.log(
                logging.INFO,
                "Validation passed: Table ev_charging_sessions has expected number of records.",
            )
        else:
            logging.log(
                logging.WARNING,
                "Validation failed: Table ev_charging_sessions does not have expected number of records.",
            )


start_task = Task(
    "start_fact_ev_charging_session_table_pipeline",
    lambda: print("Starting FACT_EV_CHARGING_SESSION table pipeline."),
)
check_table_exists_task = Task(
    "check_ev_charging_session_table_exists_task",
    lambda: is_exists_ev_charging_session_table(),
)

create_table_task = Task(
    "create_ev_charging_session_table_task",
    lambda: create_if_not_exists_session_table(check_table_exists_task.result),
)

validate_table_task = Task(
    "validate_ev_charging_session_table_task",
    lambda: validation_ev_charging_session_table(),
)

start_task >> check_table_exists_task >> create_table_task >> validate_table_task


def run_create_ev_charging_sessions_table_pipeline():
    start_task.run()

    conn.close()

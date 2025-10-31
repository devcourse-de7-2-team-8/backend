from pipelines.analytics_station_utilization import (
    run_analytics_station_utilization_pipeline,
)
from pipelines.fact_session_table_pipeline import (
    run_create_ev_charging_sessions_table_pipeline,
)
from pipelines.region_info_table_pipeline import run_region_info_pipeline
from pipelines.session_stage_ext_table_pipeline import run_session_stage_pipeline
from pipelines.snowflake_pipeline import run_snowflake_pipeline
from pipelines.station_stg_pipeline import run_station_stg_pipeline


def main():
    # Upload files to S3
    # run_s3_upload_pipeline()

    # Create TABLE STATION_STG
    run_station_stg_pipeline()

    # Create EXTERNAL TABLE SESSION_STG
    run_session_stage_pipeline()

    # Create TABLE REGION_INFO (dependencies: STATION_STG)
    run_region_info_pipeline()
    run_snowflake_pipeline()

    # Create FACT TABLE EV_CHARGING_SESSIONS (dependencies: STATION_STG, SESSION_STG)
    run_create_ev_charging_sessions_table_pipeline()

    # Create ANALYTICS TABLES (dependencies: EV_CHARGING_STATIONS, EV_CHARGING_SESSIONS, REGION_INFO)
    run_analytics_station_utilization_pipeline()

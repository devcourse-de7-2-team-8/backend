from pipelines.fact_session_table_pipeline import (
    run_create_ev_charging_sessions_table_pipeline,
)
from pipelines.s3_upload_pipeline import run_s3_upload_pipeline


def main():
    run_s3_upload_pipeline()
    run_station_stg_pipeline()
    run_session_stage_pipeline()
    run_region_info_pipeline()
    run_snowflake_pipeline()
    run_create_ev_charging_sessions_table_pipeline()
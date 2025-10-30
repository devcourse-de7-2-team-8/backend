from pipelines.fact_session_table_pipeline import (
    run_create_ev_charging_sessions_table_pipeline,
)
from pipelines.region_info_table_pipeline import run_region_info_pipeline
from pipelines.s3_upload_pipeline import run_s3_upload_pipeline
from pipelines.session_stage_ext_table_pipeline import run_session_stage_pipeline
from pipelines.snowflake_pipeline import run_snowflake_pipeline
from pipelines.station_stg_pipeline import run_station_stg_pipeline
from pipelines.analytics_station_utilization import run_analytics_station_utilization_pipeline


def main():
    # run_s3_upload_pipeline()
    # run_station_stg_pipeline()
    # run_session_stage_pipeline()
    # run_region_info_pipeline()
    # run_snowflake_pipeline()
    # run_create_ev_charging_sessions_table_pipeline()
    run_analytics_station_utilization_pipeline()
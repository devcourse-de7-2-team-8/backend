from pipelines.s3_upload_pipeline import run_s3_upload_pipeline
from pipelines.session_stage_ext_table_pipeline import run_session_stage_pipeline
from pipelines.station_stg_pipeline import run_station_stg_pipeline


def main():
    run_s3_upload_pipeline()
    run_station_stg_pipeline()
    run_session_stage_pipeline()
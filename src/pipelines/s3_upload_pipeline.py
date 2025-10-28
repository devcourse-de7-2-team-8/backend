import logging
import os
from typing import (
    List,
)

from common.base_tasks import (
    Task,
)
from utils.conn_utils import (
    get_s3_conn,
)
from utils.file_utils import (
    get_path,
)


def convert_session_data_to_csv(years: List[str]):
    output_paths = []
    rename_columns = {
        "충전소명": "station_name",
        "충전시작시간": "start_time",
        "충전종료시간": "end_time",
        "충전량(kW)": "charged_kwh",
    }
    file_paths = [
        get_path(f"data/sessions/year={year}/ev_charging_session.xlsx")
        for year in years
    ]

    for file in file_paths:
        from utils.file_utils import (
            xlsx_to_csv,
        )

        out_path = file.replace(".xlsx", ".csv")

        xlsx_to_csv(file, out_path, skip_rows=3, rename_columns=rename_columns)
        output_paths.append(out_path)

    return output_paths


def convert_station_data_to_csv(station_files: List[str]):
    output_paths = []
    rename_columns = {
        "충전소명": "station_name",
        "주소": "address",
        "위도": "latitude",
        "경도": "longitude",
        "충전소 용량(kW)": "capacity_kw",
        "전체": "total_chargers",
        "완속": "slow_chargers",
        "급속": "fast_chargers",
        "콘센트": "outlet_chargers",
    }
    file_paths = [
        get_path(f"data/stations/{stations}.xlsx") for stations in station_files
    ]

    for file in file_paths:
        from utils.file_utils import (
            xlsx_to_csv,
        )

        out_path = file.replace(".xlsx", ".csv")
        xlsx_to_csv(file, out_path, skip_rows=3, rename_columns=rename_columns)
        output_paths.append(out_path)
    return output_paths


def upload_to_s3(local_paths: List[str]):
    s3 = get_s3_conn()

    for path in local_paths:
        key = path.split("data/")[-1]
        s3_key = f"data/{key}"

        try:
            s3.upload_file(path, os.environ.get("S3_BUCKET_NAME"), s3_key)
        except Exception as e:
            logging.log(logging.ERROR, f"Failed to upload {path} to S3: {e}")


convert_session_task = Task(
    "convert_session_task", lambda: convert_session_data_to_csv(["2024", "2025"])
)
convert_station_task = Task(
    "convert_station_task", lambda: convert_station_data_to_csv(["seoul_station"])
)

upload_to_s3_task = Task(
    "upload_to_s3_task",
    lambda: upload_to_s3(convert_session_task.result + convert_station_task.result),
)

convert_station_task >> convert_session_task >> upload_to_s3_task


def run_s3_upload_pipeline():
    convert_station_task.run()

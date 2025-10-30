import logging
import os
from typing import (
    List,
)

from common.base_tasks import (
    Task,
)
from tasks.convert_xlsx_to_parquet import (
    run_convert_pipeline,
)
from utils.conn_utils import (
    get_s3_conn,
)


def upload_to_s3(local_paths: List[str]):
    s3 = get_s3_conn()

    for path in local_paths:
        key = path.split("data/")[-1]
        s3_key = f"data/{key}"

        try:
            s3.upload_file(path, os.environ.get("S3_BUCKET_NAME"), s3_key)
        except Exception as e:
            logging.log(logging.ERROR, f"Failed to upload {path} to S3: {e}")


convert_to_parquet_task = Task(
    "convert_to_parquet_task",
    run_convert_pipeline,
)

upload_to_s3_task = Task(
    "upload_to_s3_task",
    lambda: upload_to_s3(convert_to_parquet_task.result),
)

convert_to_parquet_task >> upload_to_s3_task


def run_s3_upload_pipeline():
    convert_to_parquet_task.run()

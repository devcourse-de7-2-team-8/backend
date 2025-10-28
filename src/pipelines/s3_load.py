import os
import boto3
from src.common.base_tasks import Task
from src.utils.file_utils import get_path
from dotenv import load_dotenv
from pathlib import Path

# 1. 환경 변수 가져오기
load_dotenv()
AWS_REGION = os.getenv("AWS_REGION")
BUCKET = os.getenv("S3_BUCKET")


# 2. 함수 생성
def upload_csv_files():
    project_root = Path(get_path("."))
    data_dir = project_root / "data"

    s3 = boto3.client("s3", region_name=AWS_REGION)
    csv_files = sorted(data_dir.rglob("*.csv"))

    for file_path in csv_files:
        rel_path = file_path.relative_to(data_dir).as_posix()
        s3_key = f"data/{rel_path}"

        s3.upload_file(
            str(file_path),
            BUCKET,
            s3_key,
            ExtraArgs={"ContentType": "text/csv"},
        )
        print(f"[OK] {file_path} → s3://{BUCKET}/{s3_key}")


# 3. Task 정의
upload_task = Task("upload_csv_files", upload_csv_files)


def run_upload_pipeline():
    upload_task.run()
    print("업로드 완료")


if __name__ == "__main__":
    run_upload_pipeline()
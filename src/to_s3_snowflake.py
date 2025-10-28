import boto3
import snowflake.connector
import os
from dotenv import load_dotenv

# .env 파일 불러오기 (AWS S3, SNOWFLAKE 정보)
load_dotenv()

import boto3
import os

# -------------------------------------------------
# 함수 정의
# -------------------------------------------------
def upload_to_s3(local_path, bucket_name, s3_key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="ap-northeast-2"
    )
    s3.upload_file(local_path, bucket_name, s3_key)
    print(f"Uploaded {local_path} → s3://{bucket_name}/{s3_key}")

# -------------------------------------------------
# 실행부 (여기에 입력)
# ("로컬경로", "s3경로")
# -------------------------------------------------
if __name__ == "__main__":
bucket = "soojin-ev"

files_to_upload = [
    ("../data/stations/seoul_station.csv", "data/stations/seoul_station.csv"),
    ("../data/sessions/year=2024/ev_charging_session_2024.csv", "data/sessions/year=2024/ev_charging_session.csv"),
    ("../data/sessions/year=2025/ev_charging_session_2025.csv", "data/sessions/year=2025/ev_charging_session.csv"),
]

for local_file, s3_key in files_to_upload:
    upload_to_s3(local_file, bucket, s3_key)
    
print("모든 CSV 파일 S3 업로드 완료")

# -------------------------------------------------
# Snowflake 연결
# -------------------------------------------------

# -------------------------------------------------
# Snowflake COPY INTO
# -------------------------------------------------
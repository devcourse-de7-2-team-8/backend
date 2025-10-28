import os
from pathlib import Path
import boto3
from dotenv import load_dotenv

# 1. .env 불러오기
load_dotenv()

# 2. 환경 변수 가져오기
AWS_REGION = os.getenv("AWS_REGION")
BUCKET = os.getenv("S3_BUCKET")

# 3. 디렉토리 설정
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

def upload_csv_files():
    """data 폴더의 모든 CSV 파일을 S3 버킷 루트에 업로드"""

    # boto3 클라이언트 생성
    s3 = boto3.client("s3", region_name=AWS_REGION)

    # 업로드 대상 파일 탐색
    csv_files = sorted(DATA_DIR.rglob("*.csv"))


    for file_path in csv_files:
        # data/ 기준 상대경로 (예: sessions/2024/ev_charging_session.csv)
        rel = file_path.relative_to(DATA_DIR).as_posix()
        # S3 key: data/ + 상대경로 (예: data/sessions/2024/ev_charging_session.csv)
        key = f"data/{rel}"

        s3.upload_file(
            str(file_path),
            BUCKET,
            key,
            ExtraArgs={"ContentType": "text/csv"}
        )
        
        print(f"[OK] {file_path} → s3://{BUCKET}/{key}")

if __name__ == "__main__":
    upload_csv_files()
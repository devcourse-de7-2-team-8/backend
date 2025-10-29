import os
import snowflake.connector
from dotenv import load_dotenv
from common.base_tasks import Task


def create_stage(cur, bucket, aws_key, aws_secret):
    """S3 Stage 생성"""
    cur.execute(f"""
        CREATE OR REPLACE STAGE s3_stage
        URL='s3://{bucket}/'
        CREDENTIALS=(
            AWS_KEY_ID='{aws_key}'
            AWS_SECRET_KEY='{aws_secret}'
        )
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    """)
    print(f"Stage [s3_stage] 생성 완료 (Bucket: {bucket})")


def copy_into_EV_CHARGING_STATIONS(cur):
    """S3 → EV_CHARGING_STATIONS 단일 테이블 생성 및 적재"""
    cur.execute("""
        CREATE OR REPLACE TABLE PUBLIC.EV_CHARGING_STATIONS (
            STATION_ID INTEGER AUTOINCREMENT START 1,
            STATION_NAME VARCHAR,
            ADDRESS VARCHAR,
            REGION_ID INT,
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            CAPACITY_KW FLOAT,
            TOTAL_CHARGERS INTEGER,
            SLOW_CHARGERS INTEGER,
            FAST_CHARGERS INTEGER,
            OUTLET_CHARGERS INTEGER,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
    """)

    cur.execute("""
        COPY INTO PUBLIC.EV_CHARGING_STATIONS
        (STATION_NAME, ADDRESS, LATITUDE, LONGITUDE, CAPACITY_KW,
         TOTAL_CHARGERS, SLOW_CHARGERS, FAST_CHARGERS, OUTLET_CHARGERS)
        FROM @s3_stage/data/stations/seoul_station.csv
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        ON_ERROR = 'CONTINUE';
    """)
    print(" EV_CHARGING_STATIONS 단일 테이블 적재 완료!")

def update_region_mapping(cur):
    """REGION_INFO 기준으로 REGION_ID 자동 매핑"""
    cur.execute("""
        UPDATE PUBLIC.EV_CHARGING_STATIONS AS s
        SET REGION_ID = r.REGION_ID
        FROM PUBLIC.REGION_INFO AS r
        WHERE s.ADDRESS LIKE CONCAT('%', r.SIDO, '%')
            AND s.ADDRESS LIKE CONCAT('%', r.GU, '%');
    """)
    print("REGION_INFO 기반 REGION_ID 매핑 완료")


def run_snowflake_pipeline():
    """전체 파이프라인 실행"""
    load_dotenv()
    conn = None

    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        cur = conn.cursor()
        print("Snowflake 연결 성공")

        #  Task 정의
        stage_task = Task(
            "Stage 생성",
            lambda: create_stage(
                cur,
                os.getenv("S3_BUCKET_NAME"),
                os.getenv("AWS_ACCESS_KEY_ID"),
                os.getenv("AWS_SECRET_ACCESS_KEY")
            )
        )

        load_task = Task(
            "EV_CHARGING_STATIONS 적재",
            lambda: copy_into_EV_CHARGING_STATIONS(cur)
        )

        join_task = Task(
            "Region FK 매핑",
            lambda: update_region_mapping(cur)
        )

        #  DAG 순서 설정 및 실행
        stage_task >> load_task >> join_task
        stage_task.run()

        print("전체 Snowflake 파이프라인 완료 (통합 테이블 업로드 성공)")

    except Exception as e:
        print(f"오류 발생: {e}")

    finally:
        if conn:
            conn.close()
            print("연결 종료 완료")


if __name__ == "__main__":
    run_snowflake_pipeline()

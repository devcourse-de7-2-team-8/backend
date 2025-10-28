import pandas as pd
import os

def main():
    #파일 경로
    file_2024 = "./data/sessions/year=2024/ev_charging_session.xlsx"
    file_2025 = "./data/sessions/year=2025/ev_charging_session.xlsx"
    file_stations = "./data/stations/seoul_station.xlsx"

    #세션 데이터 로드
    df_2024 = pd.read_excel(file_2024, header=3)
    df_2025 = pd.read_excel(file_2025, header=3)
    df_sessions = pd.concat([df_2024, df_2025], ignore_index=True)

    #NaN 제거 및 컬럼명 정리
    df_sessions = df_sessions.dropna(how="all")
    df_sessions = df_sessions.dropna(subset=["충전소명", "충전시작시간"])
    df_sessions.rename(columns={
        "충전소명": "station_name",
        "충전시작시간": "usage_start",
        "충전종료시간": "usage_end",
        "충전량(kW)": "power_kwh"
    }, inplace=True)

    #날짜 형식 변환
    df_sessions["usage_start"] = pd.to_datetime(df_sessions["usage_start"], errors="coerce")
    df_sessions["usage_end"] = pd.to_datetime(df_sessions["usage_end"], errors="coerce")
    df_sessions = df_sessions.sort_values(by="usage_start")

    #충전소 정보 로드 및 컬럼 통일
    df_stations = pd.read_excel(file_stations, header=2)
    df_stations.rename(columns={
        "충전소명": "station_name",
        "주소": "address",
        "위도": "latitude",
        "경도": "longitude",
        "충전소 용량(kW)": "capacity_kw",
        "전체": "charger_total",
        "완속": "charger_slow",
        "급속": "charger_fast",
        "콘센트": "charger_socket"
    }, inplace=True)

    #CSV 저장 (S3 적재 전용 폴더)
    output_dir = "../csv_ready"
    os.makedirs(output_dir, exist_ok=True)

    df_sessions.to_csv(os.path.join(output_dir, "session_stg.csv"), index=False, encoding="utf-8-sig")
    df_stations.to_csv(os.path.join(output_dir, "station_stg.csv"), index=False, encoding="utf-8-sig")

    print("CSV 변환 완료")

if __name__ == "__main__":
    main()

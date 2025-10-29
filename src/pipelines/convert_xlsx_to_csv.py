from src.common.base_tasks import Task
from src.utils.file_utils import get_path, xlsx_to_csv

def convert_sessions_2024():
    xlsx_path = get_path("data/sessions/year=2024/ev_charging_session.xlsx")
    csv_path  = get_path("data/sessions/year=2024/ev_charging_session.csv")

    rename = {
        "충전소명": "station_name",
        "충전시작시간": "start_time",
        "충전종료시간": "end_time",
        "충전량(kW)": "charged_kwh",
    }
    xlsx_to_csv(xlsx_path, csv_path, skip_rows=3, rename_columns=rename)


def convert_sessions_2025():
    xlsx_path = get_path("data/sessions/year=2025/ev_charging_session.xlsx")
    csv_path  = get_path("data/sessions/year=2025/ev_charging_session.csv")

    rename = {
        "충전소명": "station_name",
        "충전시작시간": "start_time",
        "충전종료시간": "end_time",
        "충전량(kW)": "charged_kwh",
    }
    xlsx_to_csv(xlsx_path, csv_path, skip_rows=3, rename_columns=rename)


def convert_seoul_stations():
    xlsx_path = get_path("data/stations/seoul_station.xlsx")
    csv_path  = get_path("data/stations/seoul_station.csv")

    rename = {
        "충전소명": "station_name",
        "주소": "address",
        "위도": "latitude",
        "경도": "longitude",
        "충전소 용량(kW)": "capacity_kw",
        "전체": "num_total",
        "완속": "num_slow",
        "급속": "num_fast",
        "콘센트": "num_outlet",
    }
    xlsx_to_csv(xlsx_path, csv_path, skip_rows=3, rename_columns=rename)



# Task 정의
t_2024 = Task("convert_sessions_2024", convert_sessions_2024)
t_2025 = Task("convert_sessions_2025", convert_sessions_2025)
t_st   = Task("convert_seoul_stations", convert_seoul_stations)

t_2024 >> t_2025 >> t_st


def run_convert_pipeline():
    t_2024.run()
    print("변환 완료")


if __name__ == "__main__":
    run_convert_pipeline()

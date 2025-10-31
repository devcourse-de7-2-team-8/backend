import asyncio

from utils.file_utils import get_path, xlsx_to_parquet


async def convert_sessions_2024():
    print("Starting convert_sessions_2024")
    xlsx_path = get_path("data/sessions/year=2024/ev_charging_session.xlsx")
    parquet_path = get_path("data/sessions/year=2024/ev_charging_session.parquet")

    rename = {
        "충전소명": "station_name",
        "충전시작시간": "start_time",
        "충전종료시간": "end_time",
        "충전량(kW)": "charged_kwh",
    }
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, xlsx_to_parquet, xlsx_path, parquet_path, 3, rename)
    return parquet_path


async def convert_sessions_2025():
    print("Starting convert_sessions_2025")
    xlsx_path = get_path("data/sessions/year=2025/ev_charging_session.xlsx")
    parquet_path = get_path("data/sessions/year=2025/ev_charging_session.parquet")

    rename = {
        "충전소명": "station_name",
        "충전시작시간": "start_time",
        "충전종료시간": "end_time",
        "충전량(kW)": "charged_kwh",
    }
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, xlsx_to_parquet, xlsx_path, parquet_path, 3, rename)
    return parquet_path


async def convert_seoul_stations():
    print("Converting Seoul stations XLSX to Parquet...")

    xlsx_path = get_path("data/stations/seoul_station.xlsx")
    parquet_path = get_path("data/stations/seoul_station.parquet")

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
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, xlsx_to_parquet, xlsx_path, parquet_path, 3, rename)
    return parquet_path


async def _run_convert_pipeline():
    results = await asyncio.gather(
        convert_sessions_2024(),
        convert_sessions_2025(),
        convert_seoul_stations(),
    )
    print("변환 완료", results)
    return results


def run_convert_pipeline():
    return asyncio.run(_run_convert_pipeline())
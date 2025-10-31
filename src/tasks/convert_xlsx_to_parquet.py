import asyncio

from utils.file_utils import get_path, xlsx_to_parquet


async def _convert_sessions_2024():
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


async def _convert_sessions_2025():
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


async def _convert_seoul_stations():
    print("Converting Seoul stations XLSX to Parquet...")

    xlsx_path = get_path("data/stations/seoul_station.xlsx")
    parquet_path = get_path("data/stations/seoul_station.parquet")

    rename = {
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
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, xlsx_to_parquet, xlsx_path, parquet_path, 3, rename)
    return parquet_path


async def async_convert_session_stations():
    results = await asyncio.gather(
        _convert_sessions_2024(),
        _convert_sessions_2025(),
        _convert_seoul_stations(),
    )
    print("csv to parquet 변환 완료")
    return results
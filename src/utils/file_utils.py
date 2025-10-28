import pandas as pd


def get_path(file_path: str) -> str:
    from dotenv import load_dotenv
    from pathlib import Path
    load_dotenv()

    project_root = Path(__file__).resolve().parents[2]

    return str((f"{project_root}/{file_path}"))


def xlsx_to_csv(xlsx_path: str, output_path: str, *, skip_rows: int = 0,
    rename_columns: dict = None):
    df = pd.read_excel(xlsx_path, skiprows=skip_rows, header=0)
    df.index = range(1, len(df) + 1)

    if rename_columns:
        df = df.rename(columns=rename_columns)

    df.to_csv(output_path, index=False, encoding='utf-8-sig')

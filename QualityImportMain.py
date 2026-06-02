import json
from pathlib import Path

import pandas as pd

from DatabaseConfig import DB_CONFIG
from data_io.DatabaseClient import DatabaseClient
from data_io.QualityInspectionReader import QualityInspectionReader


def _find_quality_files(folder: str) -> list[str]:
    base = Path(folder)
    out = []
    for path in base.iterdir():
        if path.name.startswith("~$"):
            continue
        if path.suffix.lower() in (".xlsx", ".xls") and path.name.startswith("品质检验"):
            out.append(str(path))
    return sorted(out)


def _rows_for_insert(df: pd.DataFrame, source_file: str) -> list[dict]:
    rows = []
    for idx, row in df.iterrows():
        payload = {}
        for col in df.columns:
            value = row[col]
            payload[str(col)] = None if pd.isna(value) else str(value)
        rows.append(
            {
                "source_file": source_file,
                "row_no": int(idx) + 1,
                "row_json": json.dumps(payload, ensure_ascii=False),
            }
        )
    return rows


def _insert_rows(db: DatabaseClient, rows: list[dict]):
    if not rows:
        return
    conn = db._get_connection()
    sql = (
        "REPLACE INTO quality_inspection_raw (source_file, row_no, row_json) "
        "VALUES (%s, %s, %s)"
    )
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(sql, (row["source_file"], row["row_no"], row["row_json"]))
        conn.commit()


def _insert_quality_params(db: DatabaseClient, quality_records):
    rows = []
    for record in quality_records:
        for row in record.to_quality_table_rows():
            row["source_file"] = Path(record.source_file).name
            rows.append(row)
    if not rows:
        return

    conn = db._get_connection()
    sql = (
        "INSERT INTO quality_inspection_param "
        "(batch_no, product_no, item_name, param_type, lower_bound, upper_bound, "
        "qualitative_value, actual_value, is_ok, table_is_ok, source_file) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                sql,
                (
                    row["batch_no"],
                    row["product_no"],
                    row["item_name"],
                    row["param_type"],
                    row["lower_bound"],
                    row["upper_bound"],
                    row["qualitative_value"],
                    row["actual_value"],
                    int(row["is_ok"]),
                    None if row["table_is_ok"] is None else int(row["table_is_ok"]),
                    row["source_file"],
                ),
            )
        conn.commit()


def main():
    db = DatabaseClient(DB_CONFIG, batch_size=1000)
    reader = QualityInspectionReader()
    total = 0
    parsed_total = 0

    for file_path in _find_quality_files("."):
        print(f"Processing quality file: {file_path}")
        df = pd.read_excel(file_path, engine="openpyxl")
        source_file = Path(file_path).name

        raw_rows = _rows_for_insert(df, source_file)
        _insert_rows(db, raw_rows)

        quality_records = reader.read_dataframe(df, source_file)
        _insert_quality_params(db, quality_records)

        total += len(raw_rows)
        parsed_total += sum(len(record.params) for record in quality_records)
        print(f"  raw rows={len(raw_rows)}, parsed params={parsed_total}")

    db.close()
    print(f"Quality import done: raw rows={total}, parsed params={parsed_total}")


if __name__ == "__main__":
    main()

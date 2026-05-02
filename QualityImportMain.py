import json
from pathlib import Path

import pandas as pd

from DatabaseConfig import DB_CONFIG
from data_io.DatabaseClient import DatabaseClient


def _find_quality_files(folder: str) -> list[str]:
    base = Path(folder)
    out = []
    for p in base.iterdir():
        if p.name.startswith("~$"):
            continue
        if p.suffix.lower() in (".xlsx", ".xls") and p.name.startswith("品质检验"):
            out.append(str(p))
    return sorted(out)


def _rows_for_insert(df: pd.DataFrame, source_file: str) -> list[dict]:
    rows = []
    for idx, row in df.iterrows():
        payload = {}
        for col in df.columns:
            v = row[col]
            if pd.isna(v):
                payload[str(col)] = None
            else:
                payload[str(col)] = str(v)
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


def main():
    db = DatabaseClient(DB_CONFIG, batch_size=1000)
    total = 0
    for file_path in _find_quality_files("."):
        print(f"处理品质检验文件: {file_path}")
        df = pd.read_excel(file_path, engine="openpyxl")
        rows = _rows_for_insert(df, Path(file_path).name)
        _insert_rows(db, rows)
        total += len(rows)
        print(f"  已写入 {len(rows)} 行")
    db.close()
    print(f"品质检验导入完成，总行数: {total}")


if __name__ == "__main__":
    main()

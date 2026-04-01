# 输入: Excel 路径、DataCleaner
# 输出: ProcessRecord 列表（每行批号+物料聚合为一条）
# 关键规则: 经 clean_numeric 后不得残留 np.nan；列定位与实验一一致（别名+兜底列序）

import pandas as pd

from DatabaseConfig import COLUMN_ALIASES, COLUMN_FALLBACK_INDEX
from business_logic.record import ProcessRecord
from business_logic.logic import DataCleaner


class ExcelReader:
    """用 pandas 读取工序明细 Excel，聚合为 ProcessRecord 列表。"""

    def __init__(self, cleaner: DataCleaner):
        self.cleaner = cleaner

    def _pick_columns(self, df: pd.DataFrame) -> dict:
        columns = list(df.columns)
        index_map = {}
        for logical_name, aliases in COLUMN_ALIASES.items():
            found = None
            for i, c in enumerate(columns):
                c_text = str(c)
                if any(alias in c_text for alias in aliases):
                    found = i
                    break
            if found is None:
                found = COLUMN_FALLBACK_INDEX[logical_name]
            index_map[logical_name] = found
        return index_map

    def read(self, file_path: str) -> list[ProcessRecord]:
        df = pd.read_excel(file_path, engine="openpyxl")
        return self.read_dataframe(df, file_path)

    def read_dataframe(self, df: pd.DataFrame, source_file: str = "") -> list[ProcessRecord]:
        """与 read 相同逻辑，便于测试用构造好的 DataFrame 做「不遗漏、不错误」断言。"""
        idx = self._pick_columns(df)
        grouped: dict[tuple[str, str], ProcessRecord] = {}

        for _, row in df.iterrows():
            batch_no = str(row.iloc[idx["batch_no"]]).strip()
            product_no = str(row.iloc[idx["product_no"]]).strip()
            item_name = str(row.iloc[idx["item_name"]]).strip()
            raw_result = row.iloc[idx["item_result"]]

            if not batch_no or batch_no.lower() == "nan":
                continue

            key = (batch_no, product_no)
            if key not in grouped:
                grouped[key] = ProcessRecord(batch_no, product_no, source_file)

            param_key = self.cleaner.match_param_name(item_name)
            if not param_key:
                continue

            val = self.cleaner.clean_numeric(raw_result)
            grouped[key].set_param(param_key, val)

        return list(grouped.values())

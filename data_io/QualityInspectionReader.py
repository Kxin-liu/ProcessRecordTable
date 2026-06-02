import pandas as pd

from business_logic.QualityInspectionRecord import (
    QualityInspectionParam,
    QualityInspectionRecord,
)


class QualityInspectionReader:
    """Read quality inspection rows and group them by batch."""

    FALLBACK_INDEX = {
        "batch_no": 0,
        "product_no": 2,
        "created_date": 5,
        "item_name": 6,
        "standard_param": 7,
        "inspection_param": 8,
        "equipment_name": 9,
        "is_ok": 10,
        "inspection_value": 11,
    }

    COLUMN_ALIASES = {
        "batch_no": ("批号",),
        "product_no": ("物料品号", "物料编码"),
        "created_date": ("创建日期",),
        "item_name": ("检查项目", "检验项目"),
        "standard_param": ("标准参数",),
        "inspection_param": ("检验参数",),
        "equipment_name": ("设备名称",),
        "is_ok": ("IsOK", "isOK"),
        "inspection_value": ("检验值",),
    }

    def read(self, file_path: str) -> list[QualityInspectionRecord]:
        df = pd.read_excel(file_path, engine="openpyxl")
        return self.read_dataframe(df, file_path)

    def read_dataframe(
        self, df: pd.DataFrame, source_file: str = ""
    ) -> list[QualityInspectionRecord]:
        idx = self._pick_columns(df)
        grouped: dict[tuple[str, str], QualityInspectionRecord] = {}

        for _, row in df.iterrows():
            batch_no = self._normalize_text(row.iloc[idx["batch_no"]])
            product_no = self._normalize_text(row.iloc[idx["product_no"]])
            if not batch_no:
                continue

            created_date = self._normalize_datetime(row.iloc[idx["created_date"]])
            equipment_name = self._normalize_text(row.iloc[idx["equipment_name"]])
            item_name = self._normalize_text(row.iloc[idx["item_name"]])
            rule = self._normalize_text(row.iloc[idx["inspection_param"]])
            if not rule:
                rule = self._normalize_text(row.iloc[idx["standard_param"]])

            key = (batch_no, product_no)
            if key not in grouped:
                grouped[key] = QualityInspectionRecord(
                    batch_no=batch_no,
                    product_no=product_no,
                    created_date=created_date,
                    equipment_name=equipment_name,
                    source_file=source_file,
                )
            elif not grouped[key].equipment_name and equipment_name:
                grouped[key].equipment_name = equipment_name

            grouped[key].add_param(
                QualityInspectionParam(
                    item_name=item_name,
                    raw_rule=rule,
                    raw_value=row.iloc[idx["inspection_value"]],
                    table_is_ok=row.iloc[idx["is_ok"]],
                )
            )

        return list(grouped.values())

    def _pick_columns(self, df: pd.DataFrame) -> dict[str, int]:
        columns = [str(col) for col in df.columns]
        out = {}
        for logical_name, aliases in self.COLUMN_ALIASES.items():
            found = None
            for i, col in enumerate(columns):
                if any(alias in col for alias in aliases):
                    found = i
                    break
            out[logical_name] = (
                found if found is not None else self.FALLBACK_INDEX[logical_name]
            )
        return out

    @staticmethod
    def _normalize_text(raw_val) -> str:
        if raw_val is None:
            return ""
        try:
            if pd.isna(raw_val):
                return ""
        except (TypeError, ValueError):
            pass
        text = str(raw_val).strip()
        if text.lower() in ("nan", "none", "null"):
            return ""
        return text

    @staticmethod
    def _normalize_datetime(raw_val):
        if raw_val is None:
            return None
        try:
            if pd.isna(raw_val):
                return None
        except (TypeError, ValueError):
            pass
        parsed = pd.to_datetime(raw_val, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime().replace(microsecond=0)

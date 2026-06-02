import math
import re


class QualityInspectionParam:
    """One parsed inspection parameter for a batch."""

    NUMBER_PATTERN = re.compile(r"[-+]?\d*\.\d+|[-+]?\d+")

    def __init__(
        self,
        item_name: str,
        raw_rule: str,
        raw_value,
        table_is_ok=None,
    ):
        self.item_name = self._text(item_name)
        self.raw_rule = self._text(raw_rule)
        self.raw_value = raw_value
        self.table_is_ok = self._to_bool(table_is_ok)

        self.param_type = "qualitative"
        self.lower_bound = None
        self.upper_bound = None
        self.qualitative_value = ""
        self.actual_value = self._to_float(raw_value)

        self._parse_rule()
        self.calculated_is_ok = self.calculate_is_ok()

    @staticmethod
    def _text(value) -> str:
        if value is None:
            return ""
        try:
            if hasattr(value, "__float__") and math.isnan(float(value)):
                return ""
        except (TypeError, ValueError):
            pass
        text = str(value).strip()
        if text.lower() in ("nan", "none", "null"):
            return ""
        return text

    @classmethod
    def _to_float(cls, value):
        text = cls._text(value)
        if not text:
            return None
        match = cls.NUMBER_PATTERN.search(text)
        if not match:
            return None
        try:
            return float(match.group())
        except ValueError:
            return None

    @staticmethod
    def _to_bool(value):
        text = QualityInspectionParam._text(value).lower()
        if text in ("true", "1", "yes", "y", "ok"):
            return True
        if text in ("false", "0", "no", "n", "ng"):
            return False
        return None

    def _parse_rule(self):
        rule = self.raw_rule.replace(" ", "")
        numbers = [float(x) for x in self.NUMBER_PATTERN.findall(rule)]

        if not numbers:
            self.param_type = "qualitative"
            if rule.startswith("V="):
                self.qualitative_value = rule[2:]
            else:
                self.qualitative_value = rule
            return

        self.param_type = "quantitative"
        v_pos = rule.upper().find("V")
        if len(numbers) >= 2:
            self.lower_bound = numbers[0]
            self.upper_bound = numbers[1]
            return

        bound = numbers[0]
        if v_pos < 0:
            self.lower_bound = bound
            self.upper_bound = bound
            return

        num_match = self.NUMBER_PATTERN.search(rule)
        num_pos = num_match.start() if num_match else -1
        before_v = rule[:v_pos]
        after_v = rule[v_pos + 1 :]

        if num_pos > v_pos:
            if any(op in after_v for op in (">=", ">", "≥")):
                self.lower_bound = bound
            elif any(op in after_v for op in ("<=", "<", "≤")):
                self.upper_bound = bound
        else:
            if any(op in before_v for op in ("<=", "<", "≤")):
                self.lower_bound = bound
            elif any(op in before_v for op in (">=", ">", "≥")):
                self.upper_bound = bound

    def calculate_is_ok(self) -> bool:
        if self.param_type == "quantitative":
            if self.actual_value is None:
                return False
            if self.lower_bound is not None and self.actual_value < self.lower_bound:
                return False
            if self.upper_bound is not None and self.actual_value > self.upper_bound:
                return False
            return True

        text_value = self._text(self.raw_value).lower()
        return text_value in ("ok", "true", "1", "yes", "y")

    def is_table_mismatch(self) -> bool:
        return self.table_is_ok is not None and self.table_is_ok != self.calculated_is_ok

    def to_quality_table_row(self, batch_no: str, product_no: str) -> dict:
        return {
            "batch_no": batch_no,
            "product_no": product_no,
            "item_name": self.item_name,
            "param_type": self.param_type,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "qualitative_value": self.qualitative_value,
            "actual_value": self.actual_value,
            "is_ok": self.calculated_is_ok,
            "table_is_ok": self.table_is_ok,
        }


class QualityInspectionRecord:
    """Batch-level quality inspection record containing all parsed parameters."""

    def __init__(
        self,
        batch_no: str,
        product_no: str,
        created_date=None,
        equipment_name: str = "",
        source_file: str = "",
    ):
        self.batch_no = str(batch_no).strip()
        self.product_no = str(product_no).strip()
        self.created_date = created_date
        self.equipment_name = str(equipment_name or "").strip()
        self.source_file = source_file
        self.params: list[QualityInspectionParam] = []

    def add_param(self, param: QualityInspectionParam):
        self.params.append(param)

    def quantitative_ok_percent(self) -> float:
        return self._ok_percent("quantitative")

    def qualitative_ok_percent(self) -> float:
        return self._ok_percent("qualitative")

    def mismatch_params(self) -> list[QualityInspectionParam]:
        return [param for param in self.params if param.is_table_mismatch()]

    def _ok_percent(self, param_type: str) -> float:
        params = [param for param in self.params if param.param_type == param_type]
        if not params:
            return 0.0
        return sum(1 for param in params if param.calculated_is_ok) / len(params)

    @property
    def is_ok(self) -> bool:
        return all(param.calculated_is_ok for param in self.params)

    def to_quality_table_rows(self) -> list[dict]:
        return [
            param.to_quality_table_row(self.batch_no, self.product_no)
            for param in self.params
        ]

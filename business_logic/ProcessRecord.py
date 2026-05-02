# 输入: 批号、物料品号、来源文件等元数据
# 输出: 可校验、可入库、可参与向量统计的工艺记录对象
# 关键规则: 8 个参数字段与库表 ht_param_vector 一一对应；to_tuple 顺序固定便于实验二统计


class ProcessRecord:
    """单条批号+物料品号+创建日期下的工艺记录。"""

    PARAM_ORDER = (
        "core_od",
        "jacket_od",
        "inner_die",
        "outer_die",
        "screw_speed",
        "screw_current",
        "prod_speed",
        "actual_prod_speed",
    )
    REQUIRED_FIELDS = PARAM_ORDER + ("created_date", "equipment_name")

    def __init__(
        self,
        batch_no: str,
        product_no: str,
        created_date=None,
        equipment_name: str = "",
        remark_info: str = "",
        source_file: str = "",
    ):
        self.batch_no = batch_no
        self.product_no = product_no
        self.created_date = created_date
        self.equipment_name = equipment_name
        self.remark_info = remark_info
        self.source_file = source_file

        self.core_od = None
        self.jacket_od = None
        self.inner_die = None
        self.outer_die = None
        self.screw_speed = None
        self.screw_current = None
        self.prod_speed = None
        self.actual_prod_speed = None

        self.is_valid = True
        self.invalid_reason_code = ""
        self.invalid_reason_text = ""
        self.error_msg = ""
        self.warning_msg = ""

    def set_param(self, key: str, value):
        """按标准键写入参数；value 应为 float 或 None（勿传 np.nan）。"""
        if key not in self.PARAM_ORDER:
            return
        setattr(self, key, value)

    def validate(self) -> bool:
        """
        逻辑校验：缺失仅告警；物理矛盾与负值为错误。
        实验二可在本方法内扩展更多规则。
        """
        errors = []
        missing = [k for k in self.REQUIRED_FIELDS if self._is_missing_field(k)]
        if missing:
            errors.append(f"缺失必填字段: {','.join(missing)}")

        if self.jacket_od is not None and self.core_od is not None:
            if self.core_od >= self.jacket_od:
                errors.append("缆芯外径>=护套外径")

        if self.outer_die is not None and self.inner_die is not None:
            if self.inner_die >= self.outer_die:
                errors.append("挤出内模>=挤出外模")

        for k in self.PARAM_ORDER:
            v = getattr(self, k)
            if v is not None and v < 0:
                errors.append(f"{k}为负数")

        self.is_valid = len(errors) == 0
        self.error_msg = " | ".join(errors)
        self.warning_msg = ""
        self.invalid_reason_code = "VALIDATION_ERROR" if errors else ""
        self.invalid_reason_text = self.error_msg
        return self.is_valid

    def _is_missing_field(self, field_name: str) -> bool:
        value = getattr(self, field_name)
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        return False

    def process_vector_tuple(self) -> tuple:
        """统计用工艺向量：7数值参数 + 设备名称。"""
        return (
            self.core_od,
            self.jacket_od,
            self.inner_die,
            self.outer_die,
            self.screw_speed,
            self.screw_current,
            self.actual_prod_speed,
            self.equipment_name,
        )

    def to_tuple(self) -> tuple:
        """保持向后兼容：返回 8 维原始参数向量。"""
        return tuple(getattr(self, k) for k in self.PARAM_ORDER)
    

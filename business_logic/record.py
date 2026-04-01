# 输入: 批号、物料品号、来源文件等元数据
# 输出: 可校验、可入库、可参与向量统计的工艺记录对象
# 关键规则: 8 个参数字段与库表 ht_param_vector 一一对应；to_tuple 顺序固定便于实验二统计


class ProcessRecord:
    """单条批号+物料下的 8 维工艺参数向量（实验一）。"""

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

    def __init__(self, batch_no: str, product_no: str, source_file: str = ""):
        self.batch_no = batch_no
        self.product_no = product_no
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
        warnings = []

        missing = [k for k in self.PARAM_ORDER if getattr(self, k) is None]
        if missing:
            warnings.append(f"缺失参数: {','.join(missing)}")

        if self.jacket_od is not None and self.core_od is not None:
            if self.jacket_od <= self.core_od:
                errors.append("护套外径<=缆芯外径")

        if self.outer_die is not None and self.inner_die is not None:
            if self.outer_die <= self.inner_die:
                errors.append("挤出外模<=挤出内模")

        for k in self.PARAM_ORDER:
            v = getattr(self, k)
            if v is not None and v < 0:
                errors.append(f"{k}为负数")

        self.is_valid = len(errors) == 0
        self.error_msg = " | ".join(errors)
        self.warning_msg = " | ".join(warnings)
        return self.is_valid

    def to_tuple(self) -> tuple:
        """8 维参数向量元组，顺序固定，供后续纯度/聚类/哈希等统计使用。"""
        return tuple(getattr(self, k) for k in self.PARAM_ORDER)
# 输入: 原始单元格值、项目名称字符串
# 输出: 清洗后的数值或 None；映射后的标准参数字段名或 None
# 关键规则: 斜杠/空/NaN 视为缺失；"实际生产速度"必须优先于"生产速度"排他匹配

import math
import re

import pandas as pd


class DataCleaner:
    """数据清洗器：数值清洗 + 项目名映射到参数名"""

    def clean_numeric(self, raw_val):
        """
        处理斜杠、空串、None、pandas/numpy 的 NaN，以及带单位的字符串。
        统一输出 float 或 None，避免 np.nan 进入 ProcessRecord/数据库。
        """
        if raw_val is None:
            return None
        try:
            if pd.isna(raw_val):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(raw_val, float) and math.isnan(raw_val):
            return None

        s_val = str(raw_val).strip().lower()
        if s_val in ["/", "nan", "", "none", "null"]:
            return None

        m = re.search(r"[-+]?\d*\.\d+|[-+]?\d+", s_val)
        if not m:
            return None
        try:
            return float(m.group())
        except ValueError:
            return None

    def match_param_name(self, item_name: str):
        """
        字符串排他匹配：先判"实际+速度"，再判"生产速度"且不含"实际"，避免子串冲突。
        螺杆速度仅采集挤塑主机（项目名称含「螺杆速度」且不含「辅助」），忽略辅助挤塑速度行。
        """
        name = str(item_name).strip()

        if "实际" in name and "速度" in name:
            return "actual_prod_speed"
        if "生产速度" in name and "实际" not in name:
            return "prod_speed"
        if "理论" in name and "速度" in name:
            return "prod_speed"
        if "缆芯外径" in name or "芯外径" in name:
            return "core_od"
        if "护套外径" in name:
            return "jacket_od"
        if "内模" in name:
            return "inner_die"
        if "外模" in name:
            return "outer_die"
        if "螺杆电流" in name:
            return "screw_current"
        # 螺杆速度：入库为挤塑主机速度，明确排除「辅助挤塑速度」行
        if "螺杆速度" in name:
            if "辅助" in name:
                return None
            return "screw_speed"
        return None
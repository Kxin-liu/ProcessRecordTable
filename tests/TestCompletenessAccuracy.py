# 自动化测试：证明「不遗漏、不错误」
# - 不遗漏：源表若含 8 类工艺项，则聚合后 8 个字段均非 None
# - 不错误：清洗后的 float 与人工给定的原始语义一致（含带单位字符串）

import unittest

import pandas as pd

from business_logic.logic import DataCleaner
from business_logic.record import ProcessRecord
from data_io.excel_handler import ExcelReader


def _df_from_rows(rows: list[tuple]) -> pd.DataFrame:
    """
    构造与 COLUMN_FALLBACK_INDEX 对齐的 20 列表（列名不含中文，走列序兜底）。
    rows: (批号, 物料品号, 项目名称, 项目记录结果)
    """
    cols = [f"c{i}" for i in range(20)]
    data = []
    for batch, product, item_name, raw_result in rows:
        row = [0] * 20
        row[0] = batch
        row[8] = product
        row[18] = item_name
        row[19] = raw_result
        data.append(row)
    return pd.DataFrame(data, columns=cols)


class TestCompletenessAndAccuracy(unittest.TestCase):
    """证明：单批号+物料在源表具备 8 项时，收集结果无遗漏且数值与原始一致。"""

    def setUp(self):
        self.cleaner = DataCleaner()
        self.reader = ExcelReader(self.cleaner)

    def test_eight_params_no_omission_values_match_original(self):
        """
        不遗漏：8 个标准项各出现一次 -> 8 字段均非 None。
        不错误：期望值与业务约定的原始展示一致（含小数、带单位）。
        """
        # 数值取互不相同的可区分值，便于断言「对错」；几何关系满足 validate
        expected = {
            "core_od": 2.2,
            "jacket_od": 8.5,
            "inner_die": 1.1,
            "outer_die": 2.2,
            "screw_speed": 45.0,
            "screw_current": 55.5,
            "prod_speed": 100.0,
            "actual_prod_speed": 98.5,
        }
        rows = [
            ("B_FULL_001", "P9001", "缆芯外径(mm)", "2.20"),
            ("B_FULL_001", "P9001", "护套外径(mm)", "8.5"),
            ("B_FULL_001", "P9001", "挤出内模", "1.1"),
            ("B_FULL_001", "P9001", "挤出外模", "2.2"),
            ("B_FULL_001", "P9001", "螺杆速度(rpm)-(挤塑主机速度)（转/分）", "45"),
            ("B_FULL_001", "P9001", "螺杆电流", "55.5"),
            ("B_FULL_001", "P9001", "生产速度 (米/分)", "100.0"),
            ("B_FULL_001", "P9001", "实际生产速度 (m/min)", "98.5"),
        ]
        df = _df_from_rows(rows)
        out = self.reader.read_dataframe(df, "synthetic.xlsx")
        self.assertEqual(len(out), 1)
        rec = out[0]
        for k, v in expected.items():
            self.assertIsNotNone(
                getattr(rec, k),
                msg=f"不遗漏失败：字段 {k} 为空",
            )
            self.assertAlmostEqual(
                getattr(rec, k),
                v,
                places=6,
                msg=f"数值错误：字段 {k} 与原始语义不一致",
            )
        rec.validate()
        self.assertTrue(rec.is_valid, msg="完整且合法数据应通过 validate")

    def test_speed_exclusive_no_wrong_bucket(self):
        """生产速度 / 实际生产速度 不得串入对方字段（不错误）。"""
        rows = [
            ("B_SPD", "P1", "生产速度", "20"),
            ("B_SPD", "P1", "实际生产速度", "25"),
        ]
        df = _df_from_rows(rows)
        rec = self.reader.read_dataframe(df, "x")[0]
        self.assertAlmostEqual(rec.prod_speed, 20.0)
        self.assertAlmostEqual(rec.actual_prod_speed, 25.0)

    def test_auxiliary_screw_speed_not_collected(self):
        """辅助挤塑速度行不得写入螺杆速度（不错误）。"""
        rows = [
            ("B_AUX", "P1", "螺杆速度(rpm)-(辅助挤塑速度)（转/分）", "99"),
            ("B_AUX", "P1", "螺杆速度(rpm)-(挤塑主机速度)（转/分）", "40"),
        ]
        df = _df_from_rows(rows)
        rec = self.reader.read_dataframe(df, "x")[0]
        self.assertAlmostEqual(rec.screw_speed, 40.0)
        self.assertNotAlmostEqual(rec.screw_speed, 99.0)

    def test_unit_suffix_parsed_to_same_numeric(self):
        """带单位的原始格与纯数字一致（不错误）。"""
        rows = [
            ("B_UNIT", "P1", "生产速度", "45.0(m/min)"),
        ]
        df = _df_from_rows(rows)
        rec = self.reader.read_dataframe(df, "x")[0]
        self.assertAlmostEqual(rec.prod_speed, 45.0)

    def test_repeated_param_last_row_wins(self):
        """同批号同参数多行时以后值为准（与实现一致，便于审计重复行）。"""
        rows = [
            ("B_REP", "P1", "缆芯外径(mm)", "1.0"),
            ("B_REP", "P1", "缆芯外径(mm)", "2.2"),
        ]
        df = _df_from_rows(rows)
        rec = self.reader.read_dataframe(df, "x")[0]
        self.assertAlmostEqual(rec.core_od, 2.2)

    def test_slash_missing_not_fake_zero(self):
        """缺失符 '/' 不得变成错误数值（应为 None，属于「缺失」而非「错数」）。"""
        rows = [
            ("B_MISS", "P1", "缆芯外径(mm)", "/"),
            ("B_MISS", "P1", "护套外径(mm)", "8.0"),
        ]
        df = _df_from_rows(rows)
        rec = self.reader.read_dataframe(df, "x")[0]
        self.assertIsNone(rec.core_od)
        self.assertAlmostEqual(rec.jacket_od, 8.0)


class TestEightParamCompletenessContract(unittest.TestCase):
    """辅助：用 ProcessRecord 约定检查「8 项是否齐全」。"""

    def test_all_none_when_empty(self):
        r = ProcessRecord("a", "b")
        self.assertFalse(all(getattr(r, k) is not None for k in ProcessRecord.PARAM_ORDER))

    def test_tuple_length_eight(self):
        r = ProcessRecord("a", "b")
        self.assertEqual(len(r.to_tuple()), 8)


if __name__ == "__main__":
    unittest.main(verbosity=2)

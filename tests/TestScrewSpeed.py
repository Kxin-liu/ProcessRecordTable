# 螺杆速度：仅映射挤塑主机行，忽略辅助挤塑行

import unittest

from business_logic.DataCleaner import DataCleaner


class TestScrewSpeedRule(unittest.TestCase):
    def setUp(self):
        self.cleaner = DataCleaner()

    def test_auxiliary_ignored(self):
        self.assertIsNone(
            self.cleaner.match_param_name("螺杆速度(rpm)-(辅助挤塑速度)（转/分）")
        )

    def test_main_host_mapped(self):
        self.assertEqual(
            self.cleaner.match_param_name("螺杆速度(rpm)-(挤塑主机速度)（转/分）"),
            "screw_speed",
        )

    def test_generic_screw_speed_still_mapped(self):
        """仅写「螺杆速度」、未标注辅助时仍视为可入库（常见简写）。"""
        self.assertEqual(self.cleaner.match_param_name("螺杆速度"), "screw_speed")


if __name__ == "__main__":
    unittest.main(verbosity=2)

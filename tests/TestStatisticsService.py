import unittest

from business_logic.ProcessRecord import ProcessRecord
from business_logic.StatisticsService import StatisticsService


def _record(batch_no, product_no, created_date, equipment_name, remark_info, vector):
    r = ProcessRecord(
        batch_no=batch_no,
        product_no=product_no,
        created_date=created_date,
        equipment_name=equipment_name,
        remark_info=remark_info,
    )
    (
        r.core_od,
        r.jacket_od,
        r.inner_die,
        r.outer_die,
        r.screw_speed,
        r.screw_current,
        r.prod_speed,
        r.actual_prod_speed,
    ) = vector
    return r


class TestStatisticsService(unittest.TestCase):
    def setUp(self):
        self.service = StatisticsService()
        self.records = [
            _record("B1", "P1", "2024-06-01", "E1", "返工 主机手杜伟 跟班A", (1, 2, 1, 2, 10, 20, 30, 40)),
            _record("B2", "P1", "2024-06-02", "E1", "返工 主机手杜伟 跟班B", (1, 2, 1, 2, 10, 20, 30, 40)),
            _record("B3", "P1", "2024-06-03", "E2", "返工 主机手黄刚超 跟班C", (1, 3, 1, 4, 10, 20, 30, 50)),
            _record("B4", "P2", "2024-06-03", "E2", "", (1, 3, 1, 4, 10, 20, 30, 50)),
        ]

    def test_material_purity(self):
        out = self.service.material_vector_purity(self.records)
        self.assertIn("P1", out)
        self.assertEqual(len(out["P1"]), 2)

    def test_material_production_count(self):
        out = self.service.material_production_count(self.records)
        self.assertEqual(out["P1"], 3)
        self.assertEqual(out["P2"], 1)

    def test_material_vector_detail(self):
        out = self.service.material_vector_production_detail(self.records)
        self.assertTrue(any(row["product_no"] == "P1" and row["production_count"] == 2 for row in out))


if __name__ == "__main__":
    unittest.main(verbosity=2)

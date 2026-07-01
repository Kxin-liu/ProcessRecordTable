import unittest

from business_logic.CorrelationRegressionService import CorrelationRegressionService
from business_logic.ProcessRecord import ProcessRecord
from business_logic.QualityInspectionRecord import (
    QualityInspectionParam,
    QualityInspectionRecord,
)


def _process(batch_no: str, product_no: str, base: float) -> ProcessRecord:
    record = ProcessRecord(
        batch_no=batch_no,
        product_no=product_no,
        created_date="2024-06-01",
        equipment_name="HT-1",
    )
    record.core_od = base
    record.jacket_od = base + 10
    record.inner_die = base + 1
    record.outer_die = base + 2
    record.screw_speed = base * 2
    record.screw_current = 100 - base
    record.prod_speed = base * 3
    record.actual_prod_speed = base * 3 + 1
    return record


def _quality(batch_no: str, product_no: str, base: float) -> QualityInspectionRecord:
    record = QualityInspectionRecord(batch_no=batch_no, product_no=product_no)
    for idx in range(10):
        value = base * (idx + 1)
        record.add_param(
            QualityInspectionParam(
                item_name=f"定量字段{idx + 1}",
                raw_rule="0<=V<=1000",
                raw_value=str(value),
                table_is_ok=True,
            )
        )
    return record


class TestCorrelationRegressionService(unittest.TestCase):
    def setUp(self):
        self.service = CorrelationRegressionService()
        self.process = [_process(f"B{i}", "P1", float(i)) for i in range(1, 9)]
        self.quality = [_quality(f"B{i}", "P1", float(i)) for i in range(1, 9)]
        self.rows = self.service.build_dataset(self.process, self.quality)

    def test_builds_joined_quantitative_dataset(self):
        self.assertEqual(len(self.rows), 80)
        self.assertEqual(self.rows[0]["target_field"], "定量字段1")
        self.assertIn("core_od", self.rows[0])

    def test_correlation_table_is_8_by_10(self):
        targets = self.service.quantitative_field_names(self.rows, limit=10)
        table = self.service.correlation_table(self.rows, target_fields=targets)["ALL"]
        self.assertEqual(len(table), 8)
        self.assertEqual(len(table["缆芯外径"]), 10)
        self.assertGreater(table["缆芯外径"]["定量字段1"], 0.99)
        self.assertLess(table["螺杆电流"]["定量字段1"], -0.99)

    def test_representative_targets_returns_five_fields(self):
        targets = self.service.representative_targets(self.rows, limit=5)
        self.assertEqual(len(targets), 5)
        self.assertTrue(all(target.startswith("定量字段") for target in targets))


if __name__ == "__main__":
    unittest.main(verbosity=2)

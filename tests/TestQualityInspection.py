import unittest

import pandas as pd

from business_logic.ProcessRecord import ProcessRecord
from business_logic.QualityInspectionRecord import QualityInspectionParam
from business_logic.StatisticsService import StatisticsService
from data_io.QualityInspectionReader import QualityInspectionReader


def _quality_df(rows):
    cols = [f"c{i}" for i in range(13)]
    data = []
    for (
        batch_no,
        product_no,
        created_date,
        item_name,
        standard_param,
        inspection_param,
        equipment_name,
        is_ok,
        inspection_value,
    ) in rows:
        row = [None] * 13
        row[0] = batch_no
        row[2] = product_no
        row[5] = created_date
        row[6] = item_name
        row[7] = standard_param
        row[8] = inspection_param
        row[9] = equipment_name
        row[10] = is_ok
        row[11] = inspection_value
        data.append(row)
    return pd.DataFrame(data, columns=cols)


def _process(batch_no, product_no, vector):
    record = ProcessRecord(
        batch_no, product_no, created_date="2024-06-01", equipment_name="HT-13"
    )
    (
        record.core_od,
        record.jacket_od,
        record.inner_die,
        record.outer_die,
        record.screw_speed,
        record.screw_current,
        record.prod_speed,
        record.actual_prod_speed,
    ) = vector
    return record


class TestQualityInspection(unittest.TestCase):
    def test_quantitative_closed_bounds(self):
        param = QualityInspectionParam("wall", "0.200<=V<=0.550", "0.420", True)
        self.assertEqual(param.param_type, "quantitative")
        self.assertAlmostEqual(param.lower_bound, 0.2)
        self.assertAlmostEqual(param.upper_bound, 0.55)
        self.assertTrue(param.calculated_is_ok)

    def test_quantitative_single_lower_bound(self):
        param = QualityInspectionParam("width", "V>=2.5", "2.600", True)
        self.assertEqual(param.lower_bound, 2.5)
        self.assertIsNone(param.upper_bound)
        self.assertTrue(param.calculated_is_ok)

    def test_quantitative_single_upper_bound(self):
        param = QualityInspectionParam("attenuation", "V<=0.350", "0.400", True)
        self.assertIsNone(param.lower_bound)
        self.assertEqual(param.upper_bound, 0.35)
        self.assertFalse(param.calculated_is_ok)
        self.assertTrue(param.is_table_mismatch())

    def test_qualitative_ok_from_inspection_value(self):
        param = QualityInspectionParam("appearance", "V=smooth surface", "OK", True)
        self.assertEqual(param.param_type, "qualitative")
        self.assertEqual(param.qualitative_value, "smoothsurface")
        self.assertTrue(param.calculated_is_ok)

    def test_reader_groups_batch_and_detects_mismatch(self):
        reader = QualityInspectionReader()
        records = reader.read_dataframe(
            _quality_df(
                [
                    (
                        "B1",
                        "P1",
                        "2024-06-01",
                        "diameter",
                        "0",
                        "1.0<=V<=2.0",
                        "HT-1",
                        True,
                        "1.5",
                    ),
                    (
                        "B1",
                        "P1",
                        "2024-06-01",
                        "appearance",
                        "",
                        "V=good",
                        "HT-1",
                        True,
                        "OK",
                    ),
                    (
                        "B2",
                        "P1",
                        "2024-06-01",
                        "diameter",
                        "0",
                        "1.0<=V<=2.0",
                        "HT-1",
                        True,
                        "2.5",
                    ),
                ]
            ),
            "quality.xlsx",
        )

        self.assertEqual(len(records), 2)
        errors = StatisticsService().quality_isok_error_batches(records)
        self.assertEqual(errors[0]["batch_no"], "B2")

    def test_material_vector_quality_topn(self):
        reader = QualityInspectionReader()
        quality = reader.read_dataframe(
            _quality_df(
                [
                    ("B1", "P1", "2024-06-01", "q1", "0", "1.0<=V<=2.0", "HT-1", True, "1.5"),
                    ("B1", "P1", "2024-06-01", "a1", "", "V=good", "HT-1", True, "OK"),
                    ("B2", "P1", "2024-06-01", "q1", "0", "1.0<=V<=2.0", "HT-1", False, "2.5"),
                    ("B2", "P1", "2024-06-01", "a1", "", "V=good", "HT-1", True, "OK"),
                ]
            )
        )
        process = [
            _process("B1", "P1", (1, 2, 1, 2, 10, 20, 30, 40)),
            _process("B2", "P1", (1, 3, 1, 3, 10, 20, 30, 41)),
        ]

        out = StatisticsService().material_vector_quality_topn(process, quality, n=1)
        self.assertEqual(out["P1"][0]["vector"], process[0].process_vector_tuple())
        self.assertAlmostEqual(out["P1"][0]["quantitative_ok_percent"], 1.0)
        self.assertAlmostEqual(out["P1"][0]["qualitative_ok_percent"], 1.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

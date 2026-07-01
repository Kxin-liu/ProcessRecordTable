from collections import Counter, defaultdict
from typing import Iterable

from business_logic.ProcessRecord import ProcessRecord
from business_logic.QualityInspectionRecord import QualityInspectionRecord


class StatisticsService:
    """工艺参数统计服务：物料维度纯度与质量评分统计。"""

    @staticmethod
    def _count_key_vector(rows: Iterable[tuple[str, tuple]]) -> dict[str, Counter]:
        grouped = defaultdict(Counter)
        for key, vector in rows:
            grouped[key][vector] += 1
        return grouped

    @staticmethod
    def _purity_from_counter(counter: Counter) -> list[dict]:
        distinct_count = len(counter)
        if distinct_count == 0:
            return []
        return [
            {
                "vector": vector,
                "count": count,
                "purity": count / distinct_count,
            }
            for vector, count in counter.items()
        ]

    def material_vector_purity(self, records: list[ProcessRecord]) -> dict[str, list[dict]]:
        grouped = self._count_key_vector(
            (record.product_no, record.process_vector_tuple()) for record in records
        )
        return {key: self._purity_from_counter(counter) for key, counter in grouped.items()}

    def material_production_count(self, records: list[ProcessRecord]) -> dict[str, int]:
        batches_by_product = defaultdict(set)
        for record in records:
            batches_by_product[record.product_no].add(record.batch_no)
        return {k: len(v) for k, v in batches_by_product.items()}

    def material_vector_production_detail(self, records: list[ProcessRecord]) -> list[dict]:
        grouped = self._count_key_vector(
            (record.product_no, record.process_vector_tuple()) for record in records
        )
        out = []
        for product_no, counter in grouped.items():
            for vector, count in counter.items():
                out.append(
                    {
                        "product_no": product_no,
                        "vector": vector,
                        "production_count": count,
                    }
                )
        return out

    def quality_isok_error_batches(
        self, quality_records: list[QualityInspectionRecord]
    ) -> list[dict]:
        """Find batches where the sheet IsOK differs from recalculated IsOK."""
        out = []
        for record in quality_records:
            mismatches = record.mismatch_params()
            if not mismatches:
                continue
            out.append(
                {
                    "batch_no": record.batch_no,
                    "product_no": record.product_no,
                    "mismatch_count": len(mismatches),
                    "items": [param.item_name for param in mismatches],
                }
            )
        return out

    def material_vector_quality_topn(
        self,
        process_records: list[ProcessRecord],
        quality_records: list[QualityInspectionRecord],
        n: int = 3,
    ) -> dict[str, list[dict]]:
        """
        Rank process vectors by purity and quality scores.

        Sort order: quantitative OK percent, qualitative OK percent, purity, production
        count. This gives priority to measured quality while keeping stable vectors ahead
        when quality scores tie.
        """
        quality_by_batch = {record.batch_no: record for record in quality_records}
        grouped = defaultdict(list)
        for record in process_records:
            quality = quality_by_batch.get(record.batch_no)
            if not quality:
                continue
            grouped[(record.product_no, record.process_vector_tuple())].append(
                (record, quality)
            )

        product_vector_rows = defaultdict(list)
        distinct_vectors_by_product = Counter(product_no for product_no, _ in grouped)
        for (product_no, vector), pairs in grouped.items():
            quantitative_scores = [
                quality.quantitative_ok_percent() for _, quality in pairs
            ]
            qualitative_scores = [
                quality.qualitative_ok_percent() for _, quality in pairs
            ]
            row = {
                "product_no": product_no,
                "vector": vector,
                "production_count": len({record.batch_no for record, _ in pairs}),
                "purity": len(pairs) / distinct_vectors_by_product[product_no],
                "quantitative_ok_percent": sum(quantitative_scores)
                / len(quantitative_scores),
                "qualitative_ok_percent": sum(qualitative_scores)
                / len(qualitative_scores),
            }
            row["quality_rank_score"] = (
                row["quantitative_ok_percent"],
                row["qualitative_ok_percent"],
                row["purity"],
                row["production_count"],
            )
            product_vector_rows[product_no].append(row)

        for product_no, rows in product_vector_rows.items():
            rows.sort(
                key=lambda row: (
                    row["quantitative_ok_percent"],
                    row["qualitative_ok_percent"],
                    row["purity"],
                    row["production_count"],
                ),
                reverse=True,
            )
            product_vector_rows[product_no] = rows[:n]

        return dict(product_vector_rows)

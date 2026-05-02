import re
from collections import Counter, defaultdict
from typing import Iterable

from business_logic.ProcessRecord import ProcessRecord


class StatisticsService:
    """工艺参数统计服务：物料维度与操机手维度纯度统计。"""

    OPERATOR_PATTERN = re.compile(r"主机手\s*([^\s]+)")

    @staticmethod
    def extract_operator(remark_info: str) -> str:
        text = (remark_info or "").strip()
        match = StatisticsService.OPERATOR_PATTERN.search(text)
        if match:
            return match.group(1).strip()
        return "UNKNOWN_OPERATOR"

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

    def operator_vector_purity(self, records: list[ProcessRecord]) -> dict[str, list[dict]]:
        grouped = self._count_key_vector(
            (self.extract_operator(record.remark_info), record.process_vector_tuple())
            for record in records
        )
        return {key: self._purity_from_counter(counter) for key, counter in grouped.items()}

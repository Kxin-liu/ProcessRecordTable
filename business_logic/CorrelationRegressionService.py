from collections import defaultdict
from math import sqrt

from business_logic.ProcessRecord import ProcessRecord
from business_logic.QualityInspectionRecord import QualityInspectionRecord


class CorrelationRegressionService:
    """Analyze quantitative quality fields against the 8 process parameters."""

    FEATURE_FIELDS = ProcessRecord.PARAM_ORDER
    FEATURE_LABELS = {
        "core_od": "缆芯外径",
        "jacket_od": "护套外径",
        "inner_die": "挤出内模",
        "outer_die": "挤出外模",
        "screw_speed": "螺杆速度",
        "screw_current": "螺杆电流",
        "prod_speed": "生产速度",
        "actual_prod_speed": "实际生产速度",
    }

    def build_dataset(
        self,
        process_records: list[ProcessRecord],
        quality_records: list[QualityInspectionRecord],
    ) -> list[dict]:
        """Join process records with quantitative quality params by batch/product."""
        quality_by_key = defaultdict(list)
        for record in quality_records:
            quality_by_key[(record.batch_no, record.product_no)].append(record)

        rows = []
        for process in process_records:
            key = (process.batch_no, process.product_no)
            for quality in quality_by_key.get(key, []):
                for param in quality.params:
                    if param.param_type != "quantitative" or param.actual_value is None:
                        continue
                    row = {
                        "batch_no": process.batch_no,
                        "product_no": process.product_no,
                        "equipment_name": process.equipment_name,
                        "target_field": param.item_name,
                        "target_value": param.actual_value,
                    }
                    for field in self.FEATURE_FIELDS:
                        row[field] = getattr(process, field)
                    rows.append(row)
        return rows

    def quantitative_field_names(self, rows: list[dict], limit: int = 10) -> list[str]:
        """Pick representative target fields by sample size."""
        counts = defaultdict(int)
        for row in rows:
            counts[row["target_field"]] += 1
        return [
            name
            for name, _ in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[
                :limit
            ]
        ]

    def correlation_table(
        self,
        rows: list[dict],
        target_fields: list[str] | None = None,
        by_product: bool = False,
    ) -> dict:
        """
        Return an 8 x N Pearson correlation table.

        The cell sign is the positive/negative direction. The absolute value is the
        strength, where values closer to 1 mean stronger linear correlation.
        """
        if target_fields is None:
            target_fields = self.quantitative_field_names(rows, limit=10)

        if by_product:
            grouped_rows = defaultdict(list)
            for row in rows:
                grouped_rows[row["product_no"]].append(row)
        else:
            grouped_rows = {"ALL": rows}

        result = {}
        for group_key, group_rows in grouped_rows.items():
            matrix = {}
            for feature in self.FEATURE_FIELDS:
                matrix[self.FEATURE_LABELS[feature]] = {}
                for target in target_fields:
                    pairs = [
                        (row[feature], row["target_value"])
                        for row in group_rows
                        if row["target_field"] == target
                        and row.get(feature) is not None
                        and row.get("target_value") is not None
                    ]
                    matrix[self.FEATURE_LABELS[feature]][target] = self._pearson(pairs)
            result[group_key] = matrix
        return result

    def representative_targets(self, rows: list[dict], limit: int = 5) -> list[str]:
        """Choose high-sample and high-correlation target fields for regression."""
        target_fields = self.quantitative_field_names(rows, limit=10)
        table = self.correlation_table(rows, target_fields=target_fields)["ALL"]
        scored = []
        for target in target_fields:
            sample_count = sum(1 for row in rows if row["target_field"] == target)
            values = [
                abs(feature_row[target])
                for feature_row in table.values()
                if feature_row[target] is not None
            ]
            max_abs_corr = max(values) if values else 0.0
            scored.append((target, sample_count, max_abs_corr))
        scored.sort(key=lambda item: (-item[1], -item[2], item[0]))
        return [target for target, _, _ in scored[:limit]]

    def train_regression_predictors(
        self,
        rows: list[dict],
        target_fields: list[str] | None = None,
        include_product_no: bool = True,
        include_equipment_name: bool = True,
        test_size: float = 0.25,
        random_state: int = 42,
    ) -> dict[str, dict]:
        """Train one RandomForest regressor per representative quantitative field."""
        try:
            from sklearn.compose import ColumnTransformer
            from sklearn.ensemble import RandomForestRegressor
            from sklearn.metrics import mean_absolute_error, r2_score
            from sklearn.model_selection import train_test_split
            from sklearn.pipeline import Pipeline
            from sklearn.preprocessing import OneHotEncoder
        except ImportError as exc:
            raise RuntimeError(
                "回归预测需要安装 scikit-learn：pip install scikit-learn"
            ) from exc

        if target_fields is None:
            target_fields = self.representative_targets(rows, limit=5)

        out = {}
        categorical = []
        if include_product_no:
            categorical.append("product_no")
        if include_equipment_name:
            categorical.append("equipment_name")
        feature_columns = list(self.FEATURE_FIELDS) + categorical

        for target in target_fields:
            target_rows = [
                row
                for row in rows
                if row["target_field"] == target
                and row["target_value"] is not None
                and all(row.get(field) is not None for field in self.FEATURE_FIELDS)
            ]
            if len(target_rows) < 4:
                out[target] = {
                    "sample_count": len(target_rows),
                    "trained": False,
                    "reason": "样本数不足，至少需要 4 条可用记录",
                }
                continue

            x = [{col: row.get(col, "") for col in feature_columns} for row in target_rows]
            y = [row["target_value"] for row in target_rows]
            x_train, x_test, y_train, y_test = train_test_split(
                x, y, test_size=test_size, random_state=random_state
            )
            preprocess = ColumnTransformer(
                transformers=[
                    ("category", OneHotEncoder(handle_unknown="ignore"), categorical),
                ],
                remainder="passthrough",
            )
            model = Pipeline(
                steps=[
                    ("preprocess", preprocess),
                    (
                        "model",
                        RandomForestRegressor(
                            n_estimators=100,
                            random_state=random_state,
                        ),
                    ),
                ]
            )
            model.fit(x_train, y_train)
            predictions = model.predict(x_test)
            tolerance = self._accuracy_tolerance(y_train)
            hits = [
                abs(predicted - actual) <= tolerance
                for predicted, actual in zip(predictions, y_test)
            ]
            accuracy = sum(hits) / len(hits) if hits else 0.0
            out[target] = {
                "sample_count": len(target_rows),
                "trained": True,
                "mae": mean_absolute_error(y_test, predictions),
                "r2": r2_score(y_test, predictions) if len(y_test) >= 2 else 0.0,
                "precision": accuracy,
                "recall": accuracy,
                "tolerance": tolerance,
            }
        return out

    @staticmethod
    def _pearson(pairs: list[tuple[float, float]]):
        if len(pairs) < 2:
            return None
        xs = [float(x) for x, _ in pairs]
        ys = [float(y) for _, y in pairs]
        mean_x = sum(xs) / len(xs)
        mean_y = sum(ys) / len(ys)
        numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        denom_x = sqrt(sum((x - mean_x) ** 2 for x in xs))
        denom_y = sqrt(sum((y - mean_y) ** 2 for y in ys))
        if denom_x == 0 or denom_y == 0:
            return None
        return numerator / (denom_x * denom_y)

    @staticmethod
    def _accuracy_tolerance(values: list[float]) -> float:
        if not values:
            return 0.0
        spread = max(values) - min(values)
        mean_abs = sum(abs(v) for v in values) / len(values)
        return max(spread * 0.1, mean_abs * 0.05, 1e-9)

from pathlib import Path

from business_logic.CorrelationRegressionService import CorrelationRegressionService
from business_logic.DataCleaner import DataCleaner
from data_io.ExcelReader import ExcelReader
from data_io.QualityInspectionReader import QualityInspectionReader


def _find_files(prefix: str) -> list[str]:
    out = []
    for path in Path(".").iterdir():
        if path.name.startswith("~$"):
            continue
        if path.suffix.lower() in (".xlsx", ".xls") and path.name.startswith(prefix):
            out.append(str(path))
    return sorted(out)


def _read_process_records() -> list:
    reader = ExcelReader(DataCleaner())
    records = []
    for path in _find_files("HT_"):
        records.extend(reader.read(path))
    return records


def _read_quality_records() -> list:
    reader = QualityInspectionReader()
    records = []
    for path in _find_files("品质检验"):
        records.extend(reader.read(path))
    return records


def _format_corr(value) -> str:
    if value is None:
        return ""
    direction = "正相关" if value >= 0 else "负相关"
    return f"{direction} {value:.3f}"


def _print_correlation_table(title: str, matrix: dict):
    print(f"\n{title}")
    if not matrix:
        print("无可用数据")
        return
    target_fields = list(next(iter(matrix.values())).keys())
    print("\t" + "\t".join(target_fields))
    for feature_name, row in matrix.items():
        cells = [_format_corr(row[target]) for target in target_fields]
        print(feature_name + "\t" + "\t".join(cells))


def main():
    process_records = _read_process_records()
    quality_records = _read_quality_records()
    service = CorrelationRegressionService()
    rows = service.build_dataset(process_records, quality_records)

    print(
        f"Loaded process records={len(process_records)}, "
        f"quality records={len(quality_records)}, joined quantitative rows={len(rows)}"
    )
    target_fields = service.quantitative_field_names(rows, limit=10)
    table = service.correlation_table(rows, target_fields=target_fields, by_product=False)
    _print_correlation_table("8x10 Pearson 关联分析表（不区分物料品号）", table["ALL"])

    by_product = service.correlation_table(rows, target_fields=target_fields, by_product=True)
    for product_no, matrix in by_product.items():
        _print_correlation_table(f"8x10 Pearson 关联分析表（物料品号={product_no}）", matrix)

    targets = service.representative_targets(rows, limit=5)
    print("\n5 个代表性定量字段回归预测指标")
    metrics = service.train_regression_predictors(rows, target_fields=targets)
    for target, metric in metrics.items():
        if not metric["trained"]:
            print(f"{target}\t样本={metric['sample_count']}\t未训练：{metric['reason']}")
            continue
        print(
            f"{target}\t样本={metric['sample_count']}\t"
            f"MAE={metric['mae']:.4f}\tR2={metric['r2']:.4f}\t"
            f"precision={metric['precision']:.4f}\trecall={metric['recall']:.4f}\t"
            f"容差={metric['tolerance']:.4f}"
        )


if __name__ == "__main__":
    main()

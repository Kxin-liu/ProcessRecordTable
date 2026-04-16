# 这是一个工艺参数持久化的项目（shit

1. MySQL 执行 `schema.sql`
2. 将 `HT_*.xlsx` （teacher给的实际数据表格）放入本目录
3. `python main.py`

## 自动化测试（依旧shit）

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

`tests/test_completeness_accuracy.py` 用构造数据证明：8 项齐全时字段无空、数值与原始格一致；并覆盖速度排他、主机/辅助螺杆、带单位、`/` 缺失等情形。

结构：`core`（领域对象与清洗逻辑）、`io_utils`（Excel 与数据库）、`main.py`（流水线编排）。


# 这是一个工艺参数持久化的项目（shit

在 `exp1` 目录下执行：

1. MySQL 执行 `schema.sql`
2. 将 `HT_*.xlsx` 放入本目录
3. `python main.py`

## 自动化测试（不遗漏、不错误）

在 `exp1` 目录下：

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

`tests/test_completeness_accuracy.py` 用构造数据证明：8 项齐全时字段无空、数值与原始格一致；并覆盖速度排他、主机/辅助螺杆、带单位、`/` 缺失等情形。

结构：`core`（领域对象与清洗逻辑）、`io_utils`（Excel 与数据库）、`main.py`（流水线编排）。

螺杆速度：库表注释仍为「螺杆速度」四字；业务上仅写入挤塑主机对应行，名称含「辅助」的螺杆速度行忽略。

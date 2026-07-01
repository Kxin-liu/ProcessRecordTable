# ProcessRecordTable

一个面向制造场景的数据工程小项目：将工序 Excel 与品质检验 Excel 导入 MySQL，并对工艺参数进行有效性分流与统计分析。项目重点是**可扩展的数据处理架构**，而不是一次性脚本。

## 项目亮点

- **业务规则落地**：将工艺记录按规则校验，自动分流到“有效记录表 / 无效记录表”。
- **面向变化设计**：新增字段（创建日期、设备名称、备注）与新增导入源（品质检验）时，核心架构无需推翻。
- **统计能力可复用**：抽象了“key + 工艺向量 + 纯度”的统一统计逻辑，支持物料维度统计。
- **相关与预测分析**：支持品质检验定量字段与 8 个工艺参数的 Pearson 相关分析，并用 Random Forest 建立回归预测器。
- **工程可用性**：包含单元测试、批量入库、动态表结构读取、基础 `.gitignore` 规范。

## 功能概览

### 1) 工序数据导入（`DataImportMain.py`）
- 扫描 `HT_*.xlsx`
- 读取并聚合为工艺记录（`batch_no + product_no + created_date`）
- 清洗参数值（兼容空值、单位、异常文本）
- 校验业务规则后分流：
  - `ht_param_vector`（有效）
  - `ht_invalid_param_vector`（无效，附原因）

### 2) 品质检验导入（`QualityImportMain.py`）
- 扫描 `品质检验*.xlsx`
- 按“原始行 JSON”落库到 `quality_inspection_raw`
- 为后续质量度量预留数据基础

### 3) 统计服务（`business_logic/StatisticsService.py`）
- 物料维度：
  - 不同工艺向量数量
  - 向量纯度（出现次数 / 该 key 下不同向量数）
  - 生产次数（不同批号）

### 4) 相关分析与回归预测（`CorrelationRegressionMain.py`）
- 扫描所有可用的 `HT_*.xlsx` 与 `品质检验*.xlsx`
- 将工序记录与品质检验定量参数按 `批号 + 物料品号` 关联
- 输出 8x10 Pearson 关联分析表：
  - 行：缆芯外径、护套外径、挤出内模、挤出外模、螺杆速度、螺杆电流、生产速度、实际生产速度
  - 列：样本数最多的 10 个品质检验定量字段
  - 单元格：正相关/负相关 + Pearson r 值
- 分别在“不区分物料品号”和“按物料品号分组”的基础上生成关联表
- 选择最有代表性的 5 个定量字段，使用 RandomForestRegressor 建立回归预测器
- 回归指标输出 `MAE`、`R2`、`precision`、`recall`；其中 precision/recall 按“预测值落入训练集尺度自适应容差内”定义命中率

本次实验已去除“备注信息”处理需求，不再统计每个操机手的纯度信息。

## 技术栈

- Python 3
- pandas / openpyxl
- MySQL（DBeaver 可视化管理）
- unittest

## 项目结构

```text
exp2/
├─ DataImportMain.py
├─ QualityImportMain.py
├─ DatabaseConfig.py
├─ schema.sql
├─ business_logic/
│  ├─ DataCleaner.py
│  ├─ ProcessRecord.py
│  ├─ StreamProcessor.py
│  ├─ LargeExcelProcessor.py
│  ├─ CorrelationRegressionService.py
│  └─ StatisticsService.py
├─ data_io/
│  ├─ ExcelReader.py
│  └─ DatabaseClient.py
└─ tests/
   ├─ TestCompletenessAccuracy.py
   ├─ TestScrewSpeed.py
   └─ TestStatisticsService.py
```

## 快速开始

### 1. 初始化数据库
在 MySQL 中执行：

```sql
source schema.sql;
```

或直接在 DBeaver 打开并执行 `schema.sql`。

### 2. 配置连接
编辑 `DatabaseConfig.py`：

- `host`
- `user`
- `password`
- `database`（默认 `factory_data`）

### 3. 导入数据

```bash
python DataImportMain.py
python QualityImportMain.py
```

### 4. 运行相关分析与回归预测

```bash
python CorrelationRegressionMain.py
```

### 5. 运行测试

```bash
python -m unittest discover -s tests -p "Test*.py" -v
```

## 设计思路（面试可讲）

- **分层解耦**：
  - `business_logic` 负责领域规则
  - `data_io` 负责外部 I/O
  - main 脚本负责编排
- **低耦合扩展**：新增字段和新导入源时，改动集中在模型、映射和入口，不影响整体流程。
- **可审计性**：无效记录单独入库并记录原因，便于与业务方复盘数据质量。

## 后续可扩展方向

- 增加统计结果导出（CSV/API）
- 引入调度与增量导入
- 接入数据质量报表与可视化看板

---

如果你是面试官，我建议关注这个项目的三点：
1. 业务规则如何被结构化编码。
2. 我如何处理“需求变化”而不是只完成一次功能。
3. 我如何在小项目里体现工程质量（测试、分层、可维护性）。

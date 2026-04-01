# 输入: Excel 路径、DataCleaner
# 输出: ProcessRecord 列表（每行批号+物料聚合为一条）
# 关键规则: 经 clean_numeric 后不得残留 np.nan；列定位与实验一一致（别名+兜底列序）

import pandas as pd  # 导入pandas库，用于Excel文件处理

from DatabaseConfig import COLUMN_ALIASES, COLUMN_FALLBACK_INDEX  # 导入列映射配置
from business_logic.record import ProcessRecord  # 导入工艺记录类
from business_logic.logic import DataCleaner  # 导入数据清洗器


class ExcelReader:
    """用 pandas 读取工序明细 Excel，聚合为 ProcessRecord 列表。"""

    def __init__(self, cleaner: DataCleaner):  # 初始化Excel读取器
        self.cleaner = cleaner  # 保存数据清洗器引用

    def _pick_columns(self, df: pd.DataFrame) -> dict:  # 智能定位Excel列位置
        columns = list(df.columns)  # 获取Excel所有列名
        index_map = {}  # 初始化列索引映射字典
        for logical_name, aliases in COLUMN_ALIASES.items():  # 遍历逻辑列名和别名
            found = None  # 初始化找到的列索引
            for i, c in enumerate(columns):  # 遍历Excel物理列
                c_text = str(c)  # 转换列为字符串
                if any(alias in c_text for alias in aliases):  # 检查列名包含任一别名
                    found = i  # 记录匹配的列索引
                    break  # 找到就停止
            if found is None:  # 如果没找到匹配列
                found = COLUMN_FALLBACK_INDEX[logical_name]  # 使用兜底列序
            index_map[logical_name] = found  # 保存逻辑名到物理索引的映射
        return index_map  # 返回列索引映射

    def read(self, file_path: str) -> list[ProcessRecord]:  # 读取Excel文件主接口
        df = pd.read_excel(file_path, engine="openpyxl")  # pandas读取Excel文件
        return self.read_dataframe(df, file_path)   # 调用核心处理逻辑

    def read_dataframe(self, df: pd.DataFrame, source_file: str = "") -> list[ProcessRecord]:
        """与 read 相同逻辑，便于测试用构造好的 DataFrame 做「不遗漏、不错误」断言。"""
        idx = self._pick_columns(df)  # 获取列索引映射
        grouped: dict[tuple[str, str], ProcessRecord] = {}  # 初始化分组字典

        for _, row in df.iterrows():  # 遍历DataFrame每一行
            batch_no = str(row.iloc[idx["batch_no"]]).strip()  # 提取并清理批号
            product_no = str(row.iloc[idx["product_no"]]).strip()  # 提取并清理物料品号
            item_name = str(row.iloc[idx["item_name"]]).strip()  # 提取并清理项目名称
            raw_result = row.iloc[idx["item_result"]]  # 提取项目记录结果

            if not batch_no or batch_no.lower() == "nan":  # 检查批号是否有效
                continue  # 跳过无效批号行

            key = (batch_no, product_no)  # 创建分组键（批号+物料）
            if key not in grouped:  # 检查是否新组
                grouped[key] = ProcessRecord(batch_no, product_no, source_file)  # 创建新记录对象

            param_key = self.cleaner.match_param_name(item_name)  # 映射项目名称到参数名
            if not param_key:  # 检查映射是否成功
                continue  # 跳过无法识别的项目

            val = self.cleaner.clean_numeric(raw_result)  # 清洗数值数据
            grouped[key].set_param(param_key, val)  # 设置参数值到记录对象

        return list(grouped.values())  # 返回所有聚合的工艺记录列表

# 输入: 数据库连接配置
# 输出: 批量 REPLACE INTO 持久化
# 关键规则: 参数化 SQL；与 ht_param_vector 表结构一致

import pymysql

from business_logic.ProcessRecord import ProcessRecord


class DatabaseClient:
    """封装 MySQL REPLACE INTO，支持流式批量写入，避免内存堆积。"""

    def __init__(self, db_cfg: dict, batch_size: int = 1000):
        self._db_cfg = db_cfg
        self._batch_size = batch_size
        self._connection = None
        self._table_fields = None  # 缓存表字段信息

    def _get_connection(self):
        """获取数据库连接，支持复用"""
        if self._connection is None or self._connection.open is False:
            self._connection = pymysql.connect(**self._db_cfg)
        return self._connection

    def get_table_schema(self, table_name: str) -> list[str]:
        """
        从数据库读取表的字段列表和顺序
        使用反射获取真实的表结构，避免硬编码

        Args:
            table_name: 表名，如"ht_param_vector"

        Returns:
            list[str]: 按数据库中定义顺序的字段列表

        Raises:
            Exception: 如果无法读取表schema，抛出异常
        """
        if self._table_fields is not None:
            return self._table_fields  # 返回缓存的结果

        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 查询表的所有字段信息
                cur.execute(f"SHOW COLUMNS FROM {table_name}")
                columns = cur.fetchall()
                if not columns:
                    raise Exception(f"表 {table_name} 不存在或无字段")
                # 提取字段名（第一列是字段名）
                self._table_fields = [column[0] for column in columns]
                return self._table_fields
        except Exception as e:
            # 如果读取失败，抛出异常让调用者处理
            print(f"读取表schema失败: {e}")
            raise Exception(f"无法读取表 {table_name} 的schema: {e}")

    def replace_many_streaming(self, records: list[ProcessRecord], table_name: str = "ht_param_vector") -> None:
        """
        流式批量替换：达到批次大小时立即入库，清空内存
        使用动态schema，完全移除硬编码字段

        Args:
            records: ProcessRecord对象列表
            table_name: 目标表名，默认为"ht_param_vector"

        处理流程：
        1. 从数据库读取表schema获取字段列表
        2. 动态生成SQL语句
        3. 使用反射从ProcessRecord获取字段值
        4. 批量执行并提交
        """
        if not records:
            return

        # 从数据库获取表结构
        db_fields = self.get_table_schema(table_name)

        # 动态生成SQL语句（字段部分）
        field_names = ", ".join(db_fields)
        placeholders = ", ".join(["%s"] * len(db_fields))
        sql = f"REPLACE INTO {table_name} ({field_names}) VALUES ({placeholders})"

        batch_count = 0
        total_count = 0

        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 分批处理
                for i, record in enumerate(records):
                    # 使用反射动态获取字段值
                    row = self._extract_row_values(record, db_fields)
                    cur.execute(sql, row)

                    # 达到批次大小，提交并计数
                    if (i + 1) % self._batch_size == 0:
                        conn.commit()
                        batch_count += 1
                        total_count += self._batch_size

                # 提交剩余的数据
                remaining = len(records) % self._batch_size
                if remaining != 0:
                    conn.commit()
                    total_count += remaining

                print(f"已分 {batch_count + 1} 批次，总计 {total_count} 条记录入库")

        except Exception as e:
            conn.rollback()
            raise e

    def _extract_row_values(self, record: ProcessRecord, db_fields: list[str]) -> tuple:
        """
        使用反射从ProcessRecord中提取字段值
        根据数据库字段顺序返回对应的值

        Args:
            record: ProcessRecord对象
            db_fields: 数据库字段列表

        Returns:
            tuple: 按字段顺序的值元组
        """
        row = []
        for field_name in db_fields:
            # 使用反射获取实例属性值
            value = getattr(record, field_name, None)

            # 特殊处理布尔值（数据库用整数存储）
            if field_name == "is_valid":
                row.append(int(value if value is not None else True))
            # 特殊处理字符串字段（None转空字符串）
            elif field_name in ["error_msg", "warning_msg"]:
                row.append(value if value else "")
            else:
                row.append(value)

        return tuple(row)

    def replace_many(self, records: list[ProcessRecord], table_name: str = "ht_param_vector") -> None:
        """
        保持原接口兼容，默认使用流式处理
        支持指定目标表名

        Args:
            records: ProcessRecord对象列表
            table_name: 目标表名，默认为"ht_param_vector"
        """
        self.replace_many_streaming(records, table_name)

    def close(self):
        """手动关闭连接"""
        if self._connection and self._connection.open:
            self._connection.close()

    def __del__(self):
        """析构时自动关闭连接"""
        self.close()

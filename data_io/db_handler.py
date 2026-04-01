# 输入: 数据库连接配置
# 输出: 批量 REPLACE INTO 持久化
# 关键规则: 参数化 SQL；与 ht_param_vector 表结构一致

import pymysql

from business_logic.record import ProcessRecord


class DatabaseClient:
    """封装 MySQL REPLACE INTO，支持流式批量写入，避免内存堆积。"""

    def __init__(self, db_cfg: dict, batch_size: int = 1000):
        self._db_cfg = db_cfg
        self._batch_size = batch_size
        self._connection = None

    def _get_connection(self):
        """获取数据库连接，支持复用"""
        if self._connection is None or self._connection.open is False:
            self._connection = pymysql.connect(**self._db_cfg)
        return self._connection

    def replace_many_streaming(self, records: list[ProcessRecord]) -> None:
        """
        流式批量替换：达到批次大小时立即入库，清空内存
        适用于大数据量处理，避免内存堆积
        """
        if not records:
            return

        sql = """
        REPLACE INTO ht_param_vector (
            batch_no, product_no, source_file,
            core_od, jacket_od, inner_die, outer_die,
            screw_speed, screw_current, prod_speed, actual_prod_speed,
            is_valid, error_msg, warning_msg
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        batch_count = 0
        total_count = 0

        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 分批处理
                for i, record in enumerate(records):
                    row = (
                        record.batch_no,
                        record.product_no,
                        record.source_file,
                        record.core_od,
                        record.jacket_od,
                        record.inner_die,
                        record.outer_die,
                        record.screw_speed,
                        record.screw_current,
                        record.prod_speed,
                        record.actual_prod_speed,
                        int(record.is_valid),
                        record.error_msg or "",
                        record.warning_msg or "",
                    )
                    cur.execute(sql, row)

                    # 达到批次大小，提交并计数
                    if (i + 1) % self._batch_size == 0:
                        conn.commit()
                        batch_count += 1
                        total_count += self._batch_size

                # 提交剩余的数据
                if len(records) % self._batch_size != 0:
                    conn.commit()
                    total_count += len(records) % self._batch_size

                print(f"已分 {batch_count + 1} 批次，总计 {total_count} 条记录入库")

        except Exception as e:
            conn.rollback()
            raise e

    def replace_many(self, records: list[ProcessRecord]) -> None:
        """保持原接口兼容，默认使用流式处理"""
        self.replace_many_streaming(records)

    def close(self):
        """手动关闭连接"""
        if self._connection and self._connection.open:
            self._connection.close()

    def __del__(self):
        """析构时自动关闭连接"""
        self.close()

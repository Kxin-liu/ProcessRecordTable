# 流式处理器：优化内存使用和性能平衡
# 适用于超大Excel文件的处理

import gc
from typing import List
from data_io.DatabaseClient import DatabaseClient
from business_logic.ProcessRecord import ProcessRecord


class StreamProcessor:
    """流式数据处理器，平衡内存使用和性能"""

    def __init__(self, db_client: DatabaseClient, batch_size: int = 500, memory_threshold: int = 10000):
        """
        初始化流式处理器

        Args:
            db_client: 数据库客户端
            batch_size: 批处理大小（单次入库的记录数）
            memory_threshold: 内存阈值，超过时触发垃圾回收
        """
        self.db_client = db_client
        self.batch_size = batch_size
        self.memory_threshold = memory_threshold

    def process_file_streaming(self, records: List[ProcessRecord]) -> tuple[int, int]:
        """
        流式处理文件中的所有记录

        Args:
            records: 从Excel读取的记录列表

        Returns:
            tuple: (总记录数, 有效记录数)
        """
        total_count = 0
        valid_count = 0
        current_valid_batch = []
        current_invalid_batch = []

        for record in records:
            # 验证记录
            record.validate()
            if record.is_valid:
                valid_count += 1
                current_valid_batch.append(record)
            else:
                current_invalid_batch.append(record)
            total_count += 1

            # 达到批次大小时入库
            if len(current_valid_batch) >= self.batch_size:
                self._batch_insert_valid(current_valid_batch)
                current_valid_batch.clear()
            if len(current_invalid_batch) >= self.batch_size:
                self._batch_insert_invalid(current_invalid_batch)
                current_invalid_batch.clear()

            # 定期垃圾回收，防止内存溢出
            if total_count % (self.batch_size * 5) == 0:
                self._force_gc()

        # 处理剩余的记录
        if current_valid_batch:
            self._batch_insert_valid(current_valid_batch)
        if current_invalid_batch:
            self._batch_insert_invalid(current_invalid_batch)

        print(f"文件处理完成: 总记录={total_count}, 有效={valid_count}")
        return total_count, valid_count

    def _batch_insert_valid(self, records: List[ProcessRecord]) -> None:
        """批量插入有效记录到主表"""
        if not records:
            return
        self.db_client.replace_many_streaming(records, table_name="ht_param_vector")

    def _batch_insert_invalid(self, records: List[ProcessRecord]) -> None:
        """批量插入无效记录到无效表"""
        if not records:
            return
        self.db_client.replace_many_streaming(records, table_name="ht_invalid_param_vector")

    def _force_gc(self) -> None:
        """强制垃圾回收"""
        gc.collect()
        import sys
        if hasattr(sys, 'internals'):
            sys.internals.clear_memory()

    def close(self):
        """关闭连接"""
        self.db_client.close()

# 流式处理器：优化内存使用和性能平衡
# 适用于超大Excel文件的处理

import gc
from typing import List, Generator
from data_io.db_handler import DatabaseClient
from business_logic.record import ProcessRecord


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
        current_batch = []

        for record in records:
            # 验证记录
            record.validate()
            if record.is_valid:
                valid_count += 1

            current_batch.append(record)
            total_count += 1

            # 达到批次大小时入库
            if len(current_batch) >= self.batch_size:
                self._batch_insert(current_batch)
                current_batch.clear()

                # 定期垃圾回收，防止内存溢出
                if total_count % (self.batch_size * 5) == 0:
                    self._force_gc()

        # 处理剩余的记录
        if current_batch:
            self._batch_insert(current_batch)

        print(f"文件处理完成: 总记录={total_count}, 有效={valid_count}")
        return total_count, valid_count

    def _batch_insert(self, records: List[ProcessRecord]) -> None:
        """批量插入数据库"""
        if not records:
            return

        self.db_client.replace_many_streaming(records)

    def _force_gc(self) -> None:
        """强制垃圾回收"""
        gc.collect()
        import sys
        if hasattr(sys, 'internals'):
            sys.internals.clear_memory()

    def close(self):
        """关闭连接"""
        self.db_client.close()


class LargeExcelProcessor:
    """超大文件处理器，分块读取Excel"""

    def __init__(self, db_client: DatabaseClient, chunk_size: int = 10000):
        """
        Args:
            db_client: 数据库客户端
            chunk_size: 每次读取的行数
        """
        self.db_client = db_client
        self.chunk_size = chunk_size

    def process_large_excel(self, file_path: str, reader_func) -> tuple[int, int]:
        """
        分块读取大Excel文件

        Args:
            file_path: Excel文件路径
            reader_func: 读取函数，支持传入start_row和chunk_size

        Returns:
            tuple: (总记录数, 有效记录数)
        """
        total_count = 0
        valid_count = 0
        start_row = 0

        while True:
            # 分块读取
            chunk_records = reader_func(file_path, start_row, self.chunk_size)
            if not chunk_records:
                break

            # 处理当前块
            chunk_total, chunk_valid = StreamProcessor(self.db_client).process_file_streaming(chunk_records)

            total_count += chunk_total
            valid_count += chunk_valid

            # 更新起始行
            start_row += self.chunk_size

            # 强制垃圾回收
            gc.collect()

        return total_count, valid_count
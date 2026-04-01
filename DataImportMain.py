# 工艺数据导入主程序
# 功能：Excel数据读取、清洗、验证、入库（流式处理，避免内存溢出）
# 处理流程：查找HT文件 → 读取Excel → 流式处理 → 批量入库 → 释放内存

from pathlib import Path

from DatabaseConfig import DB_CONFIG
from business_logic.logic import DataCleaner
from data_io.excel_handler import ExcelReader
from data_io.db_handler import DatabaseClient
from business_logic.StreamProcessor import StreamProcessor, LargeExcelProcessor


def _find_excel_files(folder: str) -> list[str]:  # 查找所有HT_开头的Excel文件
    base = Path(folder)
    out = []
    for p in base.iterdir():
        if p.name.startswith("~$"):
            continue
        if p.suffix.lower() in (".xlsx", ".xls") and p.name.startswith("HT_"):
            out.append(str(p))
    return sorted(out)


def main():  # 主函数：流式处理Excel数据
    # 配置参数
    BATCH_SIZE = 1000  # 每批入库的记录数
    MEMORY_THRESHOLD = 5000  # 内存阈值

    # 初始化配置和组件
    cleaner = DataCleaner()
    reader = ExcelReader(cleaner)
    db = DatabaseClient(DB_CONFIG, batch_size=BATCH_SIZE)

    # 创建流式处理器
    processor = StreamProcessor(db, batch_size=BATCH_SIZE, memory_threshold=MEMORY_THRESHOLD)

    total_records = 0
    total_ok = 0
    total_bad = 0

    print("开始流式导入处理...")
    print(f"批处理大小: {BATCH_SIZE}, 内存阈值: {MEMORY_THRESHOLD}")

    # 逐文件处理，避免内存堆积
    for file_path in _find_excel_files("."):
        print(f"\n处理文件: {file_path}")

        # 读取当前文件的所有记录（单文件数据量通常可控）
        file_records = reader.read(file_path)

        # 使用流式处理器处理当前文件的记录
        file_total, file_ok = processor.process_file_streaming(file_records)
        file_bad = file_total - file_ok

        total_records += file_total
        total_ok += file_ok
        total_bad += file_bad

        print(f"  本文件批次数: {file_total}, 合规: {file_ok}, 异常: {file_bad}")

        # 显式释放内存
        del file_records
        import gc
        gc.collect()

    print(f"\n导入完成: 总批次数={total_records}, 合规={total_ok}, 异常={total_bad}")

    # 关闭数据库连接
    db.close()


def main_for_large_files():
    """
    针对超大文件的处理器（使用分块读取）
    如果Excel文件特别大（如超过50万行），使用这个版本
    """
    BATCH_SIZE = 2000  # 大文件可以使用更大的批处理大小

    cleaner = DataCleaner()
    db = DatabaseClient(DB_CONFIG, batch_size=BATCH_SIZE)
    processor = LargeExcelProcessor(db, chunk_size=10000)  # 每次读取1万行

    total_records = 0
    total_ok = 0
    total_bad = 0

    print("开始大文件流式处理...")
    print(f"分块大小: {processor.chunk_size}, 批处理大小: {BATCH_SIZE}")

    for file_path in _find_excel_files("."):
        print(f"\n处理大文件: {file_path}")

        # 需要在ExcelReader中添加支持分块读取的方法
        # 这里使用伪代码表示，实际实现需要修改ExcelReader
        def chunked_read(file_path, start_row, chunk_size):
            # 这里应该是ExcelReader的分块读取实现
            # 为了演示，我们先用原方法
            return ExcelReader(cleaner).read(file_path)  # 实际需要修改

        file_total, file_ok = processor.process_large_excel(file_path, chunked_read)
        file_bad = file_total - file_ok

        total_records += file_total
        total_ok += file_ok
        total_bad += file_bad

        print(f"  本文件批次数: {file_total}, 合规: {file_ok}, 异常: {file_bad}")

    print(f"\n大文件导入完成: 总批次数={total_records}, 合规={total_ok}, 异常={total_bad}")
    db.close()


if __name__ == "__main__":
    main()
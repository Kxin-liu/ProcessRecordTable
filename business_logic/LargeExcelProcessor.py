from business_logic.DataCleaner import DataCleaner
from data_io.ExcelReader import ExcelReader
from data_io.DatabaseClient import DatabaseClient

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

        self.excelReader = ExcelReader(DataCleaner())

    def process_large_excel(self, file_path: str) -> tuple[int, int]:
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
        #         ExcelReader(cleaner).read(file_path) 
        while True:
            # 分块读取
            # chunk_records = self.excelReader.read(file_path, start_row, self.chunk_size)
            chunk_records = self.excelReader.read(file_path)
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
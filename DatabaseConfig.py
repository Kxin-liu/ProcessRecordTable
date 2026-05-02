# 数据库与Excel列配置
# DB_CONFIG: MySQL连接参数
# COLUMN_ALIASES: Excel列名智能匹配规则
# COLUMN_FALLBACK_INDEX: 列序兜底方案

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "root",
    "database": "factory_data",
    "charset": "utf8mb4",
}

COLUMN_ALIASES = {
    "batch_no": ["批号"],
    "product_no": ["物料品号", "物料品名"],
    "created_date": ["创建日期"],
    "equipment_name": ["设备名称"],
    "remark_info": ["备注信息"],
    "item_name": ["项目名称"],
    "item_result": ["项目记录结果"],
}

COLUMN_FALLBACK_INDEX = {
    "batch_no": 0,
    "product_no": 8,
    "equipment_name": 13,
    "remark_info": 16,
    "created_date": 19,
    "item_name": 18,
    "item_result": 22,
}

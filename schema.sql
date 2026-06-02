
CREATE DATABASE IF NOT EXISTS factory_data CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE factory_data;

CREATE TABLE IF NOT EXISTS ht_param_vector (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_no VARCHAR(128) NOT NULL,
    product_no VARCHAR(128) NOT NULL,
    created_date DATETIME NULL,
    equipment_name VARCHAR(255) NULL,
    remark_info TEXT NULL,
    source_file VARCHAR(255) NOT NULL,

    core_od DOUBLE NULL COMMENT '缆芯外径',
    jacket_od DOUBLE NULL COMMENT '护套外径',
    inner_die DOUBLE NULL COMMENT '挤出内模',
    outer_die DOUBLE NULL COMMENT '挤出外模',
    screw_speed DOUBLE NULL COMMENT '螺杆速度',
    screw_current DOUBLE NULL COMMENT '螺杆电流',
    prod_speed DOUBLE NULL COMMENT '生产速度',
    actual_prod_speed DOUBLE NULL COMMENT '实际生产速度',

    is_valid TINYINT(1) NOT NULL DEFAULT 1,
    invalid_reason_code VARCHAR(255) NULL,
    invalid_reason_text TEXT NULL,
    error_msg TEXT NULL,
    warning_msg TEXT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uk_batch_product_date (batch_no, product_no, created_date),
    KEY idx_product_no (product_no),
    KEY idx_is_valid (is_valid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ht_invalid_param_vector (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_no VARCHAR(128) NOT NULL,
    product_no VARCHAR(128) NOT NULL,
    created_date DATETIME NULL,
    equipment_name VARCHAR(255) NULL,
    remark_info TEXT NULL,
    source_file VARCHAR(255) NOT NULL,

    core_od DOUBLE NULL COMMENT '缆芯外径',
    jacket_od DOUBLE NULL COMMENT '护套外径',
    inner_die DOUBLE NULL COMMENT '挤出内模',
    outer_die DOUBLE NULL COMMENT '挤出外模',
    screw_speed DOUBLE NULL COMMENT '螺杆速度',
    screw_current DOUBLE NULL COMMENT '螺杆电流',
    prod_speed DOUBLE NULL COMMENT '生产速度',
    actual_prod_speed DOUBLE NULL COMMENT '实际生产速度',

    is_valid TINYINT(1) NOT NULL DEFAULT 0,
    invalid_reason_code VARCHAR(255) NULL,
    invalid_reason_text TEXT NULL,
    error_msg TEXT NULL,
    warning_msg TEXT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uk_invalid_batch_product_date (batch_no, product_no, created_date),
    KEY idx_invalid_product_no (product_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS quality_inspection_raw (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_file VARCHAR(255) NOT NULL,
    row_no INT NOT NULL,
    row_json LONGTEXT NOT NULL,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_quality_file_row (source_file, row_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS quality_inspection_param (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_no VARCHAR(128) NOT NULL,
    product_no VARCHAR(128) NOT NULL,
    item_name VARCHAR(255) NOT NULL,
    param_type VARCHAR(32) NOT NULL,
    lower_bound DOUBLE NULL,
    upper_bound DOUBLE NULL,
    qualitative_value TEXT NULL,
    actual_value DOUBLE NULL,
    is_ok TINYINT(1) NOT NULL,
    table_is_ok TINYINT(1) NULL,
    source_file VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    KEY idx_quality_batch (batch_no),
    KEY idx_quality_product (product_no),
    KEY idx_quality_type (param_type),
    KEY idx_quality_is_ok (is_ok)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

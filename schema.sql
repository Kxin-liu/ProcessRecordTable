
CREATE DATABASE IF NOT EXISTS factory_data CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE factory_data;

CREATE TABLE IF NOT EXISTS ht_param_vector (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_no VARCHAR(128) NOT NULL,
    product_no VARCHAR(128) NOT NULL,
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
    error_msg TEXT NULL,
    warning_msg TEXT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uk_batch_product (batch_no, product_no),
    KEY idx_product_no (product_no),
    KEY idx_is_valid (is_valid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE DATABASE IF NOT EXISTS source_db;
CREATE DATABASE IF NOT EXISTS target_db;

CREATE TABLE IF NOT EXISTS source_db.orders (
    order_id      INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product       VARCHAR(100) NOT NULL,
    amount        DECIMAL(10,2) NOT NULL,
    status        VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_updated_at (updated_at)
);

CREATE TABLE IF NOT EXISTS target_db.orders (
    order_id      INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product       VARCHAR(100) NOT NULL,
    amount        DECIMAL(10,2) NOT NULL,
    status        VARCHAR(20) NOT NULL,
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL,
    synced_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE USER IF NOT EXISTS 'pipeline_reader'@'localhost' IDENTIFIED BY '*******';
GRANT SELECT ON source_db.* TO 'pipeline_reader'@'localhost';

CREATE USER IF NOT EXISTS 'pipeline_writer'@'localhost' IDENTIFIED BY '*******';
GRANT SELECT, INSERT, UPDATE ON target_db.* TO 'pipeline_writer'@'localhost';

FLUSH PRIVILEGES;

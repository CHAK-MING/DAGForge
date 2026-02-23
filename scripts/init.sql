-- DAGForge MySQL initialization script
-- Creates database, user, and grants privileges

-- Create database
CREATE DATABASE IF NOT EXISTS dagforge CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create user if not exists
CREATE USER IF NOT EXISTS 'dagforge'@'%' IDENTIFIED BY 'dagforge';

-- Grant all privileges
GRANT ALL PRIVILEGES ON dagforge.* TO 'dagforge'@'%';

-- Apply changes
FLUSH PRIVILEGES;

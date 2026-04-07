-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS transactions_ks
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Use Keyspace
USE transactions_ks;

-- Create Table
CREATE TABLE IF NOT EXISTS daily_customer_totals (
    customer_id TEXT,
    transaction_date DATE,
    daily_total FLOAT,
    PRIMARY KEY (customer_id, transaction_date)
);
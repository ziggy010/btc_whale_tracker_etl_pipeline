-- File Format: Define CSV structure for Snowpipe ingestion
CREATE OR REPLACE FILE FORMAT csv_file_format
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ',';

-- Describe Storage Integration: Check S3 connection details
DESC INTEGRATION crypto_whale_init;

-- Stage: Link S3 transformed data to Snowflake
CREATE OR REPLACE STAGE crypto_stage
    STORAGE_INTEGRATION = crypto_whale_init
    URL = 's3://crypto-whale-tracker-etl/transformed_data/'
    FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');

-- Table: Store whale transaction data
CREATE OR REPLACE TABLE whale_data (
    txn_hash STRING,
    from_wallet STRING,
    to_wallet STRING,
    amount DOUBLE,
    datetime DATETIME
);

-- Preview: Check initial table contents
SELECT * FROM whale_data;

-- Clear: Reset table for fresh data
TRUNCATE whale_data;

-- Pipe: Auto-ingest S3 CSV into whale_data
CREATE OR REPLACE PIPE whale_data_pipe
    AUTO_INGEST = TRUE
AS
    COPY INTO whale_data
    FROM @crypto_stage;

-- Filter: Find big whale moves (>100 BTC)
SELECT * FROM whale_data 
WHERE amount > 100;

-- Describe Pipe: Verify pipe config
DESC PIPE whale_data_pipe;

-- Whale Accumulation: Total BTC received by wallet
SELECT 
    to_wallet,
    SUM(amount) AS total_amount,
    COUNT(*) AS total_txn
FROM whale_data
GROUP BY to_wallet
HAVING total_amount > 100
ORDER BY total_amount DESC;

-- Whale Sell-Offs: Total BTC sent by wallet
SELECT 
    from_wallet,
    SUM(amount) AS total_amount,
    COUNT(*) AS total_txn
FROM whale_data
GROUP BY from_wallet
HAVING total_amount > 50
ORDER BY total_amount DESC;

-- Specific Wallet: Track a known whale
SELECT * FROM whale_data
WHERE from_wallet = 'bc1q6qphr80zug3v37xf503a7atzfn3au2fz0dy9ek';
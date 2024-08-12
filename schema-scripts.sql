-- CREATE DATABASE stock;
-- Create 'stock_historical_data' table
CREATE TABLE stock_historical_data (
    ticker VARCHAR(10),
    time TIMESTAMP NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    PRIMARY KEY (ticker, time),
    FOREIGN KEY (ticker) REFERENCES list_company(ticker)
);
-- 
ALTER TABLE public."Minio_Source"
ADD COLUMN "date" DATE;
--
CREATE TABLE trading_dates AS
SELECT generate_series::date AS "date"
FROM generate_series('2023-01-01'::date, '2023-12-31'::date, '1 day'::interval)
WHERE extract(dow FROM generate_series) NOT IN (0, 6);
--
UPDATE public."Minio_Source"
SET "date" = NULL;
--
SELECT * from public."Minio_Source" 
where _ab_source_file_url = 'raw/stock_data/wiki_table.pq';

SELECT
    "ticker",
    "ask",
    "bid",
    "beta",
    "ebitda",
    "marketCap",        
    "bookValue",          -- Removed single quotes
    "totalDebt",          -- Removed single quotes
    "recommendationMean"  -- Removed single quotes
FROM {{ source('stock', 'Minio_Source') }}
WHERE _ab_source_file_url = 'raw/stock_data/quote_to_minio.pq'

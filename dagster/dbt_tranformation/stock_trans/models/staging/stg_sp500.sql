SELECT
    "CIK",
    REGEXP_REPLACE("Symbol", '\.', '-') AS ticker,
    "Headquarters_Location" AS headquarters_location,
    "GICS_Sector",          -- Ensure this column exists and is correctly named
    "GICS_Sub_Industry",
    CAST(SUBSTRING("Founded" FROM 1 FOR 4) AS INTEGER) AS year_founded,
    TO_DATE(SUBSTRING("Date_added" FROM 1 FOR 10), 'YYYY-MM-DD') AS date_first_added
FROM {{ source('stock', 'Minio_Source') }}
WHERE _ab_source_file_url = 'raw/stock_data/wiki_table.pq'

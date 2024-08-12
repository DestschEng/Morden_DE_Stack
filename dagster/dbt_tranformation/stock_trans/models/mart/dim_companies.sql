{{ config(materialized='table') }}

WITH stock_quotes AS (
    SELECT
        ticker,
        ask,
        bid,
        beta,
        ebitda,
        "marketCap",
        "bookValue",
        "totalDebt",
        "recommendationMean"
    FROM {{ ref('stg_quotes_data') }}
),

company_info AS (
    SELECT
        "CIK",
        ticker,
        headquarters_location,
        "GICS_Sector",
        "GICS_Sub_Industry",
        year_founded,
        date_first_added
    FROM {{ ref('stg_sp500') }}
)

SELECT
    s.ticker,
    s.ask,
    s.bid,
    s.beta,
    s.ebitda,
    s."marketCap",
    s."bookValue",
    s."totalDebt",
    s."recommendationMean",
    c."CIK",
    c."headquarters_location",
    c."GICS_Sector",
    c."GICS_Sub_Industry",
    c."year_founded",
    c.date_first_added
FROM stock_quotes s
LEFT JOIN company_info c
    ON s.ticker = c.ticker
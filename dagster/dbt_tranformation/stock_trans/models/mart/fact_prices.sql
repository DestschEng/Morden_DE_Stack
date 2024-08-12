{{ config(materialized='view') }}

SELECT
    price.ticker,
    price.open_price,
    price.low_price,
    price.high_price,
    price.close_price,
    price.adj_close_price,
    price.volume,
    c."GICS_Sector",
    c."GICS_Sub_Industry",
    c."year_founded",
    c."date_first_added",
    c."marketCap",
    c."CIK",
    c."headquarters_location"
FROM {{ ref('stg_daily_price') }} AS price
LEFT JOIN {{ ref('dim_companies') }} AS c
    ON price.ticker = c.ticker
LEFT JOIN {{ ref('dim_GICS') }} AS gics
    ON c."GICS_Sub_Industry" = gics."sub_industry"  
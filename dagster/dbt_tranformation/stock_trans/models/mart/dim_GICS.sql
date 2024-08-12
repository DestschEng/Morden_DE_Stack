{{ config(materialized='table') }}

WITH ranked_sectors AS (
    SELECT DISTINCT
        "GICS_Sector",
        "GICS_Sub_Industry",
        DENSE_RANK() OVER (ORDER BY "GICS_Sector", "GICS_Sub_Industry") AS industry_sector_id
    FROM {{ ref('dim_companies') }}
)

SELECT
    industry_sector_id,
    "GICS_Sector" AS sector,
    "GICS_Sub_Industry" AS sub_industry
FROM ranked_sectors
ORDER BY "GICS_Sector", "GICS_Sub_Industry"
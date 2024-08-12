SELECT
    ticker,
    "Open" AS open_price,
    "Low" AS low_price,
    "High" AS high_price,
    "Close" AS close_price,
    "Adj_Close" AS adj_close_price,
    "Volume" AS volume
FROM {{ source('stock', 'Minio_Source') }}

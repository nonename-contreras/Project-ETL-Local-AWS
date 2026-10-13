{{ config(
    materialized='incremental',
    unique_key=['crypto_id', 'updated_at'] 
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('staging', 'stg_crypto') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY crypto_id 
            ORDER BY updated_at DESC
        ) as rn
    FROM source_data
)

SELECT
    crypto_id,
    UPPER(ticker) AS ticker,
    crypto_name,
    CAST(current_price AS NUMERIC(10,2)) AS current_price,
    market_cap,
    total_volume,
    price_change_percentage_24h,
    updated_at,
    processed_at
FROM deduplicated
WHERE rn = 1

{% if is_incremental() %}
  AND processed_at > (SELECT MAX(processed_at) FROM {{ this }})
{% endif %}
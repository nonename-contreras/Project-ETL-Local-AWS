{{ config(
    materialized='incremental',
    unique_key=['repo_id', 'repo_updated_at']
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('staging', 'stg_github') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY repo_id 
            ORDER BY repo_updated_at DESC
        ) as rn
    FROM source_data
)

SELECT
    repo_id,
    repo_name,
    repo_full_name,
    stars,
    forks,
    primary_language,
    repo_created_at,
    repo_updated_at,
    processed_at
FROM deduplicated
WHERE rn = 1

{% if is_incremental() %}
  AND processed_at > (SELECT MAX(processed_at) FROM {{ this }})
{% endif %}
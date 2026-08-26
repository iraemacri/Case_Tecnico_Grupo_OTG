-- ============================================================
-- MODELO: dim_affiliates
-- CAMADA: Silver
-- DESCRICAO: Dimensao de afiliados limpa e deduplicada
-- TIPO: Incremental (merge por affiliate_id + player_id)
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key=['affiliate_id', 'player_id'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

WITH source AS (
    SELECT
        affiliate_id,
        player_id,
        country,
        clicks,
        registrations,
        ftd_count,
        cpa_value,
        _ingested_at,
        _dbt_processed_at,
        ROW_NUMBER() OVER (
            PARTITION BY affiliate_id, player_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ ref('stg_affiliates') }}
),

deduplicated AS (
    SELECT
        affiliate_id,
        player_id,
        country,
        clicks,
        registrations,
        ftd_count,
        cpa_value,
        CASE
            WHEN clicks > 0 THEN ROUND(SAFE_DIVIDE(registrations, clicks) * 100, 2)
            ELSE 0
        END AS conversion_rate,
        CASE
            WHEN registrations > 0 THEN ROUND(SAFE_DIVIDE(ftd_count, registrations) * 100, 2)
            ELSE 0
        END AS ftd_rate,
        ROUND(ftd_count * cpa_value, 2) AS total_cpa_value,
        _ingested_at,
        _dbt_processed_at,
        CURRENT_TIMESTAMP() AS _silver_updated_at
    FROM source
    WHERE rn = 1
)

SELECT * FROM deduplicated

{% if is_incremental() %}
WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}

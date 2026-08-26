-- ============================================================
-- MODELO: dim_players
-- CAMADA: Silver
-- DESCRICAO: Dimensao de jogadores limpa e deduplicada
-- TIPO: Incremental (merge por player_id)
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key='player_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

WITH source AS (
    SELECT
        player_id,
        email,
        city,
        created_at,
        _ingested_at,
        _dbt_processed_at,
        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ ref('stg_players') }}
),

deduplicated AS (
    SELECT
        player_id,
        email,
        city,
        created_at,
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

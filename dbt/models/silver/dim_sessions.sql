-- ============================================================
-- MODELO: dim_sessions
-- CAMADA: Silver
-- DESCRICAO: Dimensao de sessoes limpa e deduplicada
-- TIPO: Incremental (merge por session_id)
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key='session_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

WITH source AS (
    SELECT
        session_id,
        player_id,
        ip,
        device,
        session_timestamp,
        _ingested_at,
        _dbt_processed_at,
        ROW_NUMBER() OVER (
            PARTITION BY session_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ ref('stg_sessions') }}
),

deduplicated AS (
    SELECT
        session_id,
        player_id,
        ip,
        device,
        session_timestamp,
        DATE(session_timestamp) AS session_date,
        EXTRACT(HOUR FROM session_timestamp) AS session_hour,
        EXTRACT(DAYOFWEEK FROM session_timestamp) AS day_of_week,
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

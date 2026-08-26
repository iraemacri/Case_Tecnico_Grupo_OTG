-- ============================================================
-- MODELO: stg_sessions
-- CAMADA: Staging (Bronze)
-- DESCRICAO: Leitura dos dados brutos de sessoes
-- TIPO: View (leitura direta da tabela raw)
-- ============================================================

WITH source AS (
    SELECT
        session_id,
        player_id,
        ip,
        device,
        timestamp,
        _ingested_at
    FROM {{ source('bronze', 'raw_sessions') }}
),

renamed AS (
    SELECT
        session_id,
        player_id,
        TRIM(ip) AS ip,
        LOWER(TRIM(device)) AS device,
        TIMESTAMP(timestamp) AS session_timestamp,
        _ingested_at,
        CURRENT_TIMESTAMP() AS _dbt_processed_at
    FROM source
)

SELECT * FROM renamed

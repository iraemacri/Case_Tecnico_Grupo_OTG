-- ============================================================
-- MODELO: stg_players
-- CAMADA: Staging (Bronze)
-- DESCRICAO: Leitura dos dados brutos de players
-- TIPO: View (leitura direta da tabela raw)
-- ============================================================

WITH source AS (
    SELECT
        player_id,
        email,
        city,
        created_at,
        _ingested_at
    FROM {{ source('bronze', 'raw_players') }}
),

renamed AS (
    SELECT
        player_id,
        LOWER(TRIM(email)) AS email,
        TRIM(city) AS city,
        DATE(created_at) AS created_at,
        _ingested_at,
        CURRENT_TIMESTAMP() AS _dbt_processed_at
    FROM source
)

SELECT * FROM renamed

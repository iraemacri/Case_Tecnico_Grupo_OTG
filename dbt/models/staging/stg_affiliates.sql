-- ============================================================
-- MODELO: stg_affiliates
-- CAMADA: Staging (Bronze)
-- DESCRICAO: Leitura dos dados brutos de afiliados
-- TIPO: View (leitura direta da tabela raw)
-- ============================================================

WITH source AS (
    SELECT
        affiliate_id,
        player_id,
        country,
        clicks,
        registrations,
        ftd,
        cpa_value,
        _ingested_at
    FROM {{ source('bronze', 'raw_affiliates') }}
),

renamed AS (
    SELECT
        affiliate_id,
        player_id,
        UPPER(TRIM(country)) AS country,
        CAST(clicks AS INT64) AS clicks,
        CAST(registrations AS INT64) AS registrations,
        CAST(ftd AS INT64) AS ftd_count,
        CAST(cpa_value AS DECIMAL(10,2)) AS cpa_value,
        _ingested_at,
        CURRENT_TIMESTAMP() AS _dbt_processed_at
    FROM source
)

SELECT * FROM renamed

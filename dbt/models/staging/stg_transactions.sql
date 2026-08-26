-- ============================================================
-- MODELO: stg_transactions
-- CAMADA: Staging (Bronze)
-- DESCRICAO: Leitura dos dados brutos de transacoes
-- TIPO: View (leitura direta da tabela raw)
-- ============================================================

WITH source AS (
    SELECT
        transaction_id,
        player_id,
        type,
        amount,
        timestamp,
        _ingested_at
    FROM {{ source('bronze', 'raw_transactions') }}
),

renamed AS (
    SELECT
        transaction_id,
        player_id,
        LOWER(TRIM(type)) AS transaction_type,
        CAST(amount AS DECIMAL(10,2)) AS amount,
        TIMESTAMP(timestamp) AS transaction_timestamp,
        _ingested_at,
        CURRENT_TIMESTAMP() AS _dbt_processed_at
    FROM source
)

SELECT * FROM renamed

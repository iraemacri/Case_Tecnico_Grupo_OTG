-- ============================================================
-- MODELO: fct_transactions
-- CAMADA: Silver
-- DESCRICAO: Fato de transacoes financeiras limpas
-- TIPO: Incremental (merge por transaction_id)
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

WITH source AS (
    SELECT
        transaction_id,
        player_id,
        transaction_type,
        amount,
        transaction_timestamp,
        _ingested_at,
        _dbt_processed_at,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ ref('stg_transactions') }}
),

deduplicated AS (
    SELECT
        transaction_id,
        player_id,
        transaction_type,
        amount,
        transaction_timestamp,
        DATE(transaction_timestamp) AS transaction_date,
        EXTRACT(HOUR FROM transaction_timestamp) AS transaction_hour,
        CASE
            WHEN transaction_type = 'deposit' THEN amount
            ELSE 0
        END AS deposit_amount,
        CASE
            WHEN transaction_type = 'withdraw' THEN amount
            ELSE 0
        END AS withdraw_amount,
        CASE
            WHEN transaction_type = 'bet' THEN amount
            ELSE 0
        END AS bet_amount,
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

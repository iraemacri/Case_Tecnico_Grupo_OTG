-- ============================================================
-- MODELO: fct_fraud_signals
-- CAMADA: Gold
-- DESCRICAO: Sinais de fraude detectados nos dados
-- TIPO: Table (recalculado diariamente)
-- SINAIS IMPLEMENTADOS:
--   1. Multi-accounting (IP compartilhado)
--   2. Deposit without bet (deposito sem aposta)
--   3. High-risk device patterns
-- ============================================================

WITH player_sessions AS (
    SELECT
        player_id,
        ip,
        device,
        session_timestamp,
        session_date
    FROM {{ ref('dim_sessions') }}
),

player_transactions AS (
    SELECT
        player_id,
        transaction_type,
        amount,
        transaction_timestamp,
        transaction_date
    FROM {{ ref('fct_transactions') }}
),

-- ============================================================
-- SINAL 1: MULTI-ACCOUNTING
-- Mesmo IP utilizado por multiplos jogadores
-- ============================================================
ip_sharing AS (
    SELECT
        ip,
        COUNT(DISTINCT player_id) AS distinct_players,
        ARRAY_AGG(DISTINCT player_id) AS player_list,
        MIN(session_timestamp) AS first_seen,
        MAX(session_timestamp) AS last_seen
    FROM player_sessions
    WHERE ip IS NOT NULL
    GROUP BY ip
    HAVING COUNT(DISTINCT player_id) > 1
),

-- ============================================================
-- SINAL 2: DEPOSIT WITHOUT BET
-- Jogadores que depositam mas nunca apostam
-- ============================================================
deposit_without_bet AS (
    SELECT
        t.player_id,
        SUM(CASE WHEN t.transaction_type = 'deposit' THEN t.amount ELSE 0 END) AS total_deposits,
        SUM(CASE WHEN t.transaction_type = 'bet' THEN t.amount ELSE 0 END) AS total_bets,
        SUM(CASE WHEN t.transaction_type = 'withdraw' THEN t.amount ELSE 0 END) AS total_withdrawals,
        COUNT(CASE WHEN t.transaction_type = 'deposit' THEN 1 END) AS deposit_count,
        COUNT(CASE WHEN t.transaction_type = 'bet' THEN 1 END) AS bet_count
    FROM player_transactions t
    GROUP BY t.player_id
    HAVING
        SUM(CASE WHEN t.transaction_type = 'deposit' THEN t.amount ELSE 0 END) > 0
        AND SUM(CASE WHEN t.transaction_type = 'bet' THEN t.amount ELSE 0 END) = 0
),

-- ============================================================
-- SINAL 3: HIGH-RISK DEVICE PATTERNS
-- Multiplos jogadores no mesmo dispositivo
-- ============================================================
device_sharing AS (
    SELECT
        device,
        COUNT(DISTINCT player_id) AS distinct_players,
        ARRAY_AGG(DISTINCT player_id) AS player_list
    FROM player_sessions
    WHERE device IS NOT NULL
    GROUP BY device
    HAVING COUNT(DISTINCT player_id) > 5
),

-- ============================================================
-- SINAL 4: RAPID DEPOSIT-WITHDRAW CYCLE
-- Deposito e saque em menos de 1 hora
-- ============================================================
rapid_cycles AS (
    SELECT
        d.player_id,
        d.transaction_timestamp AS deposit_time,
        w.transaction_timestamp AS withdraw_time,
        d.amount AS deposit_amount,
        w.amount AS withdraw_amount,
        TIMESTAMP_DIFF(w.transaction_timestamp, d.transaction_timestamp, MINUTE) AS minutes_between
    FROM player_transactions d
    INNER JOIN player_transactions w
        ON d.player_id = w.player_id
    WHERE
        d.transaction_type = 'deposit'
        AND w.transaction_type = 'withdraw'
        AND w.transaction_timestamp > d.transaction_timestamp
        AND TIMESTAMP_DIFF(w.transaction_timestamp, d.transaction_timestamp, MINUTE) < 60
),

-- ============================================================
-- CONSOLIDACAO DOS SINAIS
-- ============================================================
fraud_signals AS (
    -- Sinal 1: Multi-accounting
    SELECT
        p.player_id,
        'MULTI_ACCOUNTING' AS signal_type,
        CONCAT('IP compartilhado entre ', CAST(s.distinct_players AS STRING), ' jogadores') AS signal_description,
        'HIGH' AS risk_level,
        s.first_seen AS detected_at,
        ARRAY_TO_STRING(s.player_list, ', ') AS related_players
    FROM ip_sharing s
    CROSS JOIN UNNEST(s.player_list) AS p(player_id)

    UNION ALL

    -- Sinal 2: Deposit without bet
    SELECT
        player_id,
        'DEPOSIT_WITHOUT_BET' AS signal_type,
        CONCAT('Depositos totais: $', CAST(total_deposits AS STRING), ' sem nenhuma aposta') AS signal_description,
        'MEDIUM' AS risk_level,
        CURRENT_TIMESTAMP() AS detected_at,
        NULL AS related_players
    FROM deposit_without_bet

    UNION ALL

    -- Sinal 3: Device sharing
    SELECT
        p.player_id,
        'DEVICE_SHARING' AS signal_type,
        CONCAT('Device ', device, ' usado por ', CAST(distinct_players AS STRING), ' jogadores') AS signal_description,
        'MEDIUM' AS risk_level,
        CURRENT_TIMESTAMP() AS detected_at,
        ARRAY_TO_STRING(player_list, ', ') AS related_players
    FROM device_sharing s
    CROSS JOIN UNNEST(s.player_list) AS p(player_id)

    UNION ALL

    -- Sinal 4: Rapid deposit-withdraw cycle
    SELECT
        player_id,
        'RAPID_DEPOSIT_WITHDRAW' AS signal_type,
        CONCAT('Deposito de $', CAST(deposit_amount AS STRING), ' e saque de $', CAST(withdraw_amount AS STRING), ' em ', CAST(minutes_between AS STRING), ' minutos') AS signal_description,
        'HIGH' AS risk_level,
        deposit_time AS detected_at,
        NULL AS related_players
    FROM rapid_cycles
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['player_id', 'signal_type', 'detected_at']) }} AS fraud_signal_id,
    player_id,
    signal_type,
    signal_description,
    risk_level,
    detected_at,
    related_players,
    CURRENT_TIMESTAMP() AS _gold_processed_at
FROM fraud_signals

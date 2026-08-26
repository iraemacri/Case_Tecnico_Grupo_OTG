-- ============================================================
-- MODELO: fct_financial_summary
-- CAMADA: Gold
-- DESCRICAO: Resumo financeiro para dashboards
-- TIPO: Table (recalculado diariamente)
-- METRICAS: Depósitos, saques, GGR, NGR, LTV, ARPU
-- ============================================================

WITH player_transactions AS (
    SELECT
        player_id,
        transaction_type,
        amount,
        transaction_date,
        transaction_timestamp
    FROM {{ ref('fct_transactions') }}
),

player_info AS (
    SELECT
        player_id,
        city,
        created_at
    FROM {{ ref('dim_players') }}
),

-- ============================================================
-- AGREGACAO POR PLAYER
-- ============================================================
player_financials AS (
    SELECT
        t.player_id,
        COUNT(DISTINCT t.transaction_date) AS active_days,
        MIN(t.transaction_timestamp) AS first_transaction,
        MAX(t.transaction_timestamp) AS last_transaction,

        -- Depositos
        SUM(CASE WHEN t.transaction_type = 'deposit' THEN t.amount ELSE 0 END) AS total_deposits,
        COUNT(CASE WHEN t.transaction_type = 'deposit' THEN 1 END) AS deposit_count,
        AVG(CASE WHEN t.transaction_type = 'deposit' THEN t.amount ELSE NULL END) AS avg_deposit,

        -- Saques
        SUM(CASE WHEN t.transaction_type = 'withdraw' THEN t.amount ELSE 0 END) AS total_withdrawals,
        COUNT(CASE WHEN t.transaction_type = 'withdraw' THEN 1 END) AS withdrawal_count,
        AVG(CASE WHEN t.transaction_type = 'withdraw' THEN t.amount ELSE NULL END) AS avg_withdrawal,

        -- Apostas
        SUM(CASE WHEN t.transaction_type = 'bet' THEN t.amount ELSE 0 END) AS total_bets,
        COUNT(CASE WHEN t.transaction_type = 'bet' THEN 1 END) AS bet_count,
        AVG(CASE WHEN t.transaction_type = 'bet' THEN t.amount ELSE NULL END) AS avg_bet

    FROM player_transactions t
    GROUP BY t.player_id
),

-- ============================================================
-- CALCULO DE METRICAS FINANCEIRAS
-- ============================================================
financial_metrics AS (
    SELECT
        pf.player_id,
        p.city,
        p.created_at,
        pf.active_days,
        pf.first_transaction,
        pf.last_transaction,

        -- Totais
        pf.total_deposits,
        pf.deposit_count,
        pf.avg_deposit,
        pf.total_withdrawals,
        pf.withdrawal_count,
        pf.avg_withdrawal,
        pf.total_bets,
        pf.bet_count,
        pf.avg_bet,

        -- GGR (Gross Gaming Revenue) = Apostas - Saques
        pf.total_bets - pf.total_withdrawals AS ggr,

        -- Net Deposit = Depositos - Saques
        pf.total_deposits - pf.total_withdrawals AS net_deposit,

        -- LTV estimado (depositos totais)
        pf.total_deposits AS estimated_ltv,

        -- Dias desde primeiro ate ultimo transacao
        DATE_DIFF(
            DATE(pf.last_transaction),
            DATE(pf.first_transaction),
            DAY
        ) AS customer_lifespan_days

    FROM player_financials pf
    INNER JOIN player_info p
        ON pf.player_id = p.player_id
),

-- ============================================================
-- METRICAS POR CIDADE
-- ============================================================
city_metrics AS (
    SELECT
        city,
        COUNT(DISTINCT player_id) AS total_players,
        SUM(total_deposits) AS city_total_deposits,
        SUM(total_withdrawals) AS city_total_withdrawals,
        SUM(total_bets) AS city_total_bets,
        SUM(ggr) AS city_ggr,
        AVG(total_deposits) AS avg_deposits_per_player,
        AVG(total_bets) AS avg_bets_per_player
    FROM financial_metrics
    GROUP BY city
),

-- ============================================================
-- METRICAS GLOBAIS (ARPU)
-- ============================================================
global_metrics AS (
    SELECT
        COUNT(DISTINCT player_id) AS total_active_players,
        SUM(total_deposits) AS global_total_deposits,
        SUM(total_withdrawals) AS global_total_withdrawals,
        SUM(total_bets) AS global_total_bets,
        SUM(ggr) AS global_ggr,
        SAFE_DIVIDE(SUM(total_deposits), COUNT(DISTINCT player_id)) AS arpu_deposits,
        SAFE_DIVIDE(SUM(total_bets), COUNT(DISTINCT player_id)) AS arpu_bets
    FROM financial_metrics
)

-- ============================================================
-- OUTPUT FINAL
-- ============================================================
SELECT
    fm.player_id,
    fm.city,
    fm.created_at,
    fm.active_days,
    fm.first_transaction,
    fm.last_transaction,

    -- Totais
    fm.total_deposits,
    fm.deposit_count,
    fm.avg_deposit,
    fm.total_withdrawals,
    fm.withdrawal_count,
    fm.avg_withdrawal,
    fm.total_bets,
    fm.bet_count,
    fm.avg_bet,

    -- Metricas financeiras
    fm.ggr,
    fm.net_deposit,
    fm.estimated_ltv,
    fm.customer_lifespan_days,

    -- Metricas da cidade
    cm.total_players AS city_total_players,
    cm.city_total_deposits,
    cm.city_total_withdrawals,
    cm.city_total_bets,
    cm.city_ggr,
    cm.avg_deposits_per_player AS city_avg_deposits,
    cm.avg_bets_per_player AS city_avg_bets,

    -- Metricas globais (ARPU)
    gm.total_active_players,
    gm.global_total_deposits,
    gm.global_total_withdrawals,
    gm.global_total_bets,
    gm.global_ggr,
    gm.arpu_deposits,
    gm.arpu_bets,

    -- Classificacao de risco
    CASE
        WHEN fm.total_deposits > 0 AND fm.total_bets = 0 THEN 'HIGH_RISK_NO_BETS'
        WHEN fm.total_withdrawals > fm.total_deposits THEN 'HIGH_RISK_OVER_WITHDRAWAL'
        WHEN fm.active_days <= 2 AND fm.total_deposits > 1000 THEN 'MEDIUM_RISK_NEW_HIGH_VALUE'
        ELSE 'NORMAL'
    END AS risk_classification,

    CURRENT_TIMESTAMP() AS _gold_processed_at

FROM financial_metrics fm
LEFT JOIN city_metrics cm
    ON fm.city = cm.city
CROSS JOIN global_metrics gm

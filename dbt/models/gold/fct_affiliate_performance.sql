-- ============================================================
-- MODELO: fct_affiliate_performance
-- CAMADA: Gold
-- DESCRICAO: Metricas de performance de afiliados
-- TIPO: Table (recalculado diariamente)
-- METRICAS: CPA, FTD, taxa de conversao, ROI
-- ============================================================

WITH affiliate_data AS (
    SELECT
        affiliate_id,
        player_id,
        country,
        clicks,
        registrations,
        ftd_count,
        cpa_value,
        conversion_rate,
        ftd_rate,
        total_cpa_value
    FROM {{ ref('dim_affiliates') }}
),

player_transactions AS (
    SELECT
        player_id,
        transaction_type,
        amount,
        transaction_date
    FROM {{ ref('fct_transactions') }}
),

-- ============================================================
-- AGREGACAO POR AFILIADO
-- ============================================================
affiliate_metrics AS (
    SELECT
        a.affiliate_id,
        a.country,

        -- Volume
        COUNT(DISTINCT a.player_id) AS total_players,
        SUM(a.clicks) AS total_clicks,
        SUM(a.registrations) AS total_registrations,
        SUM(a.ftd_count) AS total_ftd,

        -- Valores
        SUM(a.total_cpa_value) AS total_cpa_revenue,
        AVG(a.cpa_value) AS avg_cpa_value,

        -- Taxas
        CASE
            WHEN SUM(a.clicks) > 0
            THEN ROUND(SAFE_DIVIDE(SUM(a.registrations), SUM(a.clicks)) * 100, 2)
            ELSE 0
        END AS overall_conversion_rate,

        CASE
            WHEN SUM(a.registrations) > 0
            THEN ROUND(SAFE_DIVIDE(SUM(a.ftd_count), SUM(a.registrations)) * 100, 2)
            ELSE 0
        END AS overall_ftd_rate

    FROM affiliate_data a
    GROUP BY a.affiliate_id, a.country
),

-- ============================================================
-- CALCULO DE RECEITA GERADA PELOS PLAYERS
-- ============================================================
player_revenue AS (
    SELECT
        t.player_id,
        SUM(CASE WHEN t.transaction_type = 'deposit' THEN t.amount ELSE 0 END) AS total_deposits,
        SUM(CASE WHEN t.transaction_type = 'bet' THEN t.amount ELSE 0 END) AS total_bets,
        SUM(CASE WHEN t.transaction_type = 'withdraw' THEN t.amount ELSE 0 END) AS total_withdrawals,
        SUM(CASE WHEN t.transaction_type = 'bet' THEN t.amount ELSE 0 END)
            - SUM(CASE WHEN t.transaction_type = 'withdraw' THEN t.amount ELSE 0 END) AS net_revenue
    FROM player_transactions t
    GROUP BY t.player_id
),

-- ============================================================
-- JOIN COM RECEITA
-- ============================================================
affiliate_with_revenue AS (
    SELECT
        a.affiliate_id,
        a.country,
        a.total_players,
        a.total_clicks,
        a.total_registrations,
        a.total_ftd,
        a.total_cpa_revenue,
        a.avg_cpa_value,
        a.overall_conversion_rate,
        a.overall_ftd_rate,
        COALESCE(SUM(r.total_deposits), 0) AS player_total_deposits,
        COALESCE(SUM(r.total_bets), 0) AS player_total_bets,
        COALESCE(SUM(r.total_withdrawals), 0) AS player_total_withdrawals,
        COALESCE(SUM(r.net_revenue), 0) AS player_net_revenue,

        -- ROI do Afiliado
        CASE
            WHEN a.total_cpa_revenue > 0
            THEN ROUND(SAFE_DIVIDE(
                COALESCE(SUM(r.net_revenue), 0) - a.total_cpa_revenue,
                a.total_cpa_revenue
            ) * 100, 2)
            ELSE 0
        END AS affiliate_roi

    FROM affiliate_metrics a
    LEFT JOIN affiliate_data ad
        ON a.affiliate_id = ad.affiliate_id
        AND a.country = ad.country
    LEFT JOIN player_revenue r
        ON ad.player_id = r.player_id
    GROUP BY
        a.affiliate_id,
        a.country,
        a.total_players,
        a.total_clicks,
        a.total_registrations,
        a.total_ftd,
        a.total_cpa_revenue,
        a.avg_cpa_value,
        a.overall_conversion_rate,
        a.overall_ftd_rate
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['affiliate_id', 'country']) }} AS affiliate_performance_id,
    affiliate_id,
    country,
    total_players,
    total_clicks,
    total_registrations,
    total_ftd,
    total_cpa_revenue,
    avg_cpa_value,
    overall_conversion_rate,
    overall_ftd_rate,
    player_total_deposits,
    player_total_bets,
    player_total_withdrawals,
    player_net_revenue,
    affiliate_roi,
    CURRENT_TIMESTAMP() AS _gold_processed_at
FROM affiliate_with_revenue

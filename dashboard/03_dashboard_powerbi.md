# Dashboard PowerBI - iGaming Analytics

## Visão Geral

Dashboard com 3 páginas focadas em:
1. **Fraud Overview** — Visão geral de sinais de fraude
2. **Affiliate Metrics** — Performance de afiliados
3. **Financial Signals** — Sinais financeiros e KPIs

---

## Conexão com Dados

### Fonte: BigQuery

```
Servidor: igaming-project
Database: igaming_gold
Tabelas:
  - fct_fraud_signals
  - fct_affiliate_performance
  - fct_financial_summary
```

### Power Query (M) - Conexão

```m
let
    Source = GoogleBigQuery.Database([Project="igaming-project"]),
    igaming_gold = Source{[Name="igaming_gold"]}[Data],
    fct_fraud_signals = igaming_gold{[Name="fct_fraud_signals"]}[Data],
    fct_affiliate_performance = igaming_gold{[Name="fct_affiliate_performance"]}[Data],
    fct_financial_summary = igaming_gold{[Name="fct_financial_summary"]}[Data]
in
    fct_fraud_signals
```

---

## Wireframe - Layout Geral

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  [LOGO]  DASHBOARD IGAMING ANALYTICS                    [Filtros] [Data]  │
├─────────────────────────────────────────────────────────────────────────────┤
│  [TAB: Fraud Overview]  [TAB: Affiliate Metrics]  [TAB: Financial Signals]│
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐              │
│  │  TOTAL CONTAS   │ │  SINAIS         │ │  RISCO          │              │
│  │  600            │ │  2.847          │ │  ALTO: 12%      │              │
│  │  (KPI Card)     │ │  (KPI Card)     │ │  (KPI Card)     │              │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘              │
│                                                                             │
│  ┌───────────────────────────────┐ ┌───────────────────────────────┐      │
│  │                               │ │                               │      │
│  │   GRÁFICO: Sinais de Fraude   │ │   GRÁFICO: Risco por Cidade  │      │
│  │   (Barras por tipo)           │ │   (Mapa/Heatmap)             │      │
│  │                               │ │                               │      │
│  └───────────────────────────────┘ └───────────────────────────────┘      │
│                                                                             │
│  ┌───────────────────────────────┐ ┌───────────────────────────────┐      │
│  │                               │ │                               │      │
│  │   GRÁFICO: Timeline Fraude    │ │   GRÁFICO: Top Players        │      │
│  │   (Linha temporal)            │ │   (Tabela com risco)          │      │
│  │                               │ │                               │      │
│  └───────────────────────────────┘ └───────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Página 1: Fraud Overview

### KPIs (Cards no topo)

| KPI | Métrica | Fórmula DAX |
|-----|---------|-------------|
| Total Contas Analisadas | Distinct players | `DISTINCTCOUNT(fct_fraud_signals[player_id])` |
| Total Sinais de Fraude | Total signals | `COUNTROWS(fct_fraud_signals)` |
| % Contas com Risco | % de contas sinalizadas | `DIVIDE([Total Contas com Sinal], [Total Contas Geral])` |

### Gráfico 1: Sinais de Fraude por Tipo (Barras)

**Tipo:** Clustered Bar Chart
**Eixo X:** Quantidade de sinais
**Eixo Y:** Tipo de sinal (MULTI_ACCOUNTING, DEPOSIT_WITHOUT_BET, etc.)
**Legenda:** Nível de risco (HIGH, MEDIUM, LOW)

```dax
// Medida: Contagem por tipo de sinal
Contagem Sinais = 
CALCULATE(
    COUNTROWS(fct_fraud_signals),
    ALLSELECTED(fct_fraud_signals)
)
```

### Gráfico 2: Sinais de Fraude por Cidade (Mapa)

**Tipo:** Map
**Localização:** City (do dim_players)
**Tamanho:** Contagem de sinais
**Cor:** Nível de risco

```dax
// Medita: Sinais por cidade
Sinais por Cidade = 
CALCULATE(
    COUNTROWS(fct_fraud_signals),
    ALLEXCEPT(fct_fraud_signals, dim_players[city])
)
```

### Gráfico 3: Timeline de Sinais (Linha)

**Tipo:** Line Chart
**Eixo X:** Data (detected_at)
**Eixo Y:** Quantidade de sinais
**Legenda:** Tipo de sinal

```dax
// Medida: Sinais por dia
Sinais por Dia = 
CALCULATE(
    COUNTROWS(fct_fraud_signals),
    DATESBETWEEN(fct_fraud_signals[detected_at], MIN(fct_fraud_signals[detected_at]), MAX(fct_fraud_signals[detected_at]))
)
```

### Gráfico 4: Top Players com Sinais (Tabela)

**Tipo:** Table
**Colunas:** player_id, signal_type, risk_level, signal_description

```dax
// Medida: Detalhes do sinal
Detalhes Sinal = 
CONCATENATEX(
    TOPN(10, fct_fraud_signals, fct_fraud_signals[detected_at], DESC),
    fct_fraud_signals[player_id] & " - " & fct_fraud_signals[signal_type],
    " | "
)
```

---

## Página 2: Affiliate Metrics

### KPIs (Cards no topo)

| KPI | Métrica | Fórmula DAX |
|-----|---------|-------------|
| Total Afiliados | Distinct affiliates | `DISTINCTCOUNT(fct_affiliate_performance[affiliate_id])` |
| Total FTDs | First Time Deposits | `SUM(fct_affiliate_performance[total_ftd])` |
| Conversão Média | Avg conversion rate | `AVERAGE(fct_affiliate_performance[overall_conversion_rate])` |
| ROI Médio | Avg ROI | `AVERAGE(fct_affiliate_performance[affiliate_roi])` |

### Gráfico 1: Performance por Afiliado (Barras)

**Tipo:** Clustered Bar Chart
**Eixo X:** affiliate_id
**Eixo Y:** total_ftd, total_cpa_revenue
**Ordenação:** Decrescente por FTD

```dax
// Medita: FTD por afiliado
FTD por Afiliado = 
SUM(fct_affiliate_performance[total_ftd])
```

### Gráfico 2: Conversão por País (Donut)

**Tipo:** Donut Chart
**Legenda:** country
**Valor:** total_ftd

```dax
// Medita: FTD por país
FTD por País = 
CALCULATE(
    SUM(fct_affiliate_performance[total_ftd]),
    ALLEXCEPT(fct_affiliate_performance, fct_affiliate_performance[country])
)
```

### Gráfico 3: ROI vs FTD (Scatter)

**Tipo:** Scatter Chart
**Eixo X:** total_ftd
**Eixo Y:** affiliate_roi
**Tamanho:** total_cpa_revenue
**Legenda:** country

### Gráfico 4: Top 10 Afiliados (Tabela)

**Tipo:** Table
**Colunas:** affiliate_id, country, total_ftd, total_cpa_revenue, overall_conversion_rate, affiliate_roi

---

## Página 3: Financial Signals

### KPIs (Cards no topo)

| KPI | Métrica | Fórmula DAX |
|-----|---------|-------------|
| Depósitos Totais | Total deposits | `SUM(fct_financial_summary[total_deposits])` |
| GGR Total | Gross Gaming Revenue | `SUM(fct_financial_summary[ggr])` |
| ARPU | Avg Revenue Per User | `AVERAGE(fct_financial_summary[total_deposits])` |
| LTV Médio | Avg Lifetime Value | `AVERAGE(fct_financial_summary[estimated_ltv])` |

### Gráfico 1: GGR por Cidade (Barras)

**Tipo:** Clustered Bar Chart
**Eixo X:** city
**Eixo Y:** ggr
**Ordenação:** Decrescente

```dax
// Medita: GGR por cidade
GGR por Cidade = 
SUM(fct_financial_summary[ggr])
```

### Gráfico 2: Classificação de Risco (Donut)

**Tipo:** Donut Chart
**Legenda:** risk_classification
**Valor:** Contagem de jogadores

```dax
// Medita: Contagem por classificação
Contagem Risco = 
COUNTROWS(fct_financial_summary)
```

### Gráfico 3: Depósitos vs Apostas (Linha)

**Tipo:** Line Chart
**Eixo X:** city
**Eixo Y:** total_deposits, total_bets
**Legenda:** Métrica

### Gráfico 4: Top Players de Risco (Tabela)

**Tipo:** Table
**Colunas:** player_id, city, total_deposits, total_bets, ggr, risk_classification
**Filtro:** Apenas risco != NORMAL

---

## Data Model (Relacionamentos)

```
┌─────────────────┐
│  dim_players    │
│  (player_id PK) │
└────────┬────────┘
         │
    1:N  │
         │
┌────────┴────────┐
│ dim_sessions    │──── 1:N ──── fct_fraud_signals
│ (session_id PK) │
└────────┬────────┘
         │
    1:N  │
         │
┌────────┴────────┐
│fct_transactions │
│(transaction_id) │
└────────┬────────┘
         │
    N:1  │
         │
┌────────┴────────┐
│dim_affiliates   │──── 1:N ──── fct_affiliate_performance
│(affiliate_id)   │
└─────────────────┘

fct_financial_summary ← (agregado por player_id)
```

---

## Filtros Globais

Adicionar nos 3 painéis:

| Filtro | Campo | Tipo |
|--------|-------|------|
| Período | detected_at / transaction_date | Date Range |
| Cidade | city | Dropdown |
| País | country | Dropdown |
| Dispositivo | device | Dropdown |
| Nível de Risco | risk_level / risk_classification | Dropdown |

---

## Formatação Visual

| Elemento | Especificação |
|----------|--------------|
| Fonte | Segoe UI, 10pt |
| Cores | Azul (#1E3A5F), Verde (#27AE60), Vermelho (#E74C3C), Amarelo (#F39C12) |
| Fundo | Branco (#FFFFFF) |
| Cards | Fundo cinza claro (#F5F5F5) |
| Bordas | 1px sólida (#E0E0E0) |
| Títulos | Bold, 14pt |
| KPIs | Bold, 24pt |

---

## Passo a Passo para Criar no Power BI

1. **Abrir Power BI Desktop**
2. **Dados → Get Data → Google BigQuery**
3. **Conectar** ao projeto `igaming-project`
4. **Selecionar** tabelas da camada Gold
5. **Modelar** relacionamentos (conforme Data Model acima)
6. **Criar** medidas DAX (copiar código acima)
7. **Criar** 3 páginas (tabs)
8. **Adicionar** visuais conforme wireframe
9. **Aplicar** formatação visual
10. **Publicar** no Power BI Service

---

## Arquivo de Entrega

Salvar como: `dashboard/igaming_analytics.pbix`

Incluir na pasta `dashboard/`:
- `igaming_analytics.pbix` (arquivo do dashboard)
- `prints/` (prints das 3 páginas para o README)

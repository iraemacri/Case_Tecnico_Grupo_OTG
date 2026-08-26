# Arquitetura Medallion - Pipeline de Dados

## Visão Geral

Pipeline completa utilizando **Medallion Architecture** (Bronze → Silver → Gold) para ingestão, tratamento e análise de dados de plataforma de iGaming com foco em detecção de fraude e performance de afiliados.

---

## Diagrama da Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS (Raw)                               │
│  players.json │ sessions.json │ transactions.csv │ affiliate_cpa_ftd.csv   │
└───────┬───────────────┬───────────────┬───────────────────┬─────────────────┘
        │               │               │                   │
        ▼               ▼               ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     ORQUESTRAÇÃO - Apache Airflow                           │
│   DAG: dag_ingestao_igaming                                                │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐              │
│   │ Ingest   │→ │ Ingest   │→ │ Ingest   │→ │   Ingest     │              │
│   │ players  │  │ sessions │  │ transac. │  │  affiliates  │              │
│   └──────────┘  └──────────┘  └──────────┘  └──────────────┘              │
│                              │                                              │
│                              ▼                                              │
│                     Observabilidade (Logging, Alerts, Metrics)              │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CAMADA BRONZE (Raw Ingestion)                                             │
│  BigQuery Dataset: igaming_bronze                                           │
│                                                                             │
│  ┌─────────────────┐ ┌──────────────────┐ ┌───────────────────┐            │
│  │ raw_players     │ │ raw_sessions     │ │ raw_transactions  │            │
│  │ (JSON → TABLE)  │ │ (JSON → TABLE)   │ │ (CSV → TABLE)     │            │
│  └─────────────────┘ └──────────────────┘ └───────────────────┘            │
│  ┌──────────────────────┐                                                 │
│  │ raw_affiliates       │                                                 │
│  │ (CSV → TABLE)        │                                                 │
│  └──────────────────────┘                                                 │
│                                                                             │
│  • Dados 100% originais, sem transformação                                 │
│  • Coluna _ingested_at adicionada para controle                            │
│  • Schema preservado conforme fonte                                        │
│  • Particionado por data de ingestão                                       │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CAMADA SILVER (Cleaned & Conformed)                                       │
│  BigQuery Dataset: igaming_silver                                           │
│                                                                             │
│  ┌─────────────────┐ ┌──────────────────┐ ┌───────────────────┐            │
│  │ dim_players     │ │ dim_sessions     │ │ fct_transactions  │            │
│  └─────────────────┘ └──────────────────┘ └───────────────────┘            │
│  ┌──────────────────────┐ ┌─────────────────────────────┐                 │
│  │ dim_affiliates       │ │ bridge_player_sessions      │                 │
│  └──────────────────────┘ └─────────────────────────────┘                 │
│                                                                             │
│  • Deduplicação por chave primária                                         │
│  • Padronização de tipos de dados                                          │
│  • Normalização de emails (lowercase)                                      │
│  • Validção de integridade referencial                                     │
│  • Join entre tabelas para enriquecimento                                  │
│  • Colunas derivadas: data_particionamento, hora                           │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CAMADA GOLD (Business Analytics)                                          │
│  BigQuery Dataset: igaming_gold                                            │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                     FRAUD ANALYTICS                               │    │
│  │  • fct_fraud_signals          (sinais de fraude por player)       │    │
│  │  • fct_multi_accounting       (IPs/devices compartilhados)       │    │
│  │  • fct_deposit_without_bet    (depósito sem aposta)              │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                   AFFILIATE PERFORMANCE                           │    │
│  │  • fct_affiliate_performance   (CPA, FTD, conversão, ROI)        │    │
│  │  • fct_affiliate_fraud         (afiliados com padrão suspeito)   │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                   FINANCIAL SIGNALS                               │    │
│  │  • fct_financial_summary       (depósitos, saques, GGR)          │    │
│  │  • fct_player_ltv              (LTV e ARPU por player)           │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  • Métricas pré-calculadas para dashboards                                 │
│  • Agregações por período, afiliado, player                                │
│  • Sinais de fraude derivados da Silver                                    │
│  • Pronto para consumo por Power BI                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     CONSUMO - Power BI Dashboard                            │
│                                                                             │
│  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────────────┐   │
│  │ Fraud Overview   │ │ Affiliate Metrics│ │ Financial Signals        │   │
│  │ • Contas suspeit.│ │ • CPA/FTE        │ │ • Volume depósitos/saques│   │
│  │ • IPs compartilh│ │ • Conversão      │ │ • GGR/NGR               │   │
│  │ • Taxa de fraude │ │ • ROI afiliados  │ │ • LTV/ARPU              │   │
│  └──────────────────┘ └──────────────────┘ └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Descrição das Camadas

### Bronze (Raw Ingestion)
**Propósito**: Preservar os dados exatamente como chegaram da fonte.

- Dados 100% originais sem nenhuma transformação
- Coluna `_ingested_at` adicionada para rastreabilidade
- Schema idêntico ao arquivo fonte (JSON/CSV)
- Particionamento por data de ingestão para controle de cargas incrementais
- **Não há** deduplicação, validação ou join entre tabelas
- BigQuery: Dataset `igaming_bronze`, tabela `raw_{nome_fonte}`

### Silver (Cleaned & Conformed)
**Propósito**: Dados limpos, validados e conformados para análise.

- **Deduplicação**: Remoção de registros duplicados por chave primária
- **Padronização**: Normalização de emails (lowercase), datas (ISO 8601), tipos
- **Validação**: Verificação de integridade referencial (player_id existe)
- **Enriquecimento**: Join entre tabelas para campos derivados
- **Derivação**: Colunas calculadas (data_particao, hora, dia_semana)
- **Tipagem**: Conversão de tipos (STRING → DATE, FLOAT → DECIMAL)
- BigQuery: Dataset `igaming_silver`, tabelas `dim_*` e `fct_*`

### Gold (Business Analytics)
**Propósito**: Métricas e KPIs prontos para consumo analítico e dashboards.

- **Fraude**: Sinais de fraude pré-calculados (multi-accounting, depósito sem aposta)
- **Afiliados**: Performance (CPA, FTD, taxa de conversão, ROI)
- **Financeiro**: Resumo de depósitos/saques, GGR, LTV, ARPU
- **Agregações**: Métricas prontas para Power BI (sem necessidade de cálculos complexos no dashboard)
- BigQuery: Dataset `igaming_gold`, tabelas `fct_*`

---

## Stack Tecnológica

| Componente | Ferramenta | Justificativa |
|------------|-----------|---------------|
| Orquestração | Apache Airflow | Orquestração de pipelines, agendamento, retry, logging |
| Transformação | dbt (data build tool) | Transformação SQL versionada, testes, documentação automática |
| Data Warehouse | Google BigQuery | Data warehouse serverless, escalável, com tier gratuito |
| Dashboard | Power BI | Visualização interativa, conectores nativos para BigQuery |
| Linguagem | SQL + Python | SQL para transformações (dbt), Python para Airflow e scripts auxiliares |

---

## Fluxo de Controle de Qualidade

```
Ingestão → Schema Validation → Duplicatas → Integridade → Regras de Negócio
   │              │                  │              │                │
   │         Rejeita se         Remove        Alerta se       Marca se
   │         schema mudou      duplicados     FK inválida     regra violada
   │              │                  │              │                │
   └──────────────┴──────────────────┴──────────────┴────────────────┘
                              │
                         Bronze ←→ Silver ←→ Gold
```

---

## Observabilidade

| Ponto de Observação | Métrica | Ação |
|---------------------|---------|------|
| Ingestão | Volume de registros ingeridos | Alerta se < 90% do esperado |
| Schema | Mudança de estrutura | Alerta imediato (possível quebra) |
| Qualidade | % registros válidos | Alerta se < 95% |
| Latência | Tempo entre fonte e Gold | Dashboard de SLA |
| Processamento | Tempo de execução dbt | Alerta se > threshold |
| Fraude | Nº sinais detectados | Dashboard em tempo real |

# Case Técnico — Especialista de Dados | Grupo OTG

**Autor:** Iraê Szabo Macri
**Email:** irae.macri@gmail.com
**Data:** Agosto 2026

---

## Contexto

Pipeline completa de dados para plataforma de iGaming, construída com **Medallion Architecture** (Bronze → Silver → Gold). O projeto resolve ingestion, tratamento e análise de dados de múltiplas fontes (jogadores, sessões, transações financeiras e afiliados) com foco em **detecção de fraude** e **performance de afiliados**.

---

## Stack Tecnológica

| Camada | Ferramenta | Função |
|--------|-----------|--------|
| Orquestração | Apache Airflow | DAGs, agendamento, retry, logging |
| Transformação | dbt | Modelos SQL versionados, testes, documentação |
| Data Warehouse | Google BigQuery | Armazenamento e processamento analítico |
| Dashboard | Power BI | Visualização interativa |
| Linguagem | SQL + Python | Transformações e orquestração |

---

## Arquitetura Medallion

```
Fontes (JSON/CSV)
       │
       ▼
┌──────────────┐
│   BRONZE     │  Dados crus, sem transformação
│  (Raw)       │  Schema idêntico à fonte
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   SILVER     │  Limpeza, deduplicação, validação
│  (Clean)     │  Joins entre tabelas, padronização
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    GOLD      │  Métricas analíticas prontas
│ (Analytics)  │  KPIs, sinais de fraude, afiliados
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  POWER BI    │  Dashboards interativos
│ (Dashboard)  │  Fraud / Affiliates / Financial
└──────────────┘
```

Documentação completa: [`arquitetura/01_arquitetura_medallion.md`](arquitetura/01_arquitetura_medallion.md)

---

## Definição de Cargas

| Dataset | Frequência | Tipo | Justificativa |
|---------|-----------|------|---------------|
| players.json | Diária | Full | Volume pequeno, atualizações raras |
| sessions.json | Horária | Incremental | Detecção de fraude exige dados recentes |
| transactions.csv | Horária | Incremental | Transações financeiras são críticas em tempo real |
| affiliate_cpa_ftd.csv | Diária | Incremental | Performance de afiliados avaliada diariamente |

Documentação completa: [`arquitetura/02_definicao_cargas.md`](arquitetura/02_definicao_cargas.md)

---

## Estrutura do Projeto

```
.
├── arquitetura/              # Documentação da arquitetura e cargas
│   ├── 01_arquitetura_medallion.md
│   └── 02_definicao_cargas.md
├── airflow/
│   └── dags/                 # DAGs de orquestração
├── dbt/
│   ├── dbt_project.yml       # Configuração do projeto dbt
│   └── models/
│       ├── bronze/           # Raw ingestion (staging)
│       ├── silver/           # Limpeza e conformação
│       └── gold/             # Métricas analíticas
├── dashboard/                # Prints e documentação do dashboard
├── BASES_CASE/               # Dados brutos fornecidos
│   ├── players.json
│   ├── sessions.json
│   ├── transactions.csv
│   └── affiliate_cpa_ftd.csv
└── README.md
```

---

## Como Rodar

### Pré-requisitos
- Python 3.9+
- Google Cloud Platform (BigQuery)
- Airflow (local ou managed)
- dbt Core
- Power BI Desktop

### 1. Configuração do dbt
```bash
cd dbt
pip install dbt-bigquery
dbt deps
dbt seed --profiles-dir ~/.dbt
dbt run --profiles-dir ~/.dbt
dbt test --profiles-dir ~/.dbt
```

### 2. Configuração do Airflow
```bash
cd airflow
pip install apache-airflow-providers-google
# Copiar dags/ para o DAG_FOLDER do Airflow
# Configurar conexão com BigQuery no Airflow UI
```

### 3. Dashboard
Abrir o arquivo `.pbix` no Power BI Desktop ou acessar os prints na pasta `dashboard/`.

---

## Sinais de Fraude Implementados

A camada **Gold** implementa detecção de fraude com base em:

1. **Multi-accounting** — Mesmo IP ou device utilizado em múltiplas contas de jogadores
2. **Depósito sem aposta (Deposit without Bet)** — Jogadores que depositam mas não realizam apostas
3. **Padrão anômalo de afiliado** — Afiliados com alta taxa de FTD mas baixo volume de clicks

Detalhes em: [`dbt/models/gold/`](dbt/models/gold/)

---

## Entregáveis do Case

| # | Entregável | Status | Localização |
|---|-----------|--------|-------------|
| 1 | Arquitetura (diagrama) | Concluído | [`arquitetura/01_arquitetura_medallion.md`](arquitetura/01_arquitetura_medallion.md) |
| 2 | Definição de cargas | Concluído | [`arquitetura/02_definicao_cargas.md`](arquitetura/02_definicao_cargas.md) |
| 3 | Airflow DAG | Concluído | [`airflow/dags/`](airflow/dags/) |
| 4 | Modelos dbt | Concluído | [`dbt/models/`](dbt/models/) |
| 5 | Observabilidade | Concluído | [`arquitetura/01_arquitetura_medallion.md`](arquitetura/01_arquitetura_medallion.md#observabilidade) |
| 6 | Análise de fraude | Concluído | [`dbt/models/gold/`](dbt/models/gold/) |
| 7 | Dashboard Power BI | Concluído | [`dashboard/`](dashboard/) |

---

## Contato

**[Seu Nome]**
- LinkedIn: [seu-linkedin]
- Email: [seu-email]

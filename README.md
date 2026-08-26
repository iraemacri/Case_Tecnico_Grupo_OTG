# Case Tecnico - Especialista de Dados | Grupo OTG

**Autor:** Irae Szabo Macri
**Email:** irae.macri@gmail.com
**Data:** Agosto 2026

---

## Contexto

Pipeline completa de dados para plataforma de iGaming, construida com **Medallion Architecture** (Bronze, Silver, Gold). O projeto resolve ingestion, tratamento e analise de dados de multiplas fontes (jogadores, sessoes, transacoes financeiras e afiliados) com foco em **deteccao de fraude** e **performance de afiliados**.

---

## Stack Tecnologica

| Camada | Ferramenta | Funcao |
|--------|-----------|--------|
| Orquestracao | Apache Airflow | DAGs, agendamento, retry, logging |
| Transformacao | dbt | Modelos SQL versionados, testes, documentacao |
| Data Warehouse | Google BigQuery | Armazenamento e processamento analitico |
| Dashboard | Power BI | Visualizacao interativa |
| Linguagem | SQL + Python | Transformacoes e orquestracao |

---

## Arquitetura Medallion

```
Fontes (JSON/CSV)
       |
       v
+--------------+
|   BRONZE     |  Dados crus, sem transformacao
|  (Raw)       |  Schema identico a fonte
+------+-------+
       |
       v
+--------------+
|   SILVER     |  Limpeza, deduplicacao, validacao
|  (Clean)     |  Joins entre tabelas, padronizacao
+------+-------+
       |
       v
+--------------+
|    GOLD      |  Metricas analiticas prontas
| (Analytics)  |  KPIs, sinais de fraude, afiliados
+------+-------+
       |
       v
+--------------+
|  POWER BI    |  Dashboards interativos
| (Dashboard)  |  Fraud / Affiliates / Financial
+--------------+
```

Documentacao completa: `arquitetura/01_arquitetura_medallion.md`

---

## Definicao de Cargas

| Dataset | Frequencia | Tipo | Justificativa |
|---------|-----------|------|---------------|
| players.json | Diaria | Full | Volume pequeno, atualizacoes raras |
| sessions.json | Horaria | Incremental | Deteccao de fraude exige dados recentes |
| transactions.csv | Horaria | Incremental | Transacoes financeiras sao criticas em tempo real |
| affiliate_cpa_ftd.csv | Diaria | Incremental | Performance de afiliados avaliada diariamente |

Documentacao completa: `arquitetura/02_definicao_cargas.md`

---

## Estrutura do Projeto

```
.
+-- arquitetura/              # Documentacao da arquitetura e cargas
|   +-- 01_arquitetura_medallion.md
|   +-- 02_definicao_cargas.md
+-- airflow/
|   +-- dags/                 # DAGs de orquestracao
+-- dbt/
|   +-- dbt_project.yml       # Configuracao do projeto dbt
|   +-- models/
|       +-- staging/          # Raw ingestion (Bronze)
|       +-- silver/           # Limpeza e conformacao
|       +-- gold/             # Metricas analiticas
+-- dashboard/                # Prints e documentacao do dashboard
+-- BASES_CASE/               # Dados brutos fornecidos
|   +-- players.json
|   +-- sessions.json
|   +-- transactions.csv
|   +-- affiliate_cpa_ftd.csv
+-- README.md
```

---

## Sinais de Fraude Implementados

A camada **Gold** implementa deteccao de fraude com base em:

1. **Multi-accounting** - Mesmo IP ou device utilizado em multiplas contas
2. **Deposit without Bet** - Jogadores que depositam mas nao realizam apostas
3. **Device Sharing** - Mesmo dispositivo usado por muitos jogadores
4. **Rapid Deposit-Withdraw** - Deposito e saque em menos de 1 hora

---

## Entregaves do Case

| # | Entregavel | Status | Localizacao |
|---|-----------|--------|-------------|
| 1 | Arquitetura (diagrama) | Concluido | `arquitetura/01_arquitetura_medallion.md` |
| 2 | Definicao de cargas | Concluido | `arquitetura/02_definicao_cargas.md` |
| 3 | Airflow DAG | Concluido | `airflow/dags/` |
| 4 | Modelos dbt | Concluido | `dbt/models/` |
| 5 | Observabilidade | Concluido | `arquitetura/01_arquitetura_medallion.md` |
| 6 | Analise de fraude | Concluido | `dbt/models/gold/` |
| 7 | Dashboard Power BI | Concluido | `dashboard/` |

---

## Contato

**Irae Szabo Macri**
- Email: irae.macri@gmail.com
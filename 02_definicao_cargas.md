# Definição de Cargas de Dados

## Visão Geral

Cada dataset possui características distintas que determinam a melhor estratégia de carga. A definição leva em consideração: frequência de atualização na fonte, volume de dados, criticidade temporal e custo de processamento.

---

## Tabela de Definição de Cargas

| Dataset | Frequência | Tipo de Carga | Justificativa |
|---------|-----------|---------------|---------------|
| `players.json` | Diária | **Full** | Dados de cadastro mudam pouco; volume pequeno (600 registros); full load é mais simples e garante consistência |
| `sessions.json` | Horária | **Incremental** | Alto volume (4.000 registros); dados chegam contínuus; precisamos de dados recentes para detecção de fraude em tempo próximo do real |
| `transactions.csv` | Horária | **Incremental** | Movimentações financeiras são críticas e mudam constantemente; precisamos capturar depósitos/saques/apostas o mais rápido possível |
| `affiliate_cpa_ftd.csv` | Diária | **Incremental** | Performance de afiliados é avaliada diariamente; FTDs e registros podem ser atualizados ao longo do dia; incremental evita recarga desnecessária |

---

## Justificativas Detalhadas

### 1. players.json — Diária / Full Load

**Por que diária?**
- Cadastro de jogadores não muda em tempo real
- Novos registros chegam ao longo do dia, mas não há urgência em capturá-los a cada hora
- Processamento diário reduz custo de computação no BigQuery

**Por que Full Load?**
- Volume pequeno (~600 registros) — recarregar tudo é rápido e barato
- Dados de cadastro podem ter atualizações pontuais (ex: correção de email)
- Full load garante que qualquer atualização seja capturada, mesmo sem controle incremental
- Simplifica a lógica de transformação (não precisa de merge/upsert)

**Controle de qualidade:**
- Validação pós-carga: comparar contagem de registros com fonte
- Verificar se schema não mudou

---

### 2. sessions.json — Horária / Incremental

**Por que horária?**
- Sessões de acesso são dados de comportamento em tempo real
- Detecção de fraude depende de dados recentes (ex: múltiplos logins do mesmo IP em 1 hora)
- Atraso de 24h compromete a capacidade de identificar padrões suspeitos
- Horário é suficiente — não precisamos de streaming (que seria overkill para este caso)

**Por que Incremental?**
- Alto volume (4.000 registros) — recarregar tudo a cada hora seria desperdício
- Dados antigos não mudam (uma sessão criada é estática)
- Incremental usa `timestamp` como controle — carrega apenas novos registros
- Reduz custo de processamento e tempo de execução

**Campos de controle incremental:**
- `timestamp` — filtro para carregar apenas registros novos desde a última carga
- `_ingested_at` — rastreabilidade de quando cada registro entrou na Bronze

**Lógica:**
```sql
WHERE timestamp > (SELECT MAX(timestamp) FROM bronze.raw_sessions)
```

---

### 3. transactions.csv — Horária / Incremental

**Por que horária?**
- Transações financeiras são o heart beat da operação
- Depósitos e saques precisam ser monitorados em tempo próximo do real
- Fraudes financeiras (ex: depósito alto + saque imediato) precisam de detecção rápida
- Atraso de 24h pode significar perda financeira não detectada

**Por que Incremental?**
- Volume considerável (1.800 registros)
- Transações são append-only (não são atualizadas)
- Incremental elimina reprocessamento de dados históricos
- Permite controle preciso de deltas

**Campos de controle incremental:**
- `timestamp` — filtro temporal para capturar transações novas
- `_ingested_at` — controle de duplicatas na Bronze

**Lógica:**
```sql
WHERE timestamp > (SELECT MAX(timestamp) FROM bronze.raw_transactions)
```

---

### 4. affiliate_cpa_ftd.csv — Diária / Incremental

**Por que diária?**
- Performance de afiliados é avaliada em bases diárias/semanais
- FTDs (First Time Deposits) são contabilizados ao final do dia
- Não há necessidade de monitoramento em tempo real para esta métrica
- Processamento diário permite validação mais cuidadosa

**Por que Incremental?**
- Volume alto (2.000 registros)
- Dados de afiliados podem ser atualizados (ex: rebalanceamento de FTDs)
- Incremental permite atualizar apenas registros modificados
- Evita recarga de dados históricos que não mudam

**Campos de controle incremental:**
- Combinação `affiliate_id + player_id` — identifica único relacionamento
- `_ingested_at` — controle de novos registros
- `ftd` — campo que pode ser atualizado (quando player atinge baseline)

**Lógica:**
```sql
-- Upsert baseado em chave composta
MERGE INTO silver.dim_affiliates AS target
USING bronze.raw_affiliates AS source
ON target.affiliate_id = source.affiliate_id
   AND target.player_id = source.player_id
WHEN MATCHED AND source._ingested_at > target._ingested_at THEN
    UPDATE SET ...
WHEN NOT MATCHED THEN
    INSERT ...
```

---

## Fluxo de Controle Incremental

```
┌─────────────────────────────────────────────────────┐
│                  CONTROLE DE CARGA                   │
├─────────────────────────────────────────────────────┤
│                                                     │
│  1. Airflow task verifica última carga               │
│     └─ SELECT MAX(_ingested_at) FROM bronze.table   │
│                                                     │
│  2. Filtra dados fonte desde última carga            │
│     └─ WHERE timestamp > ultima_carga               │
│                                                     │
│  3. Insere na Bronze com _ingested_at = NOW()       │
│     └─ INSERT com timestamp de ingestão             │
│                                                     │
│  4. dbt Silver faz merge/upsert                     │
│     └─ MERGE INTO silver USING bronze               │
│                                                     │
│  5. dbt Gold recalcula métricas                     │
│     └─ RefRESH incremental ou full                  │
│                                                     │
│  6. Log de resultado para observabilidade           │
│     └─ Registros ingeridos / erros / latência       │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## Resumo Visual

```
Dataset              Freq        Tipo          Urgência     Volume
─────────────────────────────────────────────────────────────────
players.json         Diária      Full          Baixa        600
sessions.json        Horária     Incremental   Alta         4.000
transactions.csv     Horária     Incremental   Alta         1.800
affiliate_cpa_ftd    Diária      Incremental   Média        2.000
```

---

## Notas Importantes

1. **BigQuery é serverless**: não precisa se preocupar com infraestrutura, apenas com a lógica de carga
2. **dbt Core é open source**: gratuito, roda local ou em CI/CD
3. **Airflow é open source**: pode rodar local para demonstração do case
4. **Power BI Desktop é gratuito**: suficiente para criar o dashboard do case
5. **Tier gratuito BigQuery**: 1 TB/mês — mais que suficiente para os dados deste case

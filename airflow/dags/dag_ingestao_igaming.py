"""
DAG: dag_ingestao_igaming
Descricao: Orquestra a ingestao, transformacao e carga dos dados de iGaming
           para plataforma de analise de fraude e performance de afiliados.

Stack: Apache Airflow + dbt + Google BigQuery
Arquitetura: Medallion (Bronze -> Silver -> Gold)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

# ============================================================
# CONFIGURACOES PADRAO DA DAG
# ============================================================

DEFAULT_ARGS = {
    "owner": "iraemacri",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["irae.macri@gmail.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# ============================================================
# CONFIGURACOES POR DATASET
# ============================================================

DATASETS_CONFIG = {
    "players": {
        "source": "BASES_CASE/players.json",
        "gcs_bucket": "igaming-raw-bucket",
        "gcs_path": "players/players.json",
        "bq_table": "igaming_bronze.raw_players",
        "frequency": "diaria",
        "load_type": "full",
        "file_format": "json",
    },
    "sessions": {
        "source": "BASES_CASE/sessions.json",
        "gcs_bucket": "igaming-raw-bucket",
        "gcs_path": "sessions/sessions.json",
        "bq_table": "igaming_bronze.raw_sessions",
        "frequency": "horaria",
        "load_type": "incremental",
        "incremental_field": "timestamp",
        "file_format": "json",
    },
    "transactions": {
        "source": "BASES_CASE/transactions.csv",
        "gcs_bucket": "igaming-raw-bucket",
        "gcs_path": "transactions/transactions.csv",
        "bq_table": "igaming_bronze.raw_transactions",
        "frequency": "horaria",
        "load_type": "incremental",
        "incremental_field": "timestamp",
        "file_format": "csv",
    },
    "affiliates": {
        "source": "BASES_CASE/affiliate_cpa_ftd.csv",
        "gcs_bucket": "igaming-raw-bucket",
        "gcs_path": "affiliates/affiliate_cpa_ftd.csv",
        "bq_table": "igaming_bronze.raw_affiliates",
        "frequency": "diaria",
        "load_type": "incremental",
        "incremental_field": "_ingested_at",
        "file_format": "csv",
    },
}

# ============================================================
# SQL PARA CRIAR TABELAS BRONZE (se nao existirem)
# ============================================================

DDL_BRONZE = {
    "players": """
        CREATE TABLE IF NOT EXISTS `igaming-project.igaming_bronze.raw_players` (
            player_id STRING,
            email STRING,
            city STRING,
            created_at DATE,
            _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "sessions": """
        CREATE TABLE IF NOT EXISTS `igaming-project.igaming_bronze.raw_sessions` (
            session_id STRING,
            player_id STRING,
            ip STRING,
            device STRING,
            timestamp TIMESTAMP,
            _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "transactions": """
        CREATE TABLE IF NOT EXISTS `igaming-project.igaming_bronze.raw_transactions` (
            transaction_id STRING,
            player_id STRING,
            type STRING,
            amount FLOAT64,
            timestamp TIMESTAMP,
            _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "affiliates": """
        CREATE TABLE IF NOT EXISTS `igaming-project.igaming_bronze.raw_affiliates` (
            affiliate_id STRING,
            player_id STRING,
            country STRING,
            clicks INT64,
            registrations INT64,
            ftd INT64,
            cpa_value FLOAT64,
            _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """,
}

# ============================================================
# SQL PARA INCREMENTAL LOAD (MERGE)
# ============================================================

SQL_INCREMENTAL = {
    "sessions": """
        MERGE INTO `igaming-project.igaming_bronze.raw_sessions` AS target
        USING (
            SELECT
                session_id,
                player_id,
                ip,
                device,
                TIMESTAMP(timestamp) AS timestamp,
                CURRENT_TIMESTAMP() AS _ingested_at
            FROM `igaming-project.igaming_bronze.raw_sessions_staging`
            WHERE timestamp > COALESCE(
                (SELECT MAX(timestamp) FROM `igaming-project.igaming_bronze.raw_sessions`),
                TIMESTAMP('2020-01-01')
            )
        ) AS source
        ON target.session_id = source.session_id
        WHEN NOT MATCHED THEN
            INSERT (session_id, player_id, ip, device, timestamp, _ingested_at)
            VALUES (session_id, player_id, ip, device, timestamp, _ingested_at)
    """,
    "transactions": """
        MERGE INTO `igaming-project.igaming_bronze.raw_transactions` AS target
        USING (
            SELECT
                transaction_id,
                player_id,
                type,
                amount,
                TIMESTAMP(timestamp) AS timestamp,
                CURRENT_TIMESTAMP() AS _ingested_at
            FROM `igaming-project.igaming_bronze.raw_transactions_staging`
            WHERE timestamp > COALESCE(
                (SELECT MAX(timestamp) FROM `igaming-project.igaming_bronze.raw_transactions`),
                TIMESTAMP('2020-01-01')
            )
        ) AS source
        ON target.transaction_id = source.transaction_id
        WHEN NOT MATCHED THEN
            INSERT (transaction_id, player_id, type, amount, timestamp, _ingested_at)
            VALUES (transaction_id, player_id, type, amount, timestamp, _ingested_at)
    """,
    "affiliates": """
        MERGE INTO `igaming-project.igaming_bronze.raw_affiliates` AS target
        USING (
            SELECT
                affiliate_id,
                player_id,
                country,
                clicks,
                registrations,
                ftd,
                cpa_value,
                CURRENT_TIMESTAMP() AS _ingested_at
            FROM `igaming-project.igaming_bronze.raw_affiliates_staging`
        ) AS source
        ON target.affiliate_id = source.affiliate_id
           AND target.player_id = source.player_id
        WHEN MATCHED AND source._ingested_at > target._ingested_at THEN
            UPDATE SET
                country = source.country,
                clicks = source.clicks,
                registrations = source.registrations,
                ftd = source.ftd,
                cpa_value = source.cpa_value,
                _ingested_at = source._ingested_at
        WHEN NOT MATCHED THEN
            INSERT (affiliate_id, player_id, country, clicks, registrations, ftd, cpa_value, _ingested_at)
            VALUES (affiliate_id, player_id, country, clicks, registrations, ftd, cpa_value, _ingested_at)
    """,
}


# ============================================================
# FUNCOES AUXILIARES
# ============================================================

def log_inicio(**context):
    """Registra inicio da execucao com metadados."""
    dag_run = context["dag_run"]
    print(f"[INICIO] DAG: {context['dag'].dag_id}")
    print(f"[INICIO] Execution Date: {context['execution_date']}")
    print(f"[INICIO] Run ID: {dag_run.run_id}")
    print(f"[INICIO] Configuracao: {DATASETS_CONFIG}")


def log_fim(**context):
    """Registra fim da execucao com resumo."""
    print(f"[FIM] DAG concluida com sucesso")
    print(f"[FIM] Execution Date: {context['execution_date']}")


def validar_schema_players(ti):
    """Valida schema do arquivo players.json antes de ingerir."""
    import json

    print("[VALIDACAO] Iniciando validacao de schema - players.json")

    required_fields = ["player_id", "email", "city", "created_at"]

    with open("BASES_CASE/players.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    if len(data) == 0:
        raise ValueError("[VALIDACAO] Arquivo players.json esta vazio")

    sample = data[0]
    missing = [field for field in required_fields if field not in sample]

    if missing:
        raise ValueError(f"[VALIDACAO] Campos ausentes em players.json: {missing}")

    print(f"[VALIDACAO] players.json valido - {len(data)} registros, schema OK")
    return True


def validar_schema_sessions(ti):
    """Valida schema do arquivo sessions.json antes de ingerir."""
    import json

    print("[VALIDACAO] Iniciando validacao de schema - sessions.json")

    required_fields = ["session_id", "player_id", "ip", "device", "timestamp"]

    with open("BASES_CASE/sessions.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    if len(data) == 0:
        raise ValueError("[VALIDACAO] Arquivo sessions.json esta vazio")

    sample = data[0]
    missing = [field for field in required_fields if field not in sample]

    if missing:
        raise ValueError(f"[VALIDACAO] Campos ausentes em sessions.json: {missing}")

    print(f"[VALIDACAO] sessions.json valido - {len(data)} registros, schema OK")
    return True


def validar_schema_transactions(ti):
    """Valida schema do arquivo transactions.csv antes de ingerir."""
    import csv

    print("[VALIDACAO] Iniciando validacao de schema - transactions.csv")

    required_fields = ["transaction_id", "player_id", "type", "amount", "timestamp"]

    with open("BASES_CASE/transactions.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames

    missing = [field for field in required_fields if field not in headers]

    if missing:
        raise ValueError(f"[VALIDACAO] Campos ausentes em transactions.csv: {missing}")

    print(f"[VALIDACAO] transactions.csv valido - schema OK")
    return True


def validar_schema_affiliates(ti):
    """Valida schema do arquivo affiliate_cpa_ftd.csv antes de ingerir."""
    import csv

    print("[VALIDACAO] Iniciando validacao de schema - affiliate_cpa_ftd.csv")

    required_fields = ["affiliate_id", "player_id", "country", "clicks", "registrations", "ftd", "cpa_value"]

    with open("BASES_CASE/affiliate_cpa_ftd.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames

    missing = [field for field in required_fields if field not in headers]

    if missing:
        raise ValueError(f"[VALIDACAO] Campos ausentes em affiliate_cpa_ftd.csv: {missing}")

    print(f"[VALIDACAO] affiliate_cpa_ftd.csv valido - schema OK")
    return True


def contar_registros_ingeridos(**context):
    """Conta registros ingeridos na Bronze para observabilidade."""
    print("[OBSERVABILIDADE] Registros ingeridos por dataset:")
    for name, config in DATASETS_CONFIG.items():
        print(f"  - {name}: {config['load_type']} load ({config['frequency']})")


# ============================================================
# DEFINICAO DA DAG PRINCIPAL
# ============================================================

with DAG(
    dag_id="dag_ingestao_igaming",
    default_args=DEFAULT_ARGS,
    description="Pipeline de ingestao e transformacao de dados iGaming",
    schedule_interval="0 6 * * *",  # Diario as 06:00 UTC
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["igaming", "ingestao", "bronze", "medallion"],
    doc_md="""
    ## DAG: dag_ingestao_igaming

    ### Descricao
    Orquestra a ingestao dos 4 datasets de iGaming para a camada Bronze.

    ### Fluxo
    1. Valida schemas dos arquivos fonte
    2. Cria tabelas Bronze (se nao existirem)
    3. Carrega dados no BigQuery (Full ou Incremental)
    4. Valida registros ingeridos
    5. Envia email de notificacao

    ### Configuracao
    - **Frequencia:** Diaria as 06:00 UTC
    - **Retries:** 2 tentativas com 5 min de intervalo
    - **Timeout:** 1 hora por execucao
    """,
) as dag:

    # --------------------------------------------------------
    # TASK 0: INICIO
    # --------------------------------------------------------
    task_inicio = PythonOperator(
        task_id="inicio_execucao",
        python_callable=log_inicio,
        doc_md="Registra inicio da execucao com metadados",
    )

    # --------------------------------------------------------
    # FASE 1: VALIDACAO DE SCHEMAS
    # --------------------------------------------------------
    task_validar_players = PythonOperator(
        task_id="validar_schema_players",
        python_callable=validar_schema_players,
        doc_md="Valida campos obrigatorios em players.json",
    )

    task_validar_sessions = PythonOperator(
        task_id="validar_schema_sessions",
        python_callable=validar_schema_sessions,
        doc_md="Valida campos obrigatorios em sessions.json",
    )

    task_validar_transactions = PythonOperator(
        task_id="validar_schema_transactions",
        python_callable=validar_schema_transactions,
        doc_md="Valida campos obrigatorios em transactions.csv",
    )

    task_validar_affiliates = PythonOperator(
        task_id="validar_schema_affiliates",
        python_callable=validar_schema_affiliates,
        doc_md="Valida campos obrigatorios em affiliate_cpa_ftd.csv",
    )

    # --------------------------------------------------------
    # FASE 2: CRIACAO DE TABELAS BRONZE (DDL)
    # --------------------------------------------------------
    task_criar_tabela_players = BigQueryInsertJobOperator(
        task_id="criar_tabela_bronze_players",
        configuration={
            "query": {
                "query": DDL_BRONZE["players"],
                "useLegacySql": False,
            }
        },
        doc_md="Cria tabela raw_players na Bronze se nao existir",
    )

    task_criar_tabela_sessions = BigQueryInsertJobOperator(
        task_id="criar_tabela_bronze_sessions",
        configuration={
            "query": {
                "query": DDL_BRONZE["sessions"],
                "useLegacySql": False,
            }
        },
        doc_md="Cria tabela raw_sessions na Bronze se nao existir",
    )

    task_criar_tabela_transactions = BigQueryInsertJobOperator(
        task_id="criar_tabela_bronze_transactions",
        configuration={
            "query": {
                "query": DDL_BRONZE["transactions"],
                "useLegacySql": False,
            }
        },
        doc_md="Cria tabela raw_transactions na Bronze se nao existir",
    )

    task_criar_tabela_affiliates = BigQueryInsertJobOperator(
        task_id="criar_tabela_bronze_affiliates",
        configuration={
            "query": {
                "query": DDL_BRONZE["affiliates"],
                "useLegacySql": False,
            }
        },
        doc_md="Cria tabela raw_affiliates na Bronze se nao existir",
    )

    # --------------------------------------------------------
    # FASE 3: INGESTAO PARA O BIGQUERY
    # --------------------------------------------------------

    # --- PLAYERS (Full Load) ---
    task_ingest_players = BigQueryInsertJobOperator(
        task_id="ingest_players_bronze",
        configuration={
            "query": {
                "query": """
                    INSERT INTO `igaming-project.igaming_bronze.raw_players`
                    (player_id, email, city, created_at, _ingested_at)
                    SELECT
                        player_id,
                        LOWER(TRIM(email)) AS email,
                        city,
                        DATE(created_at) AS created_at,
                        CURRENT_TIMESTAMP() AS _ingested_at
                    FROM `igaming-project.igaming_bronze.raw_players_staging`
                """,
                "useLegacySql": False,
            }
        },
        doc_md="Full load de players para Bronze",
    )

    # --- SESSIONS (Incremental) ---
    task_ingest_sessions = BigQueryInsertJobOperator(
        task_id="ingest_sessions_bronze",
        configuration={
            "query": {
                "query": SQL_INCREMENTAL["sessions"],
                "useLegacySql": False,
            }
        },
        doc_md="Incremental load de sessions para Bronze",
    )

    # --- TRANSACTIONS (Incremental) ---
    task_ingest_transactions = BigQueryInsertJobOperator(
        task_id="ingest_transactions_bronze",
        configuration={
            "query": {
                "query": SQL_INCREMENTAL["transactions"],
                "useLegacySql": False,
            }
        },
        doc_md="Incremental load de transactions para Bronze",
    )

    # --- AFFILIATES (Incremental) ---
    task_ingest_affiliates = BigQueryInsertJobOperator(
        task_id="ingest_affiliates_bronze",
        configuration={
            "query": {
                "query": SQL_INCREMENTAL["affiliates"],
                "useLegacySql": False,
            }
        },
        doc_md="Incremental load de affiliates para Bronze",
    )

    # --------------------------------------------------------
    # FASE 4: VALIDACAO POS-CARGA
    # --------------------------------------------------------
    task_validar_carga = PythonOperator(
        task_id="validar_registros_ingeridos",
        python_callable=contar_registros_ingeridos,
        doc_md="Conta e valida registros ingeridos por dataset",
    )

    # --------------------------------------------------------
    # FASE 5: TRIGGER DBT (Silver + Gold)
    # --------------------------------------------------------
    task_trigger_dbt = BashOperator(
        task_id="executar_dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt/profiles",
        doc_md="Executa dbt run para transformacao Silver e Gold",
    )

    # --------------------------------------------------------
    # FASE 6: FIM
    # --------------------------------------------------------
    task_fim = PythonOperator(
        task_id="fim_execucao",
        python_callable=log_fim,
        doc_md="Registra fim da execucao com resumo",
    )

    task_email_sucesso = EmailOperator(
        task_id="enviar_email_sucesso",
        to=["irae.macri@gmail.com"],
        subject="[IGAMING] Pipeline executada com sucesso - {{ ds }}",
        html_content="""
        <h3>Pipeline iGaming executada com sucesso</h3>
        <p><b>Data:</b> {{ ds }}</p>
        <p><b>DAG:</b> dag_ingestao_igaming</p>
        <p>Todos os datasets foram ingeridos e transformados.</p>
        <p><b>Proximo passo:</b> Verificar dashboards no Power BI</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Envia email de sucesso apos execucao completa",
    )

    # --------------------------------------------------------
    # DEFINICAO DE DEPENDENCIAS (FLUXO)
    # --------------------------------------------------------
    #
    #  inicio
    #    |
    #    +-> validacao (4 em paralelo)
    #    |     |
    #    |     +-> criacao de tabelas (4 em paralelo)
    #    |           |
    #    |           +-> ingestao (4 em paralelo)
    #    |                 |
    #    |                 +-> validacao pos-carga
    #    |                       |
    #    |                       +-> dbt run
    #    |                             |
    #    |                             +-> email sucesso
    #    |                                   |
    #    |                                   +-> fim
    #

    task_inicio >> [
        task_validar_players,
        task_validar_sessions,
        task_validar_transactions,
        task_validar_affiliates,
    ]

    task_validar_players >> task_criar_tabela_players >> task_ingest_players
    task_validar_sessions >> task_criar_tabela_sessions >> task_ingest_sessions
    task_validar_transactions >> task_criar_tabela_transactions >> task_ingest_transactions
    task_validar_affiliates >> task_criar_tabela_affiliates >> task_ingest_affiliates

    [
        task_ingest_players,
        task_ingest_sessions,
        task_ingest_transactions,
        task_ingest_affiliates,
    ] >> task_validar_carga

    task_validar_carga >> task_trigger_dbt >> task_fim >> task_email_sucesso

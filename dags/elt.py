"""Pipeline ELT : collecte → dbt (Cosmos) → contrôles, avec reprise automatique et alertes.

- collecte : charge la tranche du jour dans raw.persons (idempotent : DELETE puis INSERT).
- dbt : une tâche Airflow par modèle, tests exécutés juste après chaque modèle.
- contrôles : règles métier finales sur analytics.persons_summary.

Variables optionnelles : ALERT_WEBHOOK_URL, DBT_PROJECT_DIR, DBT_EXECUTABLE_PATH.
Déclencher avec {"simuler_panne": true} pour tester retries et alertes.
"""

import logging
import os
import shutil
from datetime import datetime, timedelta, timezone

import requests
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import TestBehavior
from cosmos.profiles import PostgresUserPasswordProfileMapping

POSTGRES_CONN_ID = "postgres_default"
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_EXECUTABLE_PATH = (
    os.getenv("DBT_EXECUTABLE_PATH")
    or shutil.which("dbt")
    or "/home/airflow/dbt_venv/bin/dbt"
)

PROFILE_CONFIG = ProfileConfig(
    profile_name="elt_demo",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={"schema": "analytics"},
    ),
)


def _notifier(message: str) -> None:
    """Journalise l'alerte et l'envoie au webhook (Slack, Teams, Discord…) s'il est défini."""
    logging.error(message)
    if ALERT_WEBHOOK_URL:
        try:
            requests.post(ALERT_WEBHOOK_URL, json={"text": message}, timeout=10)
        except requests.RequestException:
            logging.exception("Envoi de l'alerte au webhook impossible")


def alerte_echec_run(context) -> None:
    """Appelée une fois par run en échec (retries épuisés ou dagrun_timeout dépassé)."""
    dag_run = context["dag_run"]
    _notifier(f"❌ DAG {dag_run.dag_id} en échec (run {dag_run.run_id})")


def alerte_retry(context) -> None:
    """Appelée à chaque nouvelle tentative : signale une source ou une base instable."""
    ti = context["ti"]
    _notifier(
        f"⚠️ {ti.dag_id}.{ti.task_id} relancée (tentative {ti.try_number}) : "
        f"{context.get('exception')}"
    )


with DAG(
    dag_id="elt_dbt",
    description="Collecte vers Postgres, transformations dbt via Cosmos, puis contrôles qualité.",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    on_failure_callback=alerte_echec_run,
    params={"simuler_panne": False},
    default_args={
        "owner": "data",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=20),
        "execution_timeout": timedelta(minutes=15),
        "on_retry_callback": alerte_retry,
    },
    tags=["elt", "dbt", "cosmos"],
) as dag:

    @task()
    def collecte(params=None, data_interval_start=None) -> int:
        """Simule l'extraction d'une source et charge la tranche du jour dans raw.persons."""
        if params["simuler_panne"]:
            raise ConnectionError("Source indisponible (panne simulée)")

        load_date = data_interval_start.date()
        rows = [
            (1, "Alice", 85),
            (2, "Bob", 92),
            (3, "Charlie", 78),
            (4, "Diana", 95),
            (5, "Eve", 88),
        ]

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("create schema if not exists raw")
            cur.execute(
                """
                create table if not exists raw.persons (
                    id integer not null,
                    name varchar(255),
                    score integer,
                    load_date date not null,
                    loaded_at timestamptz not null default now()
                )
                """
            )
            # Idempotent : relancer le même run remplace la tranche au lieu de la dupliquer.
            cur.execute("delete from raw.persons where load_date = %s", (load_date,))
            cur.executemany(
                "insert into raw.persons (id, name, score, load_date) values (%s, %s, %s, %s)",
                [(*row, load_date) for row in rows],
            )
        logging.info("%s lignes chargées dans raw.persons pour %s", len(rows), load_date)
        return len(rows)

    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=PROFILE_CONFIG,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        # Les tests d'un modèle bloquent les modèles en aval s'ils échouent.
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_EACH),
    )

    controles = SQLTableCheckOperator(
        task_id="controles_persons_summary",
        conn_id=POSTGRES_CONN_ID,
        table="analytics.persons_summary",
        checks={
            "non_vide": {"check_statement": "count(*) > 0"},
            "scores_plausibles": {"check_statement": "min(min_score) >= 0 and max(max_score) <= 100"},
            "personnes_comptees": {"check_statement": "sum(person_count) > 0"},
        },
        retries=0,  # un contrôle qui échoue ne se corrige pas en relançant
    )

    collecte() >> dbt >> controles

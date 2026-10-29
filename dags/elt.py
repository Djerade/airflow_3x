"""Pipeline ELT Airflow : extraction/chargement, puis transformations dbt.

Configurer ELT_LOAD_COMMAND (commande d'extraction et de chargement),
DBT_PROJECT_DIR et DBT_PROFILES_DIR dans l'environnement des workers Airflow.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
	dag_id="elt_dbt",
	description="Charge les données dans l'entrepôt puis exécute dbt.",
	start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
	schedule="@daily",
	catchup=False,
	default_args={
		"owner": "data",
		"retries": 2,
		"retry_delay": timedelta(minutes=5),
	},
	tags=["elt", "dbt"],
) as dag:
	extract_load = BashOperator(
		task_id="extract_load",
		bash_command='${ELT_LOAD_COMMAND:?Définissez ELT_LOAD_COMMAND sur le worker Airflow}',
	)

	dbt_build = BashOperator(
		task_id="dbt_build",
		bash_command=(
			'dbt build '
			'--project-dir "${DBT_PROJECT_DIR:?Définissez DBT_PROJECT_DIR}" '
			'--profiles-dir "${DBT_PROFILES_DIR:?Définissez DBT_PROFILES_DIR}"'
		),
	)

	extract_load >> dbt_build

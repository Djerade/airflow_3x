"""DAG avec branchement conditionnel — Airflow 3."""

from datetime import datetime, timedelta
from airflow.sdk import DAG, task
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="branching_example",
    default_args=default_args,
    description="Exemple de branchement conditionnel",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "branching"],
) as dag:

    start = EmptyOperator(task_id="start")

    @task.branch()
    def check_day() -> str:
        """Choisit la branche selon le jour de la semaine."""
        day = datetime.now().weekday()
        if day < 5:
            return "weekday_task"
        return "weekend_task"

    @task(task_id="weekday_task")
    def weekday_task():
        print("Exécution du traitement en semaine")

    @task(task_id="weekend_task")
    def weekend_task():
        print("Exécution du traitement du week-end")

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    branch = check_day()
    start >> branch >> [weekday_task(), weekend_task()] >> end

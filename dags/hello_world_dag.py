"""DAG Hello World — exemple de base pour Airflow 3."""

from datetime import datetime, timedelta
from airflow.sdk import DAG, task


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hello_world",
    default_args=default_args,
    description="Un DAG Hello World simple",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "hello"],
) as dag:

    @task()
    def say_hello():
        print("Bonjour depuis Airflow 3!")
        return "hello"

    @task()
    def say_world(greeting: str):
        print(f"Message reçu: {greeting} — World!")
        return f"{greeting} world"

    @task()
    def finish(result: str):
        print(f"DAG terminé avec succès: {result}")

    hello = say_hello()
    world = say_world(hello)
    finish(world)

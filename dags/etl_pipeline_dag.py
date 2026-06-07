"""DAG ETL Pipeline — exemple avec extract, transform, load."""

from datetime import datetime, timedelta
from airflow.sdk import DAG, task


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL basique: Extract → Transform → Load",
    schedule="0 6 * * *",  # Tous les jours à 6h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "pipeline"],
) as dag:

    @task()
    def extract() -> list[dict]:
        """Simule l'extraction de données depuis une source."""
        print("Extraction des données...")
        data = [
            {"id": 1, "name": "Alice", "score": 85},
            {"id": 2, "name": "Bob", "score": 92},
            {"id": 3, "name": "Charlie", "score": 78},
        ]
        print(f"{len(data)} enregistrements extraits")
        return data

    @task()
    def transform(raw_data: list[dict]) -> list[dict]:
        """Transforme et nettoie les données."""
        print("Transformation des données...")
        transformed = []
        for record in raw_data:
            transformed.append({
                **record,
                "name": record["name"].upper(),
                "grade": "A" if record["score"] >= 90 else "B" if record["score"] >= 80 else "C",
                "processed_at": datetime.now().isoformat(),
            })
        print(f"{len(transformed)} enregistrements transformés")
        return transformed

    @task()
    def load(processed_data: list[dict]) -> str:
        """Simule le chargement des données vers une destination."""
        print("Chargement des données...")
        for record in processed_data:
            print(f"  -> Chargé: {record['name']} | Score: {record['score']} | Grade: {record['grade']}")
        summary = f"{len(processed_data)} enregistrements chargés avec succès"
        print(summary)
        return summary

    raw = extract()
    cleaned = transform(raw)
    load(cleaned)

"""Collecte ERA5-Land et CHIRPS v3.0 vers MinIO (couche bronze), avec reprise automatique et alertes.

- collecte_era5_land : NetCDF horaire du jour J-7 (ERA5-Land est publié avec ~5 jours de retard).
- collecte_chirps : GeoTIFF journalier du jour J-60 (CHIRPS "final" est publié avec ~1 à 2 mois de retard).

Les fichiers sont déposés dans s3://bronze/<source>/year=AAAA/month=MM/ ; relancer un run
réécrit les mêmes clés, sans doublon.

Connexions/variables : connexion Airflow `minio_default` (type aws, endpoint_url dans extra),
CDSAPI_URL et CDSAPI_KEY pour ERA5-Land, ALERT_WEBHOOK_URL optionnelle.
Déclencher avec {"bbox": [ouest, sud, est, nord]} pour changer d'emprise.
"""

import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import DAG, task, task_group

from include.climate_download import download_chirps, download_era5_land

MINIO_CONN_ID = "minio_default"
BRONZE_BUCKET = "bronze"
ERA5_LAND_DELAI = timedelta(days=7)
CHIRPS_DELAI = timedelta(days=60)
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")


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


def _jour_cible(data_interval_start, dag_run, delai: timedelta):
    """Jour à télécharger : date du run moins le délai de publication de la source.

    Un run manuel sans date logique n'a pas de data_interval_start : on prend alors run_after.
    """
    reference = data_interval_start or dag_run.run_after
    return (reference - delai).date()


def _deposer_dans_minio(fichiers: list[Path], source: str, jour) -> list[str]:
    """Envoie les fichiers dans s3://bronze/<source>/year=AAAA/month=MM/ et renvoie leurs URI."""
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    if not hook.check_for_bucket(BRONZE_BUCKET):
        hook.create_bucket(bucket_name=BRONZE_BUCKET)

    uris = []
    for fichier in fichiers:
        cle = f"{source}/year={jour:%Y}/month={jour:%m}/{fichier.name}"
        hook.load_file(filename=str(fichier), key=cle, bucket_name=BRONZE_BUCKET, replace=True)
        uris.append(f"s3://{BRONZE_BUCKET}/{cle}")
        logging.info("Déposé : s3://%s/%s", BRONZE_BUCKET, cle)
    return uris


with DAG(
    dag_id="collecte_climat",
    description="Télécharge ERA5-Land et CHIRPS v3.0 et les dépose bruts dans MinIO (bronze).",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    on_failure_callback=alerte_echec_run,
    params={"bbox": [13.4, 7.4, 24.0, 23.5]},  # ouest, sud, est, nord : à adapter
    default_args={
        "owner": "data",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(minutes=30),
        "on_retry_callback": alerte_retry,
    },
    tags=["climat", "era5-land", "chirps", "minio"],
) as dag:
    
    @task_group(group_id="collecte_era5_land")
    def collecte_era5_land():
        @task(execution_timeout=timedelta(hours=3))
        def collecte_era5_land(params=None, data_interval_start=None, dag_run=None) -> list[str]:
            """Télécharge ERA5-Land horaire du jour J-7 et le dépose dans MinIO."""
            jour = _jour_cible(data_interval_start, dag_run, ERA5_LAND_DELAI)
            with tempfile.TemporaryDirectory() as tmp:
                fichiers = download_era5_land(jour, jour, tuple(params["bbox"]), Path(tmp))
                return _deposer_dans_minio(fichiers, "era5_land", jour)

        @task(execution_timeout=timedelta(hours=3))
        def collecte_chirps(params=None, data_interval_start=None, dag_run=None) -> list[str]:
            """Télécharge CHIRPS v3.0 journalier du jour J-60 et le dépose dans MinIO."""
            jour = _jour_cible(data_interval_start, dag_run, CHIRPS_DELAI)
            with tempfile.TemporaryDirectory() as tmp:
                fichiers = download_chirps(jour, jour, tuple(params["bbox"]), Path(tmp))
                if not fichiers:
                    raise ValueError(f"CHIRPS {jour} pas encore publié")
                return _deposer_dans_minio(fichiers, "chirps_v3_rnl", jour)


    # Indépendantes : une source en panne ne bloque pas l'autre.
    collecte_era5_land()

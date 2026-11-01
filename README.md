# Mini Projet Apache Airflow 3

## Structure du projet

```
airflow-3x/
├── dags/
│   ├── etl_pipeline_dag.py     # Pipeline ETL (Extract → Transform → Load)
│   └── elt.py                  # Collecte → dbt (Cosmos) → contrôles
├── dbt/                        # Projet dbt (models, sources, tests)
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/sources.yml       # source raw.persons
│       ├── staging/stg_persons.sql
│       └── marts/persons_summary.sql
├── plugins/                    # Plugins personnalisés
├── logs/                       # Logs générés automatiquement
├── config/
│   └── airflow.cfg             # Configuration Airflow
├── docker-compose.yml          # Stack Docker complète
├── Dockerfile                  # Image Airflow + dbt + Cosmos
├── requirements.txt            # Dépendances Python
└── .env                        # Variables d'environnement
```

## Démarrage rapide

### Avec Docker (recommandé)

```bash
# 1. Initialiser et démarrer
docker-compose up -d

# 2. Accéder à l'interface web
# URL: http://localhost:8081
# Login: admin / Password: admin
```

Le DAG `elt_dbt` charge `raw.persons` puis écrit dans le schéma `analytics` via
la connexion Airflow `postgres_default`. Le conteneur tourne avec `AIRFLOW_UID` (50000 par défaut) :
si dbt ne peut pas écrire ses artefacts, faire `sudo chown -R 50000:0 dbt`.

### Sans Docker (environnement local)

```bash
# 1. Installer les dépendances
pip install -r requirements.txt

# 2. Initialiser la base de données
airflow db migrate

# 3. Créer un utilisateur admin
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# 4. Démarrer le scheduler (terminal 1)
airflow scheduler

# 5. Démarrer le webserver (terminal 2)
airflow webserver --port 8080
```

## DAGs disponibles

| DAG | Schedule | Description |
|-----|----------|-------------|
| `etl_pipeline` | 6h daily | Pipeline ETL complet (en mémoire) |
| `elt_dbt` | Daily | Collecte → dbt via Cosmos → contrôles qualité |

## Pipeline `elt_dbt` : collecte, dbt et contrôles

```
collecte ──► dbt : stg_persons ─► tests ─► persons_summary ─► tests ──► controles_persons_summary
```

- **Collecte** : charge la tranche du jour dans `raw.persons`. Idempotente
  (DELETE puis INSERT sur `load_date`), donc relançable sans doublons.
- **dbt** (`DbtTaskGroup` Cosmos) : une tâche par modèle, tests exécutés juste
  après chaque modèle (`TestBehavior.AFTER_EACH`) : un test en échec bloque l'aval.
- **Contrôles** : `SQLTableCheckOperator` sur `analytics.persons_summary`.
- **Reprise automatique** : 2 retries avec backoff exponentiel (1 → 20 min max),
  `execution_timeout` 15 min, `dagrun_timeout` 1 h, `max_active_runs=1`.
  Les contrôles finaux ne sont pas relancés (`retries=0`).
- **Alertes** : `on_retry_callback` à chaque nouvelle tentative, `on_failure_callback`
  une fois par run en échec. Journalisées, et envoyées en JSON `{"text": ...}` à
  `ALERT_WEBHOOK_URL` si la variable est définie (Slack, Teams, Discord…).

Profil dbt généré par Cosmos depuis la connexion `postgres_default` (aucun
`profiles.yml` dans le repo). dbt est installé dans un virtualenv séparé
(`/home/airflow/dbt_venv`) car il exige `protobuf>=6`, incompatible avec les
contraintes d'Airflow 3.0.

```bash
# Reconstruire l'image après modification des dépendances
docker compose build

# Déclencher le pipeline
docker compose exec airflow-scheduler airflow dags trigger elt_dbt

# Tester retries et alertes avec une panne simulée de la source
docker compose exec airflow-scheduler airflow dags trigger elt_dbt -c '{"simuler_panne": true}'
```

## Nouveautés Airflow 3

- **`airflow.sdk`** : Nouveau SDK unifié (remplace `airflow.decorators`)
- **`airflow db migrate`** : Remplace `airflow db init`
- **Task SDK** : API TaskFlow améliorée
- **Interface Grid** : Vue par défaut modernisée

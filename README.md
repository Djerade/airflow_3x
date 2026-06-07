# Mini Projet Apache Airflow 3

## Structure du projet

```
airflow-3x/
├── dags/
│   ├── hello_world_dag.py      # DAG basique Hello World
│   ├── etl_pipeline_dag.py     # Pipeline ETL (Extract → Transform → Load)
│   └── branching_dag.py        # DAG avec branchement conditionnel
├── plugins/                    # Plugins personnalisés
├── logs/                       # Logs générés automatiquement
├── config/
│   └── airflow.cfg             # Configuration Airflow
├── docker-compose.yml          # Stack Docker complète
├── requirements.txt            # Dépendances Python
└── .env                        # Variables d'environnement
```

## Démarrage rapide

### Avec Docker (recommandé)

```bash
# 1. Initialiser et démarrer
docker-compose up -d

# 2. Accéder à l'interface web
# URL: http://localhost:8080
# Login: admin / Password: admin
```

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
| `hello_world` | Daily | Exemple basique avec TaskFlow API |
| `etl_pipeline` | 6h daily | Pipeline ETL complet |
| `branching_example` | Weekly | Branchement conditionnel |

## Nouveautés Airflow 3

- **`airflow.sdk`** : Nouveau SDK unifié (remplace `airflow.decorators`)
- **`airflow db migrate`** : Remplace `airflow db init`
- **Task SDK** : API TaskFlow améliorée
- **Interface Grid** : Vue par défaut modernisée

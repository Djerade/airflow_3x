# syntax=docker/dockerfile:1.7
FROM apache/airflow:3.0.0

ARG AIRFLOW_VERSION=3.0.0
ARG PYTHON_VERSION=3.12
ARG DBT_POSTGRES_VERSION=1.11.0

USER airflow

COPY requirements.txt /requirements.txt
RUN --mount=type=cache,target=/home/airflow/.cache/pip,uid=50000,gid=0,sharing=locked \
    pip install \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      -r /requirements.txt

# dbt dans son propre virtualenv : il exige protobuf>=6, incompatible avec les
# contraintes Airflow 3.0 (protobuf==4.25.6). Cosmos l'appelle via DBT_EXECUTABLE_PATH.
RUN --mount=type=cache,target=/home/airflow/.cache/pip,uid=50000,gid=0,sharing=locked \
    python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install "dbt-postgres==${DBT_POSTGRES_VERSION}"

ENV PATH="/home/airflow/.local/bin:${PATH}"

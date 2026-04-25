FROM apache/airflow:2.11.0-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv

USER airflow
WORKDIR /opt/airflow

COPY --chown=airflow:root pyproject.toml uv.lock ./

RUN uv export --no-hashes --no-dev --format requirements-txt \
        --no-emit-package apache-airflow \
        --no-emit-package apache-airflow-providers-postgres \
        > /tmp/requirements.txt \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

ENV PYTHONPATH=/opt/airflow/src:${PYTHONPATH}

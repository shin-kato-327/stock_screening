"""SQLAlchemy engine factory.

Inside Airflow, prefer the PostgresHook (uses Connection ID
'financial_data' so secrets stay in Airflow's metadata DB). Outside
Airflow, build the URL from POSTGRES_* env vars.
"""

from __future__ import annotations

import os

from sqlalchemy.engine import Engine, create_engine

CONN_ID = "financial_data"


def get_engine() -> Engine:
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

        return PostgresHook(postgres_conn_id=CONN_ID).get_sqlalchemy_engine()
    except Exception:
        pass

    user = os.environ["POSTGRES_USER"]
    pw = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5433")
    db = os.environ.get("POSTGRES_DB", "financial_data")
    return create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")

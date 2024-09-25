from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import glob

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'ingest_docidlist_dag',
    default_args=default_args,
    description='A simple DAG to insert data into PostgreSQL',
    schedule_interval=timedelta(days=1),
)

def insert_data(**kwargs):
    #pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    #conn = pg_hook.get_conn()
    #cursor = conn.cursor()

    engine = 'postgresql+psycopg2://root:root@pgdatabase:5432/financial_data'
    execution_date = kwargs['execution_date']
    target_date = execution_date.date() - timedelta(days=1)

    file_path = f'/opt/airflow/content/documentlist/{target_date}_documentlist.csv'
    print(f"loading the file: {file_path}")
    df = pd.read_csv(file_path)
    df.to_sql('t_doc_list', con=engine, if_exists='append', index=False)
    # Move the file to the destination directory
    #shutil.move(file_path, os.path.join(ingested_CSV_path, os.path.basename(file_path)))
    
    #cursor.execute("INSERT INTO t_test (key1, value1) VALUES (%s, %s)", (1, 'Sample Value'))
    
    #conn.commit()
    #cursor.close()
    #conn.close()

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag
)

insert_data_task
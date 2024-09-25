from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import time
import json
import os
import re
from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table
from bs4 import BeautifulSoup
import io
import requests
import glob
import re
import logging
import shutil
from pathlib import Path
import yfinance as yf
import pendulum
import numpy as np
from datetime import datetime, timedelta

local_tz = pendulum.timezone("America/Los_Angeles")  # PST

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 19, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'daily_stock_perf_dag',
    default_args=default_args,
    description='It runs daily to get stock performance of all the stocks listed in the Japan Stock Exchange',
    schedule_interval='0 16 * * *'  # This is 4 PM,
)

# Define the functions
def load_parameters():
    dag_folder = os.path.dirname(__file__)
    parameters_path = os.path.join(dag_folder, '/opt/airflow/dags/parameters.json')
    with open('/opt/airflow/dags/parameters.json', 'r') as file:
        data = json.load(file)
    return data

def get_daily_stock_perf(**kwargs):
    data = load_parameters()
    #DATABASE_URI = data["DATABASE_URI"]
    mail_address = data["mailaddress"]
    password = data["password"]
    # Create an engine and metadata object
    #engine = create_engine(DATABASE_URI)
    engine = 'postgresql+psycopg2://root:root@pgdatabase:5432/financial_data'
    metadata = MetaData()
    data={"mailaddress":mail_address, "password":password}
    r_post = requests.post("https://api.jquants.com/v1/token/auth_user", data=json.dumps(data))
    rToken = r_post.json()["refreshToken"]
    REFRESH_TOKEN = rToken
    r_post = requests.post(f"https://api.jquants.com/v1/token/auth_refresh?refreshtoken={REFRESH_TOKEN}")
    idToken = r_post.json()["idToken"]
    # 上場銘柄一覧
    headers = {'Authorization': 'Bearer {}'.format(idToken)}
    r = requests.get("https://api.jquants.com/v1/listed/info", headers=headers)

    # Pandas DafaFrameへ変換
    r_dict = r.json()
    df = pd.DataFrame(r_dict['info'])
    df["MarketCodeCleansed"] = df[df["MarketCode"].isin(["0111", "0112"])]["Code"].str[:4] + ".T"
    ticker = []
    ticker = df[df["MarketCode"].isin(["0111", "0112"])]["MarketCodeCleansed"].values.tolist()

    df_master_yf = pd.DataFrame(columns=["Code", "previousClose", "trailingPE", "volume", "marketCap", "fiftyTwoWeekLow", "fiftyTwoWeekHigh", "revenuePerShare"])
    columns_yf = ["Code", "previousClose", "trailingPE", "volume", "marketCap", "fiftyTwoWeekLow", "fiftyTwoWeekHigh", "revenuePerShare"]
    listInfo = ["previousClose", "trailingPE", "volume", "marketCap", "fiftyTwoWeekLow", "fiftyTwoWeekHigh", "revenuePerShare"]

    for tk in ticker: 
        stock = yf.Ticker(tk)
        data = []
        data.append(tk)
        for info in listInfo:
            try:
                data.append(stock.info[info])
            except:
                data.append("0")     
                
        df_yf_temp = pd.DataFrame(data=[data], columns=columns_yf)
        df_master_yf = pd.concat([df_master_yf, df_yf_temp], ignore_index=True)
    #df_yf.head()

    df_master_yf["MarketCodeCleansed"] = df_master_yf["Code"]
    df_master = pd.merge(df, df_master_yf, on='MarketCodeCleansed')
    df_master = df_master.drop('Code_y', axis=1)
    df_master.dropna(subset=["trailingPE", "marketCap"], inplace=True)
    df_master["trailingPE"] = pd.to_numeric(df_master["trailingPE"], errors='coerce')
    df_master["marketCap"] = pd.to_numeric(df_master["marketCap"], errors='coerce')
    df_master.rename(columns={'Code_x': 'ShokenCode'}, inplace=True)
    today = pd.Timestamp(datetime.today().date())
    df_master["Date"] = today

    #replace infinity values with nan before inserting the DataFrame into PG
    df_master.replace([np.inf, -np.inf], np.nan, inplace=True)
    #insert data into PostgresDB
    df_master.to_sql('t_daily_stock_perf', con=engine, if_exists='append', index=False)

ingest_daily_stock_perf_task = PythonOperator(
    task_id='ingest_daily_stock_perf_task',
    python_callable=get_daily_stock_perf,
    dag=dag
)




    
    
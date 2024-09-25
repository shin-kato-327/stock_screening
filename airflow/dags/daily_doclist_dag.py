from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import zipfile
import pandas as pd
import requests
import time
import json
import os
import re
from arelle import Cntlr, ModelManager, XbrlConst
from arelle.ModelValue import qname
from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table
from bs4 import BeautifulSoup
import io
import requests
import glob
import re
import logging
import shutil
from pathlib import Path

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'daily_doclist_dag',
    default_args=default_args,
    description='A sample dag',
    schedule_interval=timedelta(days=1),
)

# Define the functions
def load_parameters():
    dag_folder = os.path.dirname(__file__)
    parameters_path = os.path.join(dag_folder, '/opt/airflow/dags/parameters.json')
    with open('/opt/airflow/dags/parameters.json', 'r') as file:
        data = json.load(file)
    return data

def make_doc_id_list(target_date, EDINET_KEY, URL):
    # (Keep the original implementation)
    securities_report_doc_list = []
    params = {"date" : target_date, "type" : 2, "Subscription-Key" : EDINET_KEY}
    try:   
        res = requests.get(URL, params = params)
        res.raise_for_status()
        json_data = res.json()
          
        if json_data['metadata']['status'] != '200':
            raise ApiResponseError('APIのステータスが200以外のレスポンスです')
             
        for num in range(len(json_data["results"])):          
            securities_report_doc_list.append([
                json_data['results'][num]['docID'],
                json_data['results'][num]['edinetCode'],
                json_data['results'][num]['secCode'],
                json_data['results'][num]['JCN'],
                json_data['results'][num]['filerName'],
                json_data['results'][num]['fundCode'],
                json_data['results'][num]['ordinanceCode'],
                json_data['results'][num]['formCode'],
                json_data['results'][num]['docTypeCode'],
                json_data['results'][num]['periodStart'],
                json_data['results'][num]['periodEnd'],
                json_data['results'][num]['submitDateTime'],
                json_data['results'][num]['docDescription'],
                json_data['results'][num]['issuerEdinetCode'],
                json_data['results'][num]['subjectEdinetCode'],
                json_data['results'][num]['currentReportReason'],
                json_data['results'][num]['parentDocID'],
                json_data['results'][num]['opeDateTime'],
                json_data['results'][num]['xbrlFlag'],
                json_data['results'][num]['pdfFlag'],
                json_data['results'][num]['csvFlag']
            ])
        return securities_report_doc_list
        
    except RequestException as e:
        print("request failed. error=(%s)", e.response.text)
        return securities_report_doc_list
        
    except ApiResponseError as e:
        print(e)
        return securities_report_doc_list    
    pass

def process_sample_list(**kwargs):
    data = load_parameters()
    EDINET_KEY = data["EDINET_KEY"]
    URL = "https://disclosure.edinet-fsa.go.jp/api/v2/documents.json"

    execution_date = kwargs['execution_date']
    target_date = execution_date.date() - timedelta(days=1)  # Get documents from yesterday

    securities_report_doc_list = make_doc_id_list(target_date, EDINET_KEY, URL)

    columns = ["docID", "edinetCode", "secCode", "JCN", "filerName", "fundCode", "ordinanceCode", "formCode", "docTypeCode", "periodStart", "periodEnd", "submitDateTime", "docDescription", "issuerEdinetCode", "subjectEdinetCode", "currentReportReason", "parentDocID", "opeDateTime", "xbrlFlag", "pdfFlag", "csvFlag"]

    df = pd.DataFrame(data=securities_report_doc_list, columns=columns)
    filtered_df = df[(df["formCode"]=="030000") & (df["ordinanceCode"]=="010")]

    # Save to CSV
    csv_filename = f'/opt/airflow/content/documentlist/{target_date}_documentlist.csv'
    filtered_df.to_csv(csv_filename, index=False)

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

def get_xsd(**kwargs):

    data = load_parameters()
    EDINET_KEY = data["EDINET_KEY"]
    URL = "https://disclosure.edinet-fsa.go.jp/api/v2/documents.json"

    execution_date = kwargs['execution_date']
    target_date = execution_date.date() - timedelta(days=1)  # Get documents from yesterday

    securities_report_doc_list = make_doc_id_list(target_date, EDINET_KEY, URL)

    columns = ["docID", "edinetCode", "secCode", "JCN", "filerName", "fundCode", "ordinanceCode", "formCode", "docTypeCode", "periodStart", "periodEnd", "submitDateTime", "docDescription", "issuerEdinetCode", "subjectEdinetCode", "currentReportReason", "parentDocID", "opeDateTime", "xbrlFlag", "pdfFlag", "csvFlag"]

    df = pd.DataFrame(data=securities_report_doc_list, columns=columns)
    filtered_df = df[(df["formCode"]=="030000") & (df["ordinanceCode"]=="010")]
    
    docs = []
    docs = filtered_df["docID"].tolist()
    n_docs = len(docs)
    print(f"This is a log message: {n_docs} documents")
    if len(docs) > 0: 
        for docid in docs:
            # 書類取得APIのエンドポイント
            url = "https://api.edinet-fsa.go.jp/api/v2/documents/" + docid
            print(f"#### xsd file URL ####: {url}")
            time.sleep(5)

            #書類取得APIのパラメータ
            params ={"type":1,"Subscription-Key":EDINET_KEY}
            res = requests.get(url,params=params, verify=False)
            print(f"#### API response code ####: {res}")
            #ファイルへの出力
    #            print(res.status_code)
            if res.status_code == 200:
            # レスポンスからZIPファイルを読み込む
                print("#### unzipping files ####")
                with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                # ZIPファイル内のすべてのファイルをループ処理
                    for file in z.namelist():
                        if file.startswith("XBRL/PublicDoc/") and (file.endswith(".xsd") or file.endswith(".xbrl")):
                        # .xbrlもしくは.xsdファイルを見つけたら、それをディスクに書き込む
                            z.extract(file, path=f'/opt/airflow/content/xsd/{target_date}/{docid}/')

def ingest_xsd(**kwargs):
    CONSOLIDATED_OR_NONCONSOLIDATED_COL = "連結/個別"
    execution_date = kwargs['execution_date']
    target_date = execution_date.date() - timedelta(days=1)  # Get documents from yesterday
    pattern = r'/xsd/([^/]+)/'
    data = load_parameters()
    DATABASE_URI = 'postgresql+psycopg2://root:root@pgdatabase:5432/financial_data'

    engine = create_engine(DATABASE_URI)
    metadata = MetaData()

    query = """
    select distinct "docID" from t_financials;
    """

    df_docid = pd.read_sql(query, engine)
    ingested_docids = []
    ingested_docids = df_docid.docID.values

    xbrl_file_dirs = list()
    print('XBRL parsing start')
    for xbrl_file_dir in glob.glob(f'/opt/airflow/content/xsd/{target_date}/**/**/**/*.xbrl'):
        print(xbrl_file_dir)
    #    extracted_string = re.search(pattern, xbrl_file_dir).group(1)
    #    extracted_string = [Path(path).parents[2].name for path in xbrl_file_dir][0]
        path_obj = Path(xbrl_file_dir)
        extracted_string = path_obj.parents[2].name
        
        if extracted_string not in ingested_docids:
            xbrl_file_dirs.append([extracted_string, xbrl_file_dir])
    
    fact_datas = list()

    for docID, xbrl_file in xbrl_file_dirs:
        print(f'{docID} and {xbrl_file}')
        #ctrl = Cntlr.Cntlr(logFileName='logToPrint')
        ctrl = Cntlr.Cntlr()
        model_xbrl = ctrl.modelManager.load(xbrl_file)
        #fact_datas = list()

        for fact in model_xbrl.facts:
            print('in facts')

            if fact.unit is not None and str(fact.unit.value) == 'JPY':
                label_ja = fact.concept.label(preferredLabel=None, lang='ja', linkroleHint=None)             
                x_value = fact.xValue

                if fact.context.startDatetime:
                    start_date = fact.context.startDatetime
                else:
                    start_date = None
                if fact.context.endDatetime:
                    end_date = fact.context.endDatetime
                else:
                    end_date = None

                fact_datas.append([
                docID,
                label_ja,
                x_value,
                start_date,
                end_date,
                fact.contextID,
                ])
                try:
                    shutil.rmtree(f'/opt/airflow/content/xsd/{docID}')
                except:
                    continue
            else:
                continue
        print(len(fact_datas))
    try:
        df = pd.DataFrame(fact_datas, columns=['docID','itemName', 'amount', 'periodStart', 'periodEnd', 'categoryID'] )
        df_d = df[df['categoryID'] == 'CurrentYearDuration']
        df_i = df[df['categoryID'] == 'CurrentYearInstant']
        df_m = pd.concat([df_d, df_i])
        df_m.to_sql('t_financials', con=engine, if_exists='append', index=False) #somehow it errors
        print('writing data to PG complete')
        #delete XSD folders
        shutil.rmtree(f'/opt/airflow/content/xsd/{target_date}')
    except:
        print("An exception occurred")

# Create the task
get_document_list_task = PythonOperator(
    task_id='process_sample_list',
    python_callable=process_sample_list,
    provide_context=True,
    dag=dag,
)

insert_document_list_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag
)

get_xsd_task = PythonOperator(
    task_id='get_xsd',
    python_callable=get_xsd,
    dag=dag
)

ingest_xsd_task = PythonOperator(
    task_id='ingest_xsd',
    python_callable=ingest_xsd,
    dag=dag
)

# Set the task dependencies (in this case, we only have one task)
# process_sample_list_task >> insert_data_task >> get_xsd_task 
get_document_list_task >> insert_document_list_task >> get_xsd_task >> ingest_xsd_task
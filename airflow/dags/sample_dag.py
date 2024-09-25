from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import zipfile
import pandas as pd
import requests
import time
import json
import os
from sqlalchemy import create_engine

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'sample_dag',
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

# Create the task
process_sample_list_task = PythonOperator(
    task_id='process_sample_list',
    python_callable=process_sample_list,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies (in this case, we only have one task)
process_sample_list_task
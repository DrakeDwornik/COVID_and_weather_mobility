from airflow import DAG
from airflow.operators.python import PythonOperator
time import datetime

with DAG("daily_summaries", start_date=datetime(2021,11,1), schedule_interval="@yearly", catchup=False) as dag:
    capture_data_to_disk
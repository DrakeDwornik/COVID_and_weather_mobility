from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import sys

sys.path.append("/Users/dwornikdrake/dev/airflow/include")

from airflow_project_funcs.ingest_functions4 import  load_the_graphs,  graph_difference, \
    load_mobility_data, dl_weather_data_ncc, dl_weather_data_dc, load_weather_data_ncc, \
    load_weather_data_dc, convert_data_to_weekly


def start_load_mobility_data():
    load_mobility_data()


def start_dl_weather_data_ncc():
    dl_weather_data_ncc()


def start_dl_weather_data_dc():
    dl_weather_data_dc()


def start_load_weather_data_ncc():
    load_weather_data_ncc()


def start_load_weather_data_dc():
    load_weather_data_dc()


def start_convert_data_to_weekly():
    convert_data_to_weekly()


def start_do_the_graphs():
    load_the_graphs()


with DAG("mobility_weather", start_date=datetime(2021, 1, 1), schedule_interval="0 4 * * *", catchup=False) as dag:
    load_mobility = PythonOperator(task_id="load_mobility", python_callable=start_load_mobility_data)
    dl_weather_ncc = PythonOperator(task_id="dl_weather_ncc", python_callable=start_dl_weather_data_ncc, retries=5, retry_delay=timedelta(minutes=7))
    dl_weather_dc = PythonOperator(task_id="dl_weather_dc", python_callable=start_dl_weather_data_dc, retries=5, retry_delay=timedelta(minutes=11))

    load_weather_ncc = PythonOperator(task_id="load_weather_ncc", python_callable=start_load_weather_data_ncc)
    load_weather_dc = PythonOperator(task_id="load_weather_dc", python_callable=start_load_weather_data_dc)

    convert_to_weekly = PythonOperator(task_id="convert_to_weekly", python_callable=start_convert_data_to_weekly)

    do_the_graphs4 = PythonOperator(task_id="do_the_graphs", python_callable=start_do_the_graphs)

    dl_weather_ncc >> load_weather_ncc
    dl_weather_dc >> load_weather_dc
    [load_weather_ncc, load_mobility, load_weather_dc] >> convert_to_weekly >> do_the_graphs4

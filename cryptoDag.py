from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import scriptFinal

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_crypto',
    default_args=default_args,
    description='DAG para ejecutar mi script de criptomonedas',
    schedule_interval=timedelta(days=1),
)

def wrapper_get_api_data():
    data = scriptFinal.get_api_data()
    return data

def wrapper_process_data(ti):
    datos_seleccionados = ti.xcom_pull(task_ids='get_api_data')
    df = scriptFinal.process_data(datos_seleccionados)
    return df

def wrapper_insert_into_db(ti):
    df = ti.xcom_pull(task_ids='process_data')
    scriptFinal.insert_into_db(df)

def wrapper_check_btc_alert(ti):
    df = ti.xcom_pull(task_ids='process_data')
    scriptFinal.check_btc_alert(df)

get_api_data = PythonOperator(
    task_id='get_api_data',
    python_callable=wrapper_get_api_data,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=wrapper_process_data,
    dag=dag,
)

insert_into_db = PythonOperator(
    task_id='insert_into_db',
    python_callable=wrapper_insert_into_db,
    dag=dag,
)

check_btc_alert = PythonOperator(
    task_id='check_btc_alert',
    python_callable=wrapper_check_btc_alert,
    dag=dag,
)

get_api_data >> process_data >> insert_into_db >> check_btc_alert

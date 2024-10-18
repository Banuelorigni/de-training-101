import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from extract_source_file import extract_and_upload_data
from transfor_data import transform_data_for_failure_count
from load_data import save_to_csv_and_upload_to_s3
from utils.spark_util import spark


default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'yearly_drive_summary',
    default_args=default_args,
    description='Yearly Drive Summary',
    schedule=None,
    start_date=datetime.datetime(2024, 10, 14),
)

year=2019

def extract_data():
    extract_and_upload_data(2019, "Q1", 2023, "Q3")


def transform_data():
    transform_data_for_failure_count(year)


def load_data():
    save_to_csv_and_upload_to_s3(spark, f"drivefailurecount/yearly/{year}")


def stop_spark():
    spark.stop()


extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)


stop_spark_task = PythonOperator(
    task_id='stop_spark',
    python_callable=stop_spark,
    dag=dag,
)

extract_task >> transform_task >> load_task >> stop_spark_task

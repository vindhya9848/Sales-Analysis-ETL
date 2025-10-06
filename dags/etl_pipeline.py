from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
from gold.execute_sql_scripts_with_date import execute_sql_scripts_with_date
from bronze.load_data_from_source import load_data_from_source
from silver.load_data_from_staging_to_cleaned_tables import load_data_from_staging_to_cleaned_tables

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 24),
    'retries': 1  # can adjust number of retries as needed if the file is not found or if the task fails
}

def get_file_and_date(**context):
    today = datetime.today().strftime('%Y%m%d')
    filename = f"Data_for_programming_{today}.xlsx"
    file_path = Path(f"./{filename}")
    
    if not file_path.exists():
        raise FileNotFoundError(f"{filename} not found in current directory.")
    
    target_date = datetime.strptime(today, "%Y%m%d").date()
    
    # Pass to downstream tasks
    context['ti'].xcom_push(key='file_path', value=str(file_path))
    context['ti'].xcom_push(key='target_date', value=str(target_date))

def run_bronze(**context):
    file_path = context['ti'].xcom_pull(key='file_path')
    target_date = context['ti'].xcom_pull(key='target_date')
    load_data_from_source(file_path, datetime.fromisoformat(target_date).date())

def run_silver(**context):
    target_date = context['ti'].xcom_pull(key='target_date')
    load_data_from_staging_to_cleaned_tables(datetime.fromisoformat(target_date).date())

def run_gold_metrics(**context):
    target_date = context['ti'].xcom_pull(key='target_date')
    target_date = datetime.fromisoformat(target_date).date()

    sql_dir = Path("./metrics_and_views")
    sql_files = [
        "Q3_sales_country_name_table.sql",
        "Q4_metric_table.sql",
        "Q5_track_countries_view.sql",
        "Q6_sales_by_product_code_view.sql"
    ]
    
    execute_sql_scripts_with_date(sql_files, sql_dir, target_date)
    
    

with DAG(
    dag_id="etl_pipeline_with_dynamic_date",
    default_args=default_args,
    schedule_interval=None,  # we can set it as @daily if we want to run it daily
    catchup=False,
    tags=["etl"],
) as dag:

    check_file_and_get_date = PythonOperator(
        task_id='get_file_and_date',
        python_callable=get_file_and_date,
        provide_context=True
    )

    bronze_task = PythonOperator(
        task_id='load_bronze',
        python_callable=run_bronze,
        provide_context=True
    )

    silver_task = PythonOperator(
        task_id='load_silver',
        python_callable=run_silver,
        provide_context=True
    )

    gold_task =PythonOperator(
    task_id='run_metrics_sql',
    python_callable=run_gold_metrics,
    provide_context=True,
)


    check_file_and_get_date >> bronze_task >> silver_task >> gold_task

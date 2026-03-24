from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "etl_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def validate_data(**kwargs):
    data = [
        {"order_id": "1001", "customer_id": "C01", "price": "75000", "order_date": "2024-01-15"},
        {"order_id": "1002", "customer_id": "C02", "price": "25000", "order_date": "2024-01-16"},
    ]
    assert len(data) > 0
    kwargs["ti"].xcom_push(key="row_count", value=len(data))

def load_to_staging(**kwargs):
    row_count = kwargs["ti"].xcom_pull(key="row_count", task_ids="validate_raw_data")
    print(f"Loading {row_count} rows")

def spark_transform(**kwargs):
    print("Running PySpark transformation")

def load_fact_table(**kwargs):
    print("Loading fact table")

def run_analytics(**kwargs):
    print("Running analytics")

with DAG(
    dag_id="sales_etl_pipeline",
    default_args=default_args,
    description="ETL Pipeline",
    schedule="@daily",   # ✅ FIXED
    start_date=datetime(2024, 1, 1),  # ✅ FIXED
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="validate_raw_data", python_callable=validate_data)
    t2 = PythonOperator(task_id="load_to_staging", python_callable=load_to_staging)
    t3 = PythonOperator(task_id="spark_transform", python_callable=spark_transform)
    t4 = PythonOperator(task_id="load_fact_table", python_callable=load_fact_table)
    t5 = PythonOperator(task_id="run_analytics_report", python_callable=run_analytics)

    t1 >> t2 >> t3 >> t4 >> t5

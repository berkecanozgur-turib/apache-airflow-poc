from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests

# Function to call REST API
def call_rest_api(endpoint):
    response = requests.get(endpoint)
    print(f"Called {endpoint}, Status Code: {response.status_code}, Response: {response.text}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'rest_service_dag',
    default_args=default_args,
    description='A DAG to call REST services',
    schedule_interval=timedelta(minutes=10),  # Adjust the schedule as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    # Example task to call a REST API
    call_api_task = PythonOperator(
        task_id='call_rest_api',
        python_callable=call_rest_api,
        op_args=['http://example.com/api'],  # Replace with your endpoint
    )
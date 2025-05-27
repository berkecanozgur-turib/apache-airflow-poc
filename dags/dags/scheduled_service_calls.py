from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Function to fetch endpoints from the Spring Boot API
def fetch_endpoints():
    response = requests.get("http://turib-airflow-scheduler:8081/api/endpoints")
    response.raise_for_status()
    endpoints = response.json()
    return [endpoint['url'] for endpoint in endpoints]

# Function to call a REST API
def call_rest_api(endpoint):
    try:
        response = requests.get(endpoint)
        logger.info(f"Called {endpoint}, Status Code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logger.error(f"Failed to call {endpoint}: {e}")

# Function to dynamically call all endpoints
def call_all_endpoints():
    endpoints = fetch_endpoints()
    for endpoint in endpoints:
        call_rest_api(endpoint)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'scheduled_service_calls',
    default_args=default_args,
    description='A DAG to call REST services stored in the database',
    schedule_interval=timedelta(seconds=30),  # Run every 30 seconds
    start_date=datetime.now(),
    catchup=False,
) as dag:
    call_endpoints_task = PythonOperator(
        task_id='call_all_endpoints',
        python_callable=call_all_endpoints,
    )
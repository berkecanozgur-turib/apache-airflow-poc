from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import random
import time

# Default arguments
default_args = {
    'owner': 'airflow-demo',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

# DAG definition - runs every 30 seconds
dag = DAG(
    'simple_demo_dag',
    default_args=default_args,
    description='Simple Airflow Demo - Runs Every 30 Seconds',
    schedule_interval=timedelta(seconds=30),  # Every 30 seconds
    catchup=False,
    max_active_runs=1,
    tags=['demo', 'simple', '30sec']
)

# Python functions
def generate_data(**context):
    """Simple data generation"""
    number = random.randint(1, 100)
    print(f"🎲 Generated number: {number}")
    
    # Send data to next task via XCom
    context['task_instance'].xcom_push(key='generated_number', value=number)
    return number

def process_data(**context):
    """Process the data"""
    # Get data from XCom
    number = context['task_instance'].xcom_pull(task_ids='generate_data_task', key='generated_number')
    
    print(f"📊 Received number: {number}")
    
    # Simple operations
    squared = number ** 2
    tripled = number * 3
    
    print(f"   🔢 Squared: {squared}")
    print(f"   🔢 Tripled: {tripled}")
    
    # Save result to XCom
    result = {
        'original': number,
        'squared': squared,
        'tripled': tripled
    }
    
    context['task_instance'].xcom_push(key='processing_result', value=result)
    return result

def show_results(**context):
    """Display processing results"""
    result = context['task_instance'].xcom_pull(task_ids='process_data_task', key='processing_result')
    
    print("📋 RESULTS REPORT")
    print("=================")
    print(f"🎯 Original number: {result['original']}")
    print(f"⬜ Squared: {result['squared']}")
    print(f"🔢 Tripled: {result['tripled']}")
    
    # Simple decision making
    if result['original'] > 50:
        status = "HIGH"
        emoji = "🔴"
    else:
        status = "LOW"
        emoji = "🟢"
    
    print(f"{emoji} Status: {status}")
    return f"Report completed - {status}"

# Create tasks

# Start
start = DummyOperator(
    task_id='start',
    dag=dag
)

# Data generation
generate_data_task = PythonOperator(
    task_id='generate_data_task',
    python_callable=generate_data,
    dag=dag
)

# System check (Bash)
system_check = BashOperator(
    task_id='system_check',
    bash_command='''
    echo "⏰ Time: $(date)"
    echo "🖥️  System: $(uname -s)"
    echo "👤 User: $(whoami)"
    echo "📁 Directory: $(pwd)"
    echo "✅ System check complete"
    ''',
    dag=dag
)

# Data processing
process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    dag=dag
)

# Short wait (processing simulation)
wait_task = BashOperator(
    task_id='processing_wait',
    bash_command='echo "⏳ Processing..." && sleep 2 && echo "✅ Processing complete"',
    dag=dag
)

# Show results
results_task = PythonOperator(
    task_id='show_results',
    python_callable=show_results,
    dag=dag
)

# End
end = DummyOperator(
    task_id='end',
    dag=dag
)

# Task dependencies (simple sequential flow)
start >> generate_data_task >> [system_check, process_data_task]
[system_check, process_data_task] >> wait_task >> results_task >> end

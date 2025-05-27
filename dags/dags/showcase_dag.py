from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
import random
import time

# Default arguments for DAG
default_args = {
    'owner': 'turib-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition
dag = DAG(
    'turib-ecommerce_data_pipeline_poc',
    default_args=default_args,
    description='turib-airflow-demo-ecommerce-data-pipeline',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['poc', 'ecommerce', 'data-pipeline', 'demo']
)

# Python functions for tasks
def extract_customer_data(**context):
    """Müşteri verilerini simulate eder"""
    print("🔄 Müşteri verileri çekiliyor...")
    
    # Simulated customer data
    customers = []
    for i in range(100):
        customer = {
            'customer_id': f'CUST_{i:04d}',
            'name': f'Customer_{i}',
            'email': f'customer{i}@example.com',
            'registration_date': datetime.now() - timedelta(days=random.randint(1, 365)),
            'segment': random.choice(['Premium', 'Standard', 'Basic'])
        }
        customers.append(customer)
    
    print(f"✅ {len(customers)} müşteri verisi başarıyla çekildi")
    
    # share data via XCom
    context['task_instance'].xcom_push(key='customer_count', value=len(customers))
    return customers

def extract_order_data(**context):
    """Sipariş verilerini simulate eder"""
    print("🔄 Sipariş verileri çekiliyor...")
    
    orders = []
    for i in range(200):
        order = {
            'order_id': f'ORD_{i:05d}',
            'customer_id': f'CUST_{random.randint(0, 99):04d}',
            'product_name': f'Product_{random.randint(1, 50)}',
            'quantity': random.randint(1, 5),
            'price': round(random.uniform(10, 500), 2),
            'order_date': datetime.now() - timedelta(days=random.randint(0, 30))
        }
        orders.append(order)
    
    print(f"✅ {len(orders)} sipariş verisi başarıyla çekildi")
    
    # share data via XCom
    context['task_instance'].xcom_push(key='order_count', value=len(orders))
    context['task_instance'].xcom_push(key='total_revenue', value=sum([o['price'] * o['quantity'] for o in orders]))
    return orders

def validate_data(**context):
    """Veri kalitesi kontrolü yapar"""
    print("🔍 Veri kalitesi kontrol ediliyor...")
    
    # retrieve data from XCom
    customer_count = context['task_instance'].xcom_pull(task_ids='extract_customers', key='customer_count')
    order_count = context['task_instance'].xcom_pull(task_ids='extract_orders', key='order_count')
    
    print(f"📊 Kontrol Sonuçları:")
    print(f"   - Müşteri sayısı: {customer_count}")
    print(f"   - Sipariş sayısı: {order_count}")
    
    # data quality checks
    if customer_count < 50:
        raise ValueError("❌ Yetersiz müşteri verisi!")
    
    if order_count < 100:
        raise ValueError("❌ Yetersiz sipariş verisi!")
    
    print("✅ Veri kalitesi kontrolü başarılı")
    return True

def transform_data(**context):
    """Veri dönüşüm işlemleri"""
    print("🔄 Veri dönüşüm işlemleri başlıyor...")
    
    # Simulated transformation operations
    transformations = [
        "Müşteri segmentasyonu güncelleniyor",
        "Sipariş tutarları normalize ediliyor", 
        "Tarih formatları standartlaştırılıyor",
        "Eksik veriler tamamlanıyor",
        "Outlier'lar temizleniyor"
    ]
    
    for transform in transformations:
        print(f"   ⚙️ {transform}...")
        time.sleep(1)  # Simulated processing time
    
    print("✅ Veri dönüşüm işlemleri tamamlandı")
    return "transformation_complete"

def calculate_metrics(**context):
    """İş metrikleri hesaplar"""
    print("📈 İş metrikleri hesaplanıyor...")
    
    # retrieve data from XCom
    total_revenue = context['task_instance'].xcom_pull(task_ids='extract_orders', key='total_revenue')
    customer_count = context['task_instance'].xcom_pull(task_ids='extract_customers', key='customer_count')
    order_count = context['task_instance'].xcom_pull(task_ids='extract_orders', key='order_count')
    
    # Metric calculation
    avg_order_value = total_revenue / order_count if order_count > 0 else 0
    orders_per_customer = order_count / customer_count if customer_count > 0 else 0
    
    metrics = {
        'total_revenue': round(total_revenue, 2),
        'avg_order_value': round(avg_order_value, 2),
        'orders_per_customer': round(orders_per_customer, 2),
        'total_customers': customer_count,
        'total_orders': order_count
    }
    
    print("📊 Hesaplanan Metrikler:")
    for key, value in metrics.items():
        print(f"   - {key}: {value}")
    
    # metrics checks
    if avg_order_value < 50:
        print("⚠️ Uyarı: Ortalama sipariş değeri düşük!")
    
    context['task_instance'].xcom_push(key='business_metrics', value=metrics)
    return metrics

def load_to_warehouse(**context):
    """Data warehouse'a veri yükler (simulated)"""
    print("🏗️ Data warehouse'a veri yükleniyor...")
    
    # Simulated database operations
    operations = [
        "Staging tabloları temizleniyor",
        "Müşteri verileri yükleniyor",
        "Sipariş verileri yükleniyor", 
        "Metrik tabloları güncelleniyor",
        "İndeksler yeniden oluşturuluyor"
    ]
    
    for operation in operations:
        print(f"   💾 {operation}...")
        time.sleep(0.5)
    
    print("✅ Data warehouse yükleme işlemi tamamlandı")
    return "load_complete"

def send_report(**context):
    """Rapor hazırlar ve bildirim gönderir"""
    print("📋 Günlük rapor hazırlanıyor...")
    
    # retrieve metrics from XCom
    metrics = context['task_instance'].xcom_pull(task_ids='calculate_metrics', key='business_metrics')
    
    report = f"""
    📊 GÜNLÜK E-TİCARET RAPORU
    ========================
    📅 Tarih: {datetime.now().strftime('%Y-%m-%d')}
    
    💰 Toplam Gelir: ${metrics['total_revenue']}
    👥 Toplam Müşteri: {metrics['total_customers']}
    🛒 Toplam Sipariş: {metrics['total_orders']}
    💳 Ortalama Sipariş Değeri: ${metrics['avg_order_value']}
    📈 Müşteri Başına Sipariş: {metrics['orders_per_customer']}
    
    ✅ Pipeline başarıyla tamamlandı!
    """
    
    print(report)
    print("📧 Rapor paydaşlara gönderildi (simulated)")
    return "report_sent"

# Task definitions and dependencies

# pipeline start and end
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# extract_customers and extract_orders tasks
extract_customers = PythonOperator(
    task_id='extract_customers',
    python_callable=extract_customer_data,
    dag=dag
)

extract_orders = PythonOperator(
    task_id='extract_orders', 
    python_callable=extract_order_data,
    dag=dag
)

# data validation task
validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)

# data transformation task
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

# calculate metrics task
calculate_metrics_task = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_metrics,
    dag=dag
)

# system health check
system_check = BashOperator(
    task_id='system_health_check',
    bash_command='''
    echo "🔍 Sistem sağlık kontrolü başlıyor..."
    echo "💾 Disk kullanımı: $(df -h / | tail -1 | awk '{print $5}')"
    echo "🧠 Bellek kullanımı: $(free -m | awk 'NR==2{printf "%.1f%%", $3*100/$2 }')"
    echo "✅ Sistem sağlık kontrolü tamamlandı"
    ''',
    dag=dag
)

# Data warehouse load
load_data_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag
)

# send_report_task:
send_report_task = PythonOperator(
    task_id='send_daily_report',
    python_callable=send_report,
    dag=dag
)

# success notification with BashOperator
success_notification = BashOperator(
    task_id='success_notification',
    bash_command='echo "🎉 Pipeline başarıyla tamamlandı! Bildirim gönderildi."',
    dag=dag
)

# Task bağımlılıkları (dependency chain)
start_task >> [extract_customers, extract_orders]

[extract_customers, extract_orders] >> validate_data_task

validate_data_task >> transform_data_task

transform_data_task >> [calculate_metrics_task, system_check]

[calculate_metrics_task, system_check] >> load_data_task 

load_data_task >> send_report_task

send_report_task >> success_notification >> end_task
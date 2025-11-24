# dags/iss_position_tracker.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json

def create_iss_table():
    """Создание таблицы для позиций МКС если она не существует"""
    hook = PostgresHook(postgres_conn_id='postgresql982')
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS analytics.iss_position (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        latitude DECIMAL(10, 6) NOT NULL,
        longitude DECIMAL(10, 6) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(timestamp)
    );
    
    CREATE INDEX IF NOT EXISTS idx_iss_position_timestamp 
    ON analytics.iss_position(timestamp);
    
    CREATE INDEX IF NOT EXISTS idx_iss_position_created_at 
    ON analytics.iss_position(created_at);
    """
    
    try:
        # Создаем схему analytics если не существует
        hook.run("CREATE SCHEMA IF NOT EXISTS analytics;")
        
        # Создаем таблицу
        hook.run(create_table_sql)
        return "✅ Таблица analytics.iss_position создана/проверена"
        
    except Exception as e:
        return f"❌ Ошибка создания таблицы: {e}"

def get_iss_position():
    """Получение текущей позиции МКС из API"""
    try:
        response = requests.get('http://api.open-notify.org/iss-now.json', timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Извлекаем данные
        timestamp = datetime.fromtimestamp(data['timestamp'])
        latitude = float(data['iss_position']['latitude'])
        longitude = float(data['iss_position']['longitude'])
        
        result = {
            'timestamp': timestamp,
            'latitude': latitude,
            'longitude': longitude,
            'raw_data': json.dumps(data)
        }
        
        print(f"✅ Данные получены: {timestamp} - lat: {latitude}, lon: {longitude}")
        return result
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"❌ Ошибка API запроса: {e}")
    except (KeyError, ValueError) as e:
        raise Exception(f"❌ Ошибка парсинга данных: {e}")

def save_iss_position(**context):
    """Сохранение позиции МКС в базу данных"""
    try:
        # Получаем данные из предыдущей задачи
        iss_data = context['task_instance'].xcom_pull(task_ids='get_iss_position')
        
        if not iss_data:
            raise Exception("❌ Нет данных от API")
        
        hook = PostgresHook(postgres_conn_id='postgresql982')
        
        insert_sql = """
        INSERT INTO analytics.iss_position (timestamp, latitude, longitude)
        VALUES (%s, %s, %s)
        ON CONFLICT (timestamp) DO NOTHING;
        """
        
        hook.run(insert_sql, parameters=(
            iss_data['timestamp'],
            iss_data['latitude'], 
            iss_data['longitude']
        ))
        
        message = f"✅ Данные сохранены: {iss_data['timestamp']}"
        print(message)
        return message
        
    except Exception as e:
        raise Exception(f"❌ Ошибка сохранения в БД: {e}")

def check_table_exists():
    """Проверка существования таблицы (дополнительная проверка)"""
    hook = PostgresHook(postgres_conn_id='postgresql982')
    
    check_sql = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'analytics' 
        AND table_name = 'iss_position'
    );
    """
    
    exists = hook.get_first(check_sql)[0]
    return f"✅ Таблица существует: {exists}"

# Настройки DAG
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'iss_position_tracker',
    default_args=default_args,
    description='Получение и сохранение позиции МКС каждые 30 минут',
    schedule=timedelta(minutes=30),  # Запуск каждые 30 минут
    catchup=False,
    tags=['iss', 'space', 'api', 'analytics'],
    max_active_runs=1
) as dag:

    create_table_task = PythonOperator(
        task_id='create_iss_table',
        python_callable=create_iss_table,
    )

    check_table_task = PythonOperator(
        task_id='check_table_exists',
        python_callable=check_table_exists,
    )

    get_iss_position_task = PythonOperator(
        task_id='get_iss_position',
        python_callable=get_iss_position,
    )

    save_iss_position_task = PythonOperator(
        task_id='save_iss_position',
        python_callable=save_iss_position,
    )

    # Определяем порядок выполнения
    create_table_task >> check_table_task >> get_iss_position_task >> save_iss_position_task
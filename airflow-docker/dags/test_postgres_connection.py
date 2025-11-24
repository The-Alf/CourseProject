# dags/test_postgres_connection.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_postgres_connection():
    """Тестируем подключение к Postgres"""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT version();")
        result = cursor.fetchone()
        
        print("✅ Подключение успешно!")
        print(f"Версия PostgreSQL: {result[0]}")
        
        cursor.execute("SELECT datname FROM pg_database;")
        databases = cursor.fetchall()
        print("Доступные базы данных:")
        for db in databases:
            print(f" - {db[0]}")
            
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return False

def execute_sql_with_hook():
    """Выполняем SQL используя Hook"""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Выполняем SQL и получаем результат
        result = hook.get_records("SELECT 1 as test_value, 'success' as status;")
        print(f"✅ Результат запроса: {result}")
        
        return f"Query executed successfully: {result}"
        
    except Exception as e:
        return f"Error: {e}"

# Исправленный DAG для Airflow 3.x
with DAG(
    'test_postgres_connection',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Изменено с schedule_interval на schedule
    catchup=False,
    tags=['test']
) as dag:

    test_connection = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection
    )

    execute_query = PythonOperator(
        task_id='execute_sql_query',
        python_callable=execute_sql_with_hook
    )

    test_connection >> execute_query
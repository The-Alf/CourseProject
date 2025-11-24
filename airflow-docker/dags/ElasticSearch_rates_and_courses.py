from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_elasticsearch_connection():
    """
    Создает соединение с ElasticSearch используя AirFlow connection
    """
    from airflow.hooks.base import BaseHook
    import base64
    
    es_conn = BaseHook.get_connection('elasticsearch_default')
    
    # Формируем URL для подключения
    if es_conn.port:
        host = f"{es_conn.host}:{es_conn.port}"
    else:
        host = es_conn.host
    
    # Создаем клиент ElasticSearch
    if es_conn.login and es_conn.password:
        es = Elasticsearch(
            [host],
            http_auth=(es_conn.login, es_conn.password),
            scheme=es_conn.schema or 'http',
            verify_certs=False
        )
    else:
        es = Elasticsearch([host])
    
    return es

def extract_data_from_postgres():
    """
    Извлекает данные из PostgreSQL по заданному SQL-запросу
    """
    sql_query = """
    SELECT val_date, currency, cur_val FROM
    ( 
    SELECT ch.currency,
           ch.value cur_val,
           ch.val_date
      FROM analytics.currencies_history ch
     UNION ALL
    SELECT 'CB_BID',
           cb.rate_value,
           cb.rate_date
      FROM analytics.cb_bid cb
    )
    ORDER BY val_date, currency
    """
    
    try:
        hook = PostgresHook(postgres_conn_id='postgresql982')
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        
        # Преобразуем в список словарей
        data = []
        for row in results:
            row_dict = dict(zip(columns, row))
            data.append(row_dict)
        
        cursor.close()
        connection.close()
        
        logging.info(f"Извлечено {len(data)} записей из PostgreSQL")
        return data
        
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных из PostgreSQL: {str(e)}")
        raise

def transform_data_for_elasticsearch(**context):
    """
    Преобразует данные для ElasticSearch
    """
    data = context['task_instance'].xcom_pull(task_ids='extract_data_from_postgres')
    
    if not data:
        logging.warning("Нет данных для преобразования")
        return []
    
    transformed_data = []
    for record in data:
        # Преобразуем val_date в timestamp
        if record['val_date']:
            if isinstance(record['val_date'], datetime):
                timestamp = int(record['val_date'].timestamp() * 1000)  # в миллисекундах
            else:
                # Если это date, а не datetime
                timestamp = int(datetime.combine(record['val_date'], datetime.min.time()).timestamp() * 1000)
        else:
            timestamp = None
        
        transformed_record = {
            'val_date': record['val_date'].isoformat() if record['val_date'] else None,
            'val_date_timestamp': timestamp,
            'currency': record['currency'],
            'cur_val': float(record['cur_val']) if record['cur_val'] is not None else None
        }
        transformed_data.append(transformed_record)
    
    logging.info(f"Преобразовано {len(transformed_data)} записей для ElasticSearch")
    return transformed_data

def load_data_to_elasticsearch(**context):
    """
    Загружает данные в ElasticSearch
    """
    transformed_data = context['task_instance'].xcom_pull(task_ids='transform_data_for_elasticsearch')
    
    if not transformed_data:
        logging.warning("Нет данных для загрузки в ElasticSearch")
        return
    
    try:
        es = get_elasticsearch_connection()
        index_name = "rates_and_courses"
        
        # Проверяем доступность ElasticSearch
        if not es.ping():
            raise Exception("Не удалось подключиться к ElasticSearch")
        
        # Создаем индекс если не существует
        if not es.indices.exists(index=index_name):
            es.indices.create(
                index=index_name,
                body={
                    "mappings": {
                        "properties": {
                            "val_date": {"type": "date", "format": "yyyy-MM-dd"},
                            "val_date_timestamp": {"type": "date"},
                            "currency": {"type": "keyword"},
                            "cur_val": {"type": "float"}
                        }
                    }
                }
            )
            logging.info(f"Создан новый индекс: {index_name}")
        
        # Подготавливаем данные для bulk-вставки
        actions = []
        for record in transformed_data:
            doc_id = f"{record['val_date']}_{record['currency']}"
            action = {
                "_index": index_name,
                "_id": doc_id,
                "_source": record
            }
            actions.append(action)
        
        # Выполняем bulk-вставку
        success_count, errors = bulk(es, actions, stats_only=True)
        
        logging.info(f"Успешно загружено в ElasticSearch: {success_count} записей")
        if errors:
            logging.error(f"Ошибок при загрузке: {errors}")
        
        # Обновляем настройки индекса
        es.indices.refresh(index=index_name)
        
    except Exception as e:
        logging.error(f"Ошибка при загрузке в ElasticSearch: {str(e)}")
        raise

def verify_elasticsearch_data(**context):
    """
    Проверяет что данные успешно загружены в ElasticSearch
    """
    try:
        es = get_elasticsearch_connection()
        index_name = "rates_and_courses"
        
        # Проверяем доступность ElasticSearch
        if not es.ping():
            raise Exception("Не удалось подключиться к ElasticSearch")
        
        # Получаем статистику по индексу
        stats = es.indices.stats(index=index_name)
        doc_count = stats['indices'][index_name]['total']['docs']['count']
        
        logging.info(f"В индексе {index_name} содержится {doc_count} документов")
        
        # Выполняем простой поиск для проверки
        search_result = es.search(
            index=index_name,
            body={
                "size": 1,
                "sort": [{"val_date_timestamp": "desc"}]
            }
        )
        
        if search_result['hits']['hits']:
            latest_record = search_result['hits']['hits'][0]['_source']
            logging.info(f"Последняя запись: {latest_record}")
        
        return doc_count
        
    except Exception as e:
        logging.error(f"Ошибка при проверке данных в ElasticSearch: {str(e)}")
        raise

with DAG(
    dag_id='ElasticSearch_rates_and_courses',
    default_args=default_args,
    description='Передача данных из PostgreSQL в ElasticSearch',
    schedule='@daily',
    catchup=False,
    tags=['postgres', 'elasticsearch', 'data_transfer']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_from_postgres',
        python_callable=extract_data_from_postgres
    )

    transform_task = PythonOperator(
        task_id='transform_data_for_elasticsearch',
        python_callable=transform_data_for_elasticsearch
    )

    load_task = PythonOperator(
        task_id='load_data_to_elasticsearch',
        python_callable=load_data_to_elasticsearch
    )

    verify_task = PythonOperator(
        task_id='verify_elasticsearch_data',
        python_callable=verify_elasticsearch_data
    )

    extract_task >> transform_task >> load_task >> verify_task
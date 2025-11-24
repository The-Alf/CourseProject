# dags/test_elasticsearch_connection.py
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import logging
import requests
import json

logger = logging.getLogger(__name__)

def test_es_connection_simple(**kwargs):
    """Простое тестирование подключения к ElasticSearch через REST API"""
    logger.info("🔍 Начинаем тестирование подключения к ElasticSearch...")
    
    try:
        # Получаем настройки подключения из Airflow
        connection = BaseHook.get_connection('elasticsearch_default')
        
        logger.info("✅ Найдено подключение 'elasticsearch_default'")
        logger.info(f"📋 Параметры подключения:")
        logger.info(f"   - Host: {connection.host}")
        logger.info(f"   - Port: {connection.port}")
        logger.info(f"   - Login: {connection.login}")
        logger.info(f"   - Schema: {connection.schema}")
        logger.info(f"   - Extra: {connection.extra}")
        
        # Формируем URL для подключения
        if connection.port:
            es_url = f"{connection.host}:{connection.port}"
        else:
            es_url = connection.host
        
        # Добавляем схему если отсутствует
        if not es_url.startswith(('http://', 'https://')):
            es_url = f"http://{es_url}"
        
        logger.info(f"🌐 URL ElasticSearch: {es_url}")
        
        # Подготавливаем аутентификацию
        auth = None
        if connection.login and connection.password:
            auth = (connection.login, connection.password)
            logger.info("🔐 Используем базовую аутентификацию")
        
        # Тест 1: Проверка доступности кластера
        logger.info("🔄 Тест 1: Проверка доступности кластера...")
        health_url = f"{es_url}/_cluster/health"
        
        response = requests.get(health_url, auth=auth, timeout=30)
        
        if response.status_code == 200:
            health_data = response.json()
            logger.info("✅ Кластер доступен")
            logger.info(f"📊 Статус кластера: {health_data.get('status', 'unknown')}")
            logger.info(f"🏷️  Имя кластера: {health_data.get('cluster_name', 'unknown')}")
        else:
            logger.error(f"❌ Кластер недоступен. HTTP {response.status_code}: {response.text}")
            return False
        
        # Тест 2: Получение информации о узле
        logger.info("🔄 Тест 2: Получение информации о узле...")
        info_url = f"{es_url}/"
        
        response = requests.get(info_url, auth=auth, timeout=30)
        
        if response.status_code == 200:
            info_data = response.json()
            logger.info("✅ Информация о узле получена")
            logger.info(f"🔢 Версия Elasticsearch: {info_data['version']['number']}")
            logger.info(f"📛 Имя узла: {info_data['name']}")
        else:
            logger.error(f"❌ Не удалось получить информацию о узле. HTTP {response.status_code}")
            return False
        
        # Тест 3: Проверка существующих индексов
        logger.info("🔄 Тест 3: Проверка существующих индексов...")
        indices_url = f"{es_url}/_cat/indices?format=json&s=index"
        
        response = requests.get(indices_url, auth=auth, timeout=30)
        
        if response.status_code == 200:
            indices = response.json()
            logger.info(f"📊 Найдено индексов: {len(indices)}")
            for idx in indices[:5]:  # Показываем первые 5 индексов
                logger.info(f"   - {idx['index']}: {idx.get('docs.count', 'N/A')} документов")
        else:
            logger.warning(f"⚠️ Не удалось получить список индексов. HTTP {response.status_code}")
        
        # Тест 4: Создание тестового индекса
        test_index = "test_airflow_connection"
        logger.info(f"🔄 Тест 4: Создание тестового индекса '{test_index}'...")
        
        create_index_url = f"{es_url}/{test_index}"
        
        index_settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "test_field": {"type": "text"},
                    "timestamp": {"type": "date"},
                    "dag_name": {"type": "keyword"}
                }
            }
        }
        
        response = requests.put(
            create_index_url,
            json=index_settings,
            auth=auth,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            logger.info(f"✅ Тестовый индекс '{test_index}' создан")
        elif response.status_code == 400 and "already_exists" in response.text:
            logger.info(f"ℹ️ Тестовый индекс '{test_index}' уже существует")
        else:
            logger.error(f"❌ Не удалось создать индекс. HTTP {response.status_code}: {response.text}")
            return False
        
        # Тест 5: Запись тестового документа
        logger.info("🔄 Тест 5: Запись тестового документа...")
        doc_url = f"{es_url}/{test_index}/_doc"
        
        test_doc = {
            "test_field": "Тестовое сообщение от Airflow DAG",
            "timestamp": datetime.now().isoformat(),
            "dag_name": "test_elasticsearch_connection",
            "connection_id": "elasticsearch_default"
        }
        
        response = requests.post(
            doc_url,
            json=test_doc,
            auth=auth,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            doc_data = response.json()
            doc_id = doc_data['_id']
            logger.info(f"✅ Тестовый документ записан с ID: {doc_id}")
        else:
            logger.error(f"❌ Не удалось записать документ. HTTP {response.status_code}: {response.text}")
            return False
        
        # Тест 6: Чтение тестового документа
        logger.info("🔄 Тест 6: Чтение тестового документа...")
        get_doc_url = f"{es_url}/{test_index}/_doc/{doc_id}"
        
        response = requests.get(get_doc_url, auth=auth, timeout=30)
        
        if response.status_code == 200:
            doc_data = response.json()
            logger.info(f"✅ Документ прочитан: {doc_data['_source']['test_field']}")
        else:
            logger.error(f"❌ Не удалось прочитать документ. HTTP {response.status_code}")
            return False
        
        # Тест 7: Поиск тестового документа
        logger.info("🔄 Тест 7: Поиск тестового документа...")
        search_url = f"{es_url}/{test_index}/_search"
        
        search_query = {
            "query": {
                "match": {
                    "test_field": "Airflow"
                }
            }
        }
        
        response = requests.post(
            search_url,
            json=search_query,
            auth=auth,
            timeout=30
        )
        
        if response.status_code == 200:
            search_data = response.json()
            hits = search_data['hits']['total']['value']
            logger.info(f"✅ Найдено документов: {hits}")
        else:
            logger.error(f"❌ Не удалось выполнить поиск. HTTP {response.status_code}")
            return False
        
        # Тест 8: Удаление тестового индекса
        logger.info("🔄 Тест 8: Удаление тестового индекса...")
        delete_index_url = f"{es_url}/{test_index}"
        
        response = requests.delete(delete_index_url, auth=auth, timeout=30)
        
        if response.status_code in [200, 201]:
            logger.info("✅ Тестовый индекс удален")
        else:
            logger.warning(f"⚠️ Не удалось удалить индекс. HTTP {response.status_code}")
        
        # Итоговый отчет
        logger.info("🎉 Все тесты пройдены успешно!")
        logger.info("✅ Подключение к ElasticSearch работает корректно")
        
        result = {
            "status": "success",
            "cluster_name": health_data.get('cluster_name', 'unknown'),
            "version": info_data['version']['number'],
            "tests_passed": 8,
            "es_url": es_url
        }
        
        kwargs['ti'].xcom_push(key='test_result', value=result)
        return result
        
    except Exception as e:
        logger.error(f"❌ Ошибка при тестировании подключения: {e}")
        
        import traceback
        logger.error(f"🔍 Детали ошибки: {traceback.format_exc()}")
        
        result = {
            "status": "failed",
            "error": str(e),
            "tests_passed": 0
        }
        
        kwargs['ti'].xcom_push(key='test_result', value=result)
        return result

def generate_report(**kwargs):
    """Генерация отчета о тестировании"""
    ti = kwargs['ti']
    test_result = ti.xcom_pull(task_ids='test_es_connection', key='test_result')
    
    logger.info("📊 ===== ОТЧЕТ О ТЕСТИРОВАНИИ ELASTICSEARCH =====")
    
    if test_result and test_result.get('status') == 'success':
        logger.info("🎉 РЕЗУЛЬТАТ: УСПЕХ")
        logger.info(f"🏷️  Имя кластера: {test_result.get('cluster_name')}")
        logger.info(f"🔢 Версия: {test_result.get('version')}")
        logger.info(f"🌐 URL: {test_result.get('es_url')}")
        logger.info(f"✅ Пройдено тестов: {test_result.get('tests_passed')}")
        logger.info("💡 Рекомендации: Подключение настроено корректно")
    else:
        logger.error("💥 РЕЗУЛЬТАТ: ПРОВАЛ")
        logger.error(f"❌ Ошибка: {test_result.get('error', 'Неизвестная ошибка')}")
        logger.info("🔧 Рекомендации: Проверьте настройки подключения в Airflow UI")
        
        logger.info("""
🔧 КАК НАСТРОИТЬ ПОДКЛЮЧЕНИЕ:
1. Откройте Airflow UI → Admin → Connections
2. Добавьте новое подключение:
   - Conn Id: elasticsearch_default
   - Conn Type: HTTP (или Elasticsearch если доступно)
   - Host: ваш ES хост (например: http://elasticsearch:9200 или localhost:9200)
   - Port: 9200
   - Extra: {{"timeout": 30}}
3. Для аутентификации укажите Login/Password если требуется
4. Сохраните и перезапустите DAG
        """)

with DAG(
    'test_elasticsearch_connection',
    description='Тестирование подключения к ElasticSearch через REST API',
    schedule=None,  # Ручной запуск
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'elasticsearch', 'connection', 'debug'],
    default_args={
        'owner': 'airflow',
        'retries': 0,
    }
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_connection_task = PythonOperator(
        task_id='test_es_connection',
        python_callable=test_es_connection_simple,
    )
    
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )
    
    end = EmptyOperator(task_id='end')

    # Определяем порядок выполнения
    start >> test_connection_task >> generate_report_task >> end
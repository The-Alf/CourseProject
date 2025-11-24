# dags/cbr_key_rate_debug.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger(__name__)

def create_key_rate_table():
    """Создание таблицы для хранения ключевой ставки ЦБ РФ"""
    try:
        hook = PostgresHook(postgres_conn_id='postgresql982')
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS analytics.cb_bid (
            id SERIAL PRIMARY KEY,
            rate_date DATE NOT NULL,
            key_rate DECIMAL(8, 4) NOT NULL,
            sysdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(rate_date)
        );
        """
        
        hook.run("CREATE SCHEMA IF NOT EXISTS analytics;")
        hook.run(create_table_sql)
        logger.info("✅ Таблица analytics.cb_bid создана/проверена")
        return "Таблица создана успешно"
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания таблицы: {e}")
        raise

def debug_api_response():
    """Отладочная функция для проверки API ответа"""
    try:
        # SOAP запрос
        soap_request = '''<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <KeyRateXML xmlns="http://web.cbr.ru/">
      <fromDate>2020-01-01T00:00:00</fromDate>
      <ToDate>2025-11-22T23:59:59</ToDate>
    </KeyRateXML>
  </soap12:Body>
</soap12:Envelope>'''
        
        headers = {
            'Content-Type': 'application/soap+xml; charset=utf-8',
            'Content-Length': str(len(soap_request))
        }
        
        logger.info("🔍 DEBUG: Отправляем запрос к API ЦБ РФ...")
        response = requests.post(
            "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx",
            data=soap_request,
            headers=headers,
            timeout=30
        )
        
        logger.info(f"🔍 DEBUG: Статус ответа: {response.status_code}")
        logger.info(f"🔍 DEBUG: Заголовки ответа: {response.headers}")
        
        # Сохраняем полный ответ для анализа
        full_response = response.text
        logger.info(f"🔍 DEBUG: Длина ответа: {len(full_response)} символов")
        logger.info(f"🔍 DEBUG: Первые 1000 символов ответа:\n{full_response[:1000]}")
        
        # Проверяем наличие ошибок в ответе
        if "soap:Fault" in full_response:
            logger.error("🔍 DEBUG: В ответе обнаружена SOAP ошибка")
            # Парсим ошибку
            try:
                root = ET.fromstring(full_response)
                for fault in root.iter():
                    if 'faultstring' in fault.tag.lower():
                        logger.error(f"🔍 DEBUG: SOAP ошибка: {fault.text}")
            except:
                pass
        
        return full_response
        
    except Exception as e:
        logger.error(f"🔍 DEBUG: Ошибка запроса: {e}")
        raise

def parse_api_response(**context):
    """Парсинг ответа API с несколькими методами"""
    try:
        # Получаем ответ из предыдущей задачи
        full_response = context['task_instance'].xcom_pull(task_ids='debug_api')
        logger.info("🔍 DEBUG: Начинаем парсинг ответа...")
        
        records = []
        hook = PostgresHook(postgres_conn_id='postgresql982')
        
        # МЕТОД 1: Прямой парсинг XML
        try:
            logger.info("🔍 DEBUG: Пробуем метод 1 - прямой парсинг XML")
            root = ET.fromstring(full_response)
            
            # Ищем данные в разных возможных местах
            possible_paths = [
                './/{http://web.cbr.ru/}KeyRateXMLResult',
                './/KeyRateXMLResult',
                './/KeyRateXMLResponse',
                './/KeyRate'
            ]
            
            for path in possible_paths:
                elements = root.findall(path)
                logger.info(f"🔍 DEBUG: По пути '{path}' найдено {len(elements)} элементов")
                
                for elem in elements:
                    if elem.text and '<KeyRate>' in elem.text:
                        logger.info("🔍 DEBUG: Найден XML с данными внутри текста")
                        data_root = ET.fromstring(elem.text)
                        records.extend(parse_keyrate_data(data_root))
                    elif elem.tag.endswith('KeyRate'):
                        logger.info("🔍 DEBUG: Найден элемент KeyRate напрямую")
                        records.extend(parse_keyrate_data(elem))
        
        except Exception as e:
            logger.warning(f"🔍 DEBUG: Метод 1 не сработал: {e}")
        
        # МЕТОД 2: Поиск по тексту
        if not records and '<KeyRate>' in full_response:
            logger.info("🔍 DEBUG: Пробуем метод 2 - поиск по тексту")
            try:
                # Ищем блок с данными
                start = full_response.find('<KeyRateXMLResult>')
                if start == -1:
                    start = full_response.find('<KeyRate>')
                
                if start != -1:
                    # Находим закрывающий тег
                    end_tag = '</KeyRateXMLResult>' if '<KeyRateXMLResult>' in full_response else '</KeyRate>'
                    end = full_response.find(end_tag, start)
                    
                    if end != -1:
                        data_text = full_response[start:end + len(end_tag)]
                        logger.info(f"🔍 DEBUG: Извлечен блок данных: {data_text[:500]}...")
                        
                        data_root = ET.fromstring(data_text)
                        records.extend(parse_keyrate_data(data_root))
            except Exception as e:
                logger.warning(f"🔍 DEBUG: Метод 2 не сработал: {e}")
        
        # МЕТОД 3: Альтернативный API endpoint
        if not records:
            logger.info("🔍 DEBUG: Пробуем метод 3 - альтернативный endpoint")
            try:
                # Попробуем получить данные через другой метод
                alt_soap_request = '''<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <KeyRate xmlns="http://web.cbr.ru/">
      <fromDate>2020-01-01T00:00:00</fromDate>
      <ToDate>2025-11-22T23:59:59</ToDate>
    </KeyRate>
  </soap12:Body>
</soap12:Envelope>'''
                
                headers = {'Content-Type': 'application/soap+xml; charset=utf-8'}
                response = requests.post(
                    "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx",
                    data=alt_soap_request,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    root = ET.fromstring(response.text)
                    records.extend(parse_keyrate_data(root))
                    
            except Exception as e:
                logger.warning(f"🔍 DEBUG: Метод 3 не сработал: {e}")
        
        # Сохраняем записи в базу
        if records:
            logger.info(f"✅ Найдено {len(records)} записей для сохранения")
            
            for record in records:
                insert_sql = """
                INSERT INTO analytics.cb_bid (rate_date, key_rate)
                VALUES (%s, %s)
                ON CONFLICT (rate_date) DO UPDATE SET
                    key_rate = EXCLUDED.key_rate,
                    sysdate = CURRENT_TIMESTAMP;
                """
                hook.run(insert_sql, parameters=(record['rate_date'], record['key_rate']))
            
            return f"Успешно сохранено {len(records)} записей"
        else:
            logger.warning("⚠️ Не найдено ни одной записи для сохранения")
            # Сохраняем отладочную информацию
            logger.info(f"🔍 DEBUG: Полный ответ API сохранен для анализа")
            return "Записи не найдены в ответе API"
            
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга: {e}")
        raise

def parse_keyrate_data(root):
    """Парсинг данных KeyRate из XML элемента"""
    records = []
    
    try:
        # Ищем все элементы KeyRate
        for keyrate in root.findall('.//KeyRate'):
            dt_elem = keyrate.find('DT')
            rate_elem = keyrate.find('Rate')
            
            if dt_elem is not None and rate_elem is not None:
                date_str = dt_elem.text
                rate_str = rate_elem.text
                
                if date_str and rate_str:
                    try:
                        rate_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S').date()
                        key_rate = float(rate_str)
                        
                        records.append({
                            'rate_date': rate_date,
                            'key_rate': key_rate
                        })
                        
                        logger.info(f"🔍 DEBUG: Найдена запись: {rate_date} - {key_rate}%")
                    except ValueError as e:
                        logger.warning(f"🔍 DEBUG: Ошибка преобразования данных: {e}")
    
    except Exception as e:
        logger.warning(f"🔍 DEBUG: Ошибка парсинга KeyRate данных: {e}")
    
    return records

def log_final_stats():
    """Логирование финальной статистики"""
    try:
        hook = PostgresHook(postgres_conn_id='postgresql982')
        stats = hook.get_first("""
            SELECT COUNT(*), MIN(rate_date), MAX(rate_date), 
                   AVG(key_rate), MIN(key_rate), MAX(key_rate)
            FROM analytics.cb_bid
        """)
        
        if stats and stats[0] > 0:
            logger.info(f"📊 ФИНАЛЬНАЯ СТАТИСТИКА:")
            logger.info(f"   Всего записей: {stats[0]}")
            logger.info(f"   Период: {stats[1]} - {stats[2]}")
            logger.info(f"   Средняя ставка: {stats[3]:.2f}%")
            logger.info(f"   Минимальная ставка: {stats[4]}%")
            logger.info(f"   Максимальная ставка: {stats[5]}%")
            return f"Успешно: {stats[0]} записей"
        else:
            logger.warning("📊 Таблица пустая")
            return "Таблица пустая"
            
    except Exception as e:
        logger.error(f"❌ Ошибка статистики: {e}")
        return "Ошибка статистики"

# DAG с отладкой
with DAG(
    'cbr_key_rate_debug',
    description='Загрузка ключевой ставки ЦБ РФ (отладочная версия)',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['cbr', 'key_rate', 'debug']
) as dag:

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_key_rate_table,
    )

    debug_api = PythonOperator(
        task_id='debug_api',
        python_callable=debug_api_response,
    )

    parse_data = PythonOperator(
        task_id='parse_data',
        python_callable=parse_api_response,
    )

    show_stats = PythonOperator(
        task_id='show_stats',
        python_callable=log_final_stats,
    )

    create_table >> debug_api >> parse_data >> show_stats
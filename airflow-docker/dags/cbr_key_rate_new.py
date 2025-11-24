# dags/cbr_key_rate_new.py
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
        
        # Создаем схему analytics если не существует
        hook.run("CREATE SCHEMA IF NOT EXISTS analytics;")
        
        # Создаем таблицу
        hook.run(create_table_sql)
        logger.info("✅ Таблица analytics.cb_bid создана/проверена")
        return "Таблица создана успешно"
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания таблицы: {e}")
        raise

def get_cbr_key_rate_data():
    """Получение данных о ключевой ставке ЦБ РФ"""
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
        
        logger.info("📡 Отправляем запрос к ЦБ РФ...")
        response = requests.post(
            "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx",
            data=soap_request,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        logger.info("✅ Запрос выполнен успешно")
        
        # Парсим ответ
        root = ET.fromstring(response.text)
        
        # Ищем данные разными способами
        data_text = None
        
        # Способ 1: Ищем по полному пути
        for elem in root.iter():
            if 'KeyRateXMLResult' in elem.tag:
                data_text = elem.text
                break
        
        # Способ 2: Ищем в тексте
        if not data_text and '<KeyRateXMLResult>' in response.text:
            start = response.text.find('<KeyRateXMLResult>') + len('<KeyRateXMLResult>')
            end = response.text.find('</KeyRateXMLResult>')
            if start > 0 and end > start:
                data_text = response.text[start:end]
        
        if not data_text:
            logger.error("❌ Не найдены данные в ответе")
            return "Данные не найдены"
        
        # Парсим внутренний XML
        data_root = ET.fromstring(data_text)
        records = []
        hook = PostgresHook(postgres_conn_id='postgresql982')
        
        for record in data_root.findall('.//KeyRate'):
            dt_elem = record.find('DT')
            rate_elem = record.find('Rate')
            
            if dt_elem is not None and rate_elem is not None:
                date_str = dt_elem.text
                rate_str = rate_elem.text
                
                if date_str and rate_str:
                    rate_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S').date()
                    key_rate = float(rate_str)
                    
                    # Сохраняем в базу
                    insert_sql = """
                    INSERT INTO analytics.cb_bid (rate_date, key_rate)
                    VALUES (%s, %s)
                    ON CONFLICT (rate_date) DO UPDATE SET
                        key_rate = EXCLUDED.key_rate,
                        sysdate = CURRENT_TIMESTAMP;
                    """
                    hook.run(insert_sql, parameters=(rate_date, key_rate))
                    records.append((rate_date, key_rate))
        
        logger.info(f"✅ Загружено {len(records)} записей")
        return f"Успешно загружено {len(records)} записей"
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        raise

def log_stats():
    """Логирование статистики"""
    try:
        hook = PostgresHook(postgres_conn_id='postgresql982')
        stats = hook.get_first("""
            SELECT COUNT(*), MIN(rate_date), MAX(rate_date) 
            FROM analytics.cb_bid
        """)
        
        if stats:
            logger.info(f"📊 Статистика: {stats[0]} записей, период {stats[1]} - {stats[2]}")
            return f"Статистика: {stats[0]} записей"
        else:
            return "Нет данных для статистики"
            
    except Exception as e:
        logger.warning(f"Ошибка статистики: {e}")
        return "Ошибка статистики"

# Простой DAG
with DAG(
    'cbr_key_rate_new',
    description='Загрузка ключевой ставки ЦБ РФ',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['cbr', 'key_rate']
) as dag:

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_key_rate_table,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=get_cbr_key_rate_data,
    )

    show_stats = PythonOperator(
        task_id='show_stats',
        python_callable=log_stats,
    )

    create_table >> load_data >> show_stats
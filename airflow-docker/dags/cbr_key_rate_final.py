# dags/cbr_key_rate_final.py
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def load_data(**kwargs):
    """Загрузка данных о ключевой ставке ЦБ РФ"""
    logger = logging.getLogger(__name__)
    
    try:
        # SOAP запрос для получения ключевой ставки
        soap_request = '''<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <KeyRateXML xmlns="http://web.cbr.ru/">
      <fromDate>2020-01-01T00:00:00</fromDate>
      <ToDate>2025-11-22T23:59:59</ToDate>
    </KeyRateXML>
  </soap12:Body>
</soap12:Envelope>'''
        
        headers = {'Content-Type': 'application/soap+xml; charset=utf-8'}
        
        logger.info("📡 Отправляем запрос к ЦБ РФ...")
        response = requests.post(
            "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx",
            data=soap_request,
            headers=headers,
            timeout=30
        )
        
        logger.info(f"✅ Запрос выполнен успешно. Статус: {response.status_code}")
        logger.info(f"📄 Длина ответа: {len(response.text)} символов")
        
        # Парсим ответ
        root = ET.fromstring(response.text)
        
        # Упрощенный парсинг - ищем элементы напрямую
        logger.info("🔍 Ищем элементы KR напрямую...")
        
        rates_data = []
        
        # Ищем все элементы KR в любом неймспейсе
        for elem in root.iter():
            # Убираем неймспейс из тега для простоты сравнения
            tag_clean = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            
            if tag_clean == 'KR':
                # Нашли элемент KR, ищем внутри него DT и Rate
                kr_data = {}
                for child in elem:
                    child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                    if child_tag == 'DT':
                        kr_data['date'] = child.text
                    elif child_tag == 'Rate':
                        kr_data['rate'] = float(child.text) if child.text else None
                
                # Если нашли обе даты, добавляем в результат
                if 'date' in kr_data and 'rate' in kr_data:
                    rates_data.append(kr_data)
        
        logger.info(f"📊 Найдено {len(rates_data)} записей KR")
        
        if rates_data:
            # Сортируем по дате (от новых к старым)
            rates_data.sort(key=lambda x: x['date'], reverse=True)
            
            # Логируем примеры данных
            for i, item in enumerate(rates_data[:5]):
                logger.info(f"📈 Пример {i+1}: {item['date']} - {item['rate']}%")
            
            logger.info(f"✅ Успешно загружено {len(rates_data)} записей")
            
            # Передаем данные в XCom
            kwargs['ti'].xcom_push(key='key_rates_data', value=rates_data)
            return rates_data
        else:
            logger.error("❌ Не найдены данные в ответе")
            
            # Дополнительная диагностика
            logger.info("🔍 Диагностика структуры XML:")
            unique_tags = set()
            for elem in root.iter():
                tag_clean = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                unique_tags.add(tag_clean)
                if len(unique_tags) > 20:  # Ограничим вывод
                    break
            
            logger.info(f"🔍 Уникальные теги в ответе: {sorted(unique_tags)}")
            
            return []
        
    except ET.ParseError as e:
        logger.error(f"❌ Ошибка парсинга XML: {e}")
        if 'response' in locals():
            logger.info(f"📄 Первые 500 символов ответа: {response.text[:500]}")
        return []
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке данных: {e}")
        return []
    
def transform_data(**kwargs):
    """Трансформация данных"""
    logger.info("🔄 Начинаем трансформацию данных...")
    
    # Получаем данные из XCom
    ti = kwargs['ti']
    rates_data = ti.xcom_pull(task_ids='load_data', key='key_rates_data')
    
    if not rates_data:
        logger.error("❌ Нет данных для трансформации")
        return []
    
    logger.info(f"📊 Получено {len(rates_data)} записей для трансформации")
    
    # Простая трансформация - добавляем поле с годом
    transformed_data = []
    for item in rates_data:
        try:
            transformed_item = item.copy()
            # Извлекаем год из даты (формат: 2023-01-15T00:00:00)
            if item['date'] and 'T' in item['date']:
                year = item['date'].split('T')[0].split('-')[0]
                transformed_item['year'] = int(year)
            else:
                transformed_item['year'] = None
            
            transformed_data.append(transformed_item)
        except Exception as e:
            logger.warning(f"⚠️ Ошибка трансформации записи {item}: {e}")
            continue
    
    logger.info(f"✅ Трансформировано {len(transformed_data)} записей")
    
    # Логируем примеры трансформированных данных
    for i, item in enumerate(transformed_data[:3]):
        logger.info(f"🔄 Пример {i+1}: {item}")
    
    # Сохраняем в XCom
    kwargs['ti'].xcom_push(key='transformed_rates_data', value=transformed_data)
    return transformed_data

def load_to_postgres(**kwargs):
    """Загрузка данных в PostgreSQL"""
    logger.info("💾 Загружаем данные в PostgreSQL...")
    
    # Получаем данные из XCom
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_rates_data')
    
    if not transformed_data:
        logger.error("❌ Нет данных для загрузки в БД")
        return
    
    logger.info(f"📊 Загружаем {len(transformed_data)} записей в БД")
    
    try:
        # Подключаемся к PostgreSQL
        hook = PostgresHook(postgres_conn_id='postgresql982')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Вставляем данные
        insert_sql = """
        INSERT INTO analytics.cb_bid (rate_date, rate_value, year)
        VALUES (%s, %s, %s)
        ON CONFLICT (rate_date) DO UPDATE SET
            rate_value = EXCLUDED.rate_value,
            year = EXCLUDED.year,
            updated_at = CURRENT_TIMESTAMP
        """
        
        for item in transformed_data:
            cursor.execute(insert_sql, (
                item['date'].split('T')[0] if 'T' in item['date'] else item['date'],
                item['rate'],
                item.get('year')
            ))
        
        conn.commit()
        logger.info(f"✅ Успешно загружено {len(transformed_data)} записей в PostgreSQL")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке в PostgreSQL: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'cbr_key_rate_final',
    default_args=default_args,
    description='Загрузка ключевой ставки ЦБ РФ',
    schedule='0 12 * * *',  # Ежедневно в 12:00 - исправлено на schedule
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['cbr', 'key_rate', 'finance'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgresql982',
        sql="""
        CREATE TABLE IF NOT EXISTS analytics.cb_bid (
            id SERIAL PRIMARY KEY,
            rate_date DATE UNIQUE NOT NULL,
            rate_value DECIMAL(8,4) NOT NULL,
            year INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_an_cbr_key_rates_date ON analytics.cb_bid(rate_date);
        CREATE INDEX IF NOT EXISTS idx_an_cbr_key_rates_year ON analytics.cb_bid(year);
        """
    )
    
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )
    
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    
    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )
    
    end = EmptyOperator(task_id='end')

    # Определение порядка выполнения
    start >> create_table >> load_data_task >> transform_data_task >> load_to_postgres_task >> end
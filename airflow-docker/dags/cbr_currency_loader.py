# dags/cbr_currency_loader.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger(__name__)

def create_currencies_table():
    """Создание таблицы для хранения истории курсов валют"""
    hook = PostgresHook(postgres_conn_id='postgresql982')
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS analytics.currencies_history (
        id SERIAL PRIMARY KEY,
        currency VARCHAR(10) NOT NULL,
        value DECIMAL(10, 4) NOT NULL,
        val_date DATE NOT NULL,
        sysdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(currency, val_date)
    );
    
    CREATE INDEX IF NOT EXISTS idx_currencies_history_currency 
    ON analytics.currencies_history(currency);
    
    CREATE INDEX IF NOT EXISTS idx_currencies_history_date 
    ON analytics.currencies_history(val_date);
    
    CREATE INDEX IF NOT EXISTS idx_currencies_history_currency_date 
    ON analytics.currencies_history(currency, val_date);
    """
    
    try:
        # Создаем схему analytics если не существует
        hook.run("CREATE SCHEMA IF NOT EXISTS analytics;")
        
        # Создаем таблицу
        hook.run(create_table_sql)
        logger.info("✅ Таблица analytics.currencies_history создана/проверена")
        return "Таблица создана/проверена успешно"
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания таблицы: {e}")
        raise

def check_if_first_run():
    """Проверяем, является ли это первый запуск (таблица пустая)"""
    hook = PostgresHook(postgres_conn_id='postgresql982')
    
    check_sql = """
    SELECT COUNT(*) as record_count 
    FROM analytics.currencies_history;
    """
    
    try:
        result = hook.get_first(check_sql)
        record_count = result[0] if result else 0
        
        is_first_run = record_count == 0
        logger.info(f"Количество записей в таблице: {record_count}. Первый запуск: {is_first_run}")
        
        # Сохраняем результат в XCom для использования в других задачах
        return is_first_run
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки таблицы: {e}")
        # Если таблицы нет, считаем что первый запуск
        return True

def parse_cbr_xml(xml_content, currency_code):
    """Парсинг XML от ЦБ РФ и извлечение данных о курсах"""
    try:
        root = ET.fromstring(xml_content)
        records = []
        
        # Маппинг кодов валют ЦБ на символы
        currency_map = {
            'R01235': 'USD',
            'R01239': 'EUR', 
            'R01035': 'GBP',
            'R01375': 'CNY'
        }
        
        currency_name = currency_map.get(currency_code, currency_code)
        
        for record in root.findall('Record'):
            date_str = record.get('Date')
            value_str = record.find('Value').text.replace(',', '.')
            
            # Конвертируем дату из формата ЦБ (дд.мм.гггг)
            date_obj = datetime.strptime(date_str, '%d.%m.%Y').date()
            value = float(value_str)
            
            records.append({
                'currency': currency_name,
                'value': value,
                'val_date': date_obj
            })
        
        logger.info(f"✅ Извлечено {len(records)} записей для {currency_name}")
        return records
        
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга XML для {currency_code}: {e}")
        raise

def load_historical_data():
    """Загрузка исторических данных за период 2020-2025"""
    currencies = {
        'USD': 'R01235',
        'EUR': 'R01239', 
        'GBP': 'R01035',
        'CNY': 'R01375'
    }
    
    hook = PostgresHook(postgres_conn_id='postgresql982')
    all_records = []
    
    for currency_name, currency_code in currencies.items():
        try:
            url = f"https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1=01/01/2020&date_req2=24/11/2025&VAL_NM_RQ={currency_code}"
            
            logger.info(f"Загружаем исторические данные для {currency_name}...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Парсим XML
            records = parse_cbr_xml(response.content, currency_code)
            all_records.extend(records)
            
            logger.info(f"✅ Исторические данные для {currency_name} загружены")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки исторических данных для {currency_name}: {e}")
            continue
    
    # Сохраняем все записи в базу
    if all_records:
        insert_sql = """
        INSERT INTO analytics.currencies_history (currency, value, val_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (currency, val_date) DO UPDATE SET
            value = EXCLUDED.value,
            sysdate = CURRENT_TIMESTAMP;
        """
        
        for record in all_records:
            hook.run(insert_sql, parameters=(
                record['currency'],
                record['value'], 
                record['val_date']
            ))
        
        logger.info(f"✅ Сохранено {len(all_records)} исторических записей")
        return f"Сохранено {len(all_records)} исторических записей"
    
    return "Нет данных для сохранения"

def parse_daily_xml(xml_content):
    """Парсинг ежедневного XML с курсами валют"""
    try:
        root = ET.fromstring(xml_content)
        records = []
        
        # Дата из XML (в формате дд.мм.гггг)
        date_str = root.get('Date')
        val_date = datetime.strptime(date_str, '%d.%m.%Y').date()
        
        # Ищем нужные валюты по CharCode
        target_currencies = ['USD', 'EUR', 'GBP', 'CNY']
        
        for valute in root.findall('Valute'):
            char_code = valute.find('CharCode').text
            if char_code in target_currencies:
                value_str = valute.find('Value').text.replace(',', '.')
                value = float(value_str)
                
                records.append({
                    'currency': char_code,
                    'value': value,
                    'val_date': val_date
                })
        
        logger.info(f"✅ Извлечены курсы на {val_date}: {len(records)} валют")
        return records
        
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга ежедневного XML: {e}")
        raise

def load_daily_data():
    """Загрузка ежедневных данных на завтра"""
    # Получаем завтрашнюю дату
    tomorrow = datetime.now() + timedelta(days=1)
    date_req = tomorrow.strftime('%d/%m/%Y')
    
    try:
        url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_req}"
        
        logger.info(f"Загружаем ежедневные данные на {date_req}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Парсим XML
        records = parse_daily_xml(response.content)
        
        # Сохраняем в базу
        if records:
            hook = PostgresHook(postgres_conn_id='postgresql982')
            
            insert_sql = """
            INSERT INTO analytics.currencies_history (currency, value, val_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (currency, val_date) DO UPDATE SET
                value = EXCLUDED.value,
                sysdate = CURRENT_TIMESTAMP;
            """
            
            for record in records:
                hook.run(insert_sql, parameters=(
                    record['currency'],
                    record['value'],
                    record['val_date']
                ))
            
            currency_list = ', '.join([r['currency'] for r in records])
            logger.info(f"✅ Сохранены ежедневные курсы: {currency_list} на {records[0]['val_date']}")
            return f"Сохранены курсы: {currency_list}"
        
        return "Нет новых данных для сохранения"
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки ежедневных данных: {e}")
        raise

def log_final_stats(**context):
    """Логирование финальной статистики"""
    hook = PostgresHook(postgres_conn_id='postgresql982')
    
    stats_sql = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT currency) as currency_count,
        MIN(val_date) as earliest_date,
        MAX(val_date) as latest_date
    FROM analytics.currencies_history;
    """
    
    try:
        stats = hook.get_first(stats_sql)
        logger.info(f"📊 Статистика Currencies History:")
        logger.info(f"   Всего записей: {stats[0]}")
        logger.info(f"   Количество валют: {stats[1]}")
        logger.info(f"   Период: {stats[2]} - {stats[3]}")
        
        return f"Статистика: {stats[0]} записей, {stats[1]} валют"
        
    except Exception as e:
        logger.warning(f"Не удалось получить статистику: {e}")
        return "Статистика недоступна"

# Настройки DAG с запуском в 19:00
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'cbr_currency_loader',
    default_args=default_args,
    description='Загрузка курсов валют ЦБ РФ - исторические и ежедневные данные',
    # schedule=timedelta(minutes=30),
    schedule='0 19 * * *',  # ⏰ ИЗМЕНЕНО: Ежедневно в 19:00
    catchup=False,
    tags=['cbr', 'currency', 'analytics'],
    max_active_runs=1
) as dag:

    create_table_task = PythonOperator(
        task_id='create_currencies_table',
        python_callable=create_currencies_table,
    )

    check_first_run_task = PythonOperator(
        task_id='check_first_run',
        python_callable=check_if_first_run,
    )

    load_historical_data_task = PythonOperator(
        task_id='load_historical_data',
        python_callable=load_historical_data,
    )

    load_daily_data_task = PythonOperator(
        task_id='load_daily_data',
        python_callable=load_daily_data,
    )

    log_stats_task = PythonOperator(
        task_id='log_final_stats',
        python_callable=log_final_stats,
    )

    # Определяем порядок выполнения с ветвлением
    create_table_task >> check_first_run_task >> [load_historical_data_task, load_daily_data_task]
    load_historical_data_task >> log_stats_task
    load_daily_data_task >> log_stats_task
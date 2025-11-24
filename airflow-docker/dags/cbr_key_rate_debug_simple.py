# dags/cbr_key_rate_debug_simple.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger(__name__)

def debug_parsing():
    """Простая отладочная функция для проверки парсинга"""
    try:
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
        response = requests.post(
            "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx",
            data=soap_request,
            headers=headers,
            timeout=30
        )
        
        root = ET.fromstring(response.text)
        
        # Находим данные
        for elem in root.iter():
            if 'KeyRateXMLResult' in elem.tag:
                data_root = ET.fromstring(elem.text)
                break
        
        # Считаем элементы KR
        kr_elements = data_root.findall('.//KR')
        logger.info(f"🔍 Найдено {len(kr_elements)} элементов KR")
        
        # Показываем первые 3 элемента
        for i, kr in enumerate(kr_elements[:3]):
            dt = kr.find('DT').text
            rate = kr.find('Rate').text
            logger.info(f"🔍 Пример {i+1}: DT={dt}, Rate={rate}")
        
        return f"Найдено {len(kr_elements)} записей"
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        raise

with DAG(
    'cbr_key_rate_debug_simple',    
    description='Отладочный DAG для проверки парсинга',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['debug']
) as dag:

    debug_task = PythonOperator(
        task_id='debug_parsing',
        python_callable=debug_parsing,
    )
from fileinput import filename


import aiohttp
import asyncio
import json
import httpx
import time
import os
import base64

from ClassConverter import DataConverter
from ClassFiles import FileManager
from ClassLogger import LoggerConfig
from ClassHTTP import AsyncHttpClient, RequestFormat, ResponseFormat, async_test_http, async_tests_http


import asyncio
import aiofiles
import logging

import sys
# инициализируем основное логирование
logger_config = LoggerConfig(log_file='base.log', log_level="INFO", console_output=True, use_json=False)
logger_config.setup_logger()
logger = logger_config.get_logger(__name__)
logger.info("Основной файл base")


async def lifestile_task(logger: logging.Logger, interval: int = 300):
    '''Проверка жизни программы'''
    logger.info("Service lifestile: running OK (startup)")  # сразу при запуске
    while True:
        await asyncio.sleep(interval)
        logger.info("Service lifestile: running OK")

async def send_request(client: httpx.AsyncClient, url, method='GET', headers=None, params=None, data=None, json=None, timeout: float = 50.0):
    '''шаблонка для запросов'''
    try:
        response = await client.request(method=method, url=url, headers=headers, params=params, data=data,json=json, timeout=timeout)
        response.raise_for_status()
        return response.json() if response.content else None
    except httpx.HTTPStatusError as e:
        return {'status': 'error', 'status_code': e.response.status_code, 'error': str(e)}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


async def call_crm_api(client: httpx.AsyncClient):
    json_1 = {
        "action": "get_chart_data",
        "label": "mn_ai_percent_miss",
        "token": "2779115BVKQ6B45KMALD84M",
        "params": None
    }
    headers_1 = {'Content-Type': 'application/json'}
    request_1 = await send_request(client=client, url="",method="POST", json=json_1, headers=headers_1)
    #logger.info(request_1)
    logger.info(json.dumps(request_1, indent=2, ensure_ascii=False))


async def featch_data(client: httpx.AsyncClient, flag=False, filename: str = ''):
    t_get_start = time.perf_counter()

    directory = r'C:\Users\beginin-ov\Projects\Local\work\temp_audio_files'
    input_format = 'mp3'
    response = await send_request(client=client, url='/download',method="GET", params = {'filename': filename})
    t_get = round(time.perf_counter() - t_get_start,2)
    #data = response['data']
    # logger.info(f"data: {data}")
    file_base64 = response.get("recipient_data").get('file')

    if not file_base64:
        logger.info(f"файл отсутствует")
        return False
    #logger.info(f"Время получения файла: {t_get} с")
    if flag==True:
        try:
            file_bytes = base64.b64decode(file_base64)  # Декодирование из base64
            if not os.path.exists(f"{directory}"):
                os.makedirs(f"{directory}")
            with open(f"{directory}/{filename}.{input_format}", "wb") as file:
                file.write(file_bytes)
            logger.info(f"Сохранили файл {filename}.{input_format} в каталог {directory}")
            return True
        except Exception as e:
            logger.info(f"Ошибка сохранения файла {filename}: {e}")
            raise

    return True

async def featch_data_http_client(client: httpx.AsyncClient, flag=False, filename: str = ''):
    t_get_start = time.perf_counter()

    directory = r'C:\Users\beginin-ov\Projects\Local\work\temp_audio_files'
    input_format = 'mp3'
    response = await send_request(client=client, url='',method="GET", params = {'filename': filename})
    t_get = round(time.perf_counter() - t_get_start,2)
    #data = response['data']
    # logger.info(f"data: {data}")
    file_base64 = response.get("recipient_data").get('file')

    if not file_base64:
        logger.info(f"файл отсутствует")
        return False
    #logger.info(f"Время получения файла: {t_get} с")
    if flag==True:
        try:
            file_bytes = base64.b64decode(file_base64)  # Декодирование из base64
            if not os.path.exists(f"{directory}"):
                os.makedirs(f"{directory}")
            with open(f"{directory}/{filename}.{input_format}", "wb") as file:
                file.write(file_bytes)
            logger.info(f"Сохранили файл {filename}.{input_format} в каталог {directory}")
            return True
        except Exception as e:
            logger.info(f"Ошибка сохранения файла {filename}: {e}")
            raise

    return True



async def check_file_AppRecordLoarder() -> ResponseFormat:
    '''Функция для поиска файла через AppRecordLoarder'''

    filenames = ['']
    filenames = ['']
    async with AsyncHttpClient(url="") as client:
        for filename in filenames:
            request = RequestFormat(method="GET", endpoint="/download", params={'filename': filename}, return_type="bytes")
            result = await client.request_async(request)

            result = json.loads(result.data)  #
            # logger.info(result.get("data"))
            if not result["recipient_data"]["file"]:
                logger.info(f"файл {filename} на сервере отсутствует")
                return
            logger.info(f"файл {filename} получен")

#asyncio.run(check_file_AppRecordLoarder())

async def main():
    asyncio.create_task(lifestile_task(logger=logger, interval=300))  # запуск фоновой таски
    # Основная программа

def separation(data: list, input_dir: str):
    try:
        # Формируем полный путь к входному файлу
        # input_path = os.path.join(input_dir, input_file)

        # Создаем папку для результатов
        output_dir = os.path.join(input_dir, "results")
        os.makedirs(output_dir, exist_ok=True)

        # Полные пути к выходным файлам
        output_files = {
            'DLAPI': os.path.join(output_dir, 'DLAPI.txt'),
            'CP': os.path.join(output_dir, 'CP.txt'),
            'DLIVR': os.path.join(output_dir, 'DLIVR.txt'),
            'lost': os.path.join(output_dir, 'lost.txt')
        }
        # Открываем все выходные файлы для записи сразу
        with open(output_files['DLAPI'], 'a') as dlapi_file, \
                open(output_files['CP'], 'a') as cp_file, \
                open(output_files['DLIVR'], 'a') as dlivr_file, \
                open(output_files['lost'], 'a') as lost_file:

            counters = {'DLAPI': 0, 'CP': 0, 'DLIVR': 0, 'lost': 0}

            for line in data:
                if len(line)==0:
                    continue
                line = line.split(".mp3")[-2].split("/")[-1]
                if 'dlapi' in line.lower():
                    dlapi_file.write(line + '\n')
                    counters['DLAPI'] += 1
                elif 'cp' in line.lower():
                    cp_file.write(line + '\n')
                    counters['CP'] += 1
                elif 'dlivr' in line.lower():
                    dlivr_file.write(line + '\n')
                    counters['DLIVR'] += 1
                else:
                    lost_file.write(line + '\n')
                    counters['lost'] += 1

            logger.info("Обработка завершена!")
            for category, count in counters.items():
                logger.info(f"{category}: {count} файлов")
            logger.info(f"Всего: {sum(counters.values())} файлов")
            logger.info(f"Результаты сохранены в: {output_dir}")
        return counters
    except Exception as e:
        logger.info(f"Произошла ошибка: {e}")

async def split_names():
    """разбиваем один файл на файлы с разными сервисами"""
    # Основная программа
    big_file = FileManager()
    filenames = ['files_mp3_2025','files_in_archives_2025']

    for name in filenames:
        # считываем из файла частями
        for chunk in big_file.read_large_file_chunked(rf'C:\Users\beginin-ov\Projects\Local\files\{name}.txt', chunk_size=100000):
            #logger.info(chunk)
            separation(data = chunk, input_dir=r"C:\Users\beginin-ov\Projects\Local\files")
            continue

    logger.info("Основной цикл программы завершен!")

async def remove_double():
    """удаляем дубликаты в файле"""
    object = FileManager()
    object.remove_duplicates_large_file(input_file='DLAPI.txt')
    object.remove_duplicates_large_file(input_file='DLIVR.txt')
    #object.remove_duplicates_large_file(input_file='CP.txt')
    #object.remove_duplicates_large_file(input_file='lost.txt')

async def txt_to_csv():
    converter = DataConverter()
    converter.txt_to_csv(input_file=r"C:\Users\beginin-ov\Projects\Local\files\results\DLIVR_d.txt")
    #converter.txt_to_csv_chunked(input_file=r"C:\Users\beginin-ov\Projects\Local\files\results\CP_d.txt")
    converter.txt_to_csv(input_file=r"C:\Users\beginin-ov\Projects\Local\files\results\DLAPI_d.txt")

async def t1():
    await remove_double()
    await asyncio.sleep(60)
    logger.info(f"спим")
    await txt_to_csv()

#asyncio.run(split_names())
#asyncio.run(remove_double())
#asyncio.run(txt_to_csv())
#asyncio.run(t1())

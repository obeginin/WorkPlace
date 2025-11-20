from threading import Semaphore

import paramiko
import asyncio

import aiofiles



import httpx
#from Converter.Converter import DataConverter
from ClassLogger import LoggerConfig
from SSHClientClass import AsyncSSHClient

import logging
from pathlib import Path
import pandas as pd
import json

import re


logger_config = LoggerConfig(log_file='search_filenames_2025.log', log_level="INFO", console_output=True, use_json=False)
logger_config.setup_logger()
logger = logger_config.get_logger(__name__)
logger.info("Поиск архивов")

logging.getLogger("httpx").disabled = True
logging.getLogger("httpcore").disabled = True


async def process_single_folder(object, path, file_handle, semaphore):
    """Обрабатывает одну папку и сразу записывает в файл"""
    async with semaphore:
        try:
            archives = await object.find_tar_archives_2(search_path=path)
            if archives:
                async with asyncio.Lock():  # Блокировка для записи
                    for tar in archives:
                        logger.info(f"Found: {tar}")
                        await file_handle.write(f"{tar}\n")
                        await file_handle.flush()

        except Exception as e:
            logger.error(f"Error processing {path}: {e}")



async def mainnn():
    '''Функция для поиска всех tar архивов'''
    try:
        ssh_client = AsyncSSHClient(host='dialer-store2.dmz.local', username='beginin-ov', password='jXwMjKuamyAholbLMTQ2')
        semaphore = asyncio.Semaphore(5)

        #await object.find_asterisk()
        #await object.search_mp3_calc()
        # ищем базовые папки
        folders = await ssh_client.find_folders()
        logger.info(f'папки:{folders}')
        tasks_folders = []
        path_tar_list = r'C:\Users\beginin-ov\Projects\Local\files\tar_list_2025.txt'

        async with aiofiles.open(path_tar_list, 'a', encoding='utf-8') as f:
            tasks = []

            for folder in folders:
                path = f"{folder}/2025/"
                task = asyncio.create_task(process_single_folder(ssh_client, path, f, semaphore))
                tasks.append(task)

            await asyncio.gather(*tasks)

    except Exception as e:
        logger.exception(f"Исключение в основной функции main: {e}")
        return

asyncio.run(mainnn())

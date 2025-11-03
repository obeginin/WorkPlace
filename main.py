from aiohttp import request

from ClassFiles import FileManager
from ClassLogger import LoggerConfig
from ClassHTTP import AsyncHttpClient, RequestFormat, ResponseFormat, async_test_http, async_tests_http
from ClassConverter import DataConverter

import asyncio
import aiofiles
import logging
import os
import sys

async def lifestile_task(logger: logging.Logger, interval: int = 300):
    '''Проверка жизни программы'''
    logger.info("Service lifestile: running OK (startup)")  # сразу при запуске
    while True:
        logger.info("Service lifestile: running OK")
        await asyncio.sleep(interval)

async def main():
    # инициализируем основное логирование
    logger_config = LoggerConfig(log_file='app.log', log_level="INFO",console_output=True, use_json=False)
    logger_config.setup_logger()  # вызываем 1 раз в основном проекте
    logger = logger_config.get_logger(__name__)
    logger.info("Основной файл main")

    asyncio.create_task(lifestile_task(logger=logger, interval=300)) # запуск фоновой таски
    # Основная программа

    for i in range (1,2):
        #response = await async_test_http(logger=logger)
        response = await async_tests_http(logger=logger)
        logger.info(f"data {response}")
        logger.info(f"Спим {i} секунд!")
        await asyncio.sleep(i)


    logger.info("Основной цикл программы завершен!")

if __name__ == "__main__":
    asyncio.run(main())

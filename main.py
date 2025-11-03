from ClassFiles import FileManager
from ClassLogger import LoggerConfig
from ClassHTTP import AsyncHttpClient
from ClassConverter import DataConverter

import asyncio
import aiofiles
import logging
import os
import sys

async def lifestile_task(logger: logging.Logger, interval: int = 300):
    '''Проверка жизни программы'''
    while True:
        logger.info("Service lifestile: running OK")
        await asyncio.sleep(interval)

async def main():
    # инициализируем основное логирование
    logger_config = LoggerConfig(log_file='main.log', log_level="INFO",console_output=True, use_json=False)
    logger_config.setup_logger()  # вызываем 1 раз в основном проекте
    logger = logger_config.get_logger(__name__)
    logger.info("Основной файл main")
    asyncio.create_task(lifestile_task(logger=logger, interval=300))
    for i in range (1,10):
        logger.info(f"Спим {i} секунд!")
        await asyncio.sleep(i)

if __name__ == "__main__":
    asyncio.run(main())

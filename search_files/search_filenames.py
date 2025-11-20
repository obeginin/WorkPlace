import os
import zipfile
import tarfile
import logging
import asyncio
import os
import argparse
from pathlib import Path
import sys
from pathlib import Path
from ClassLogger import LoggerConfig
import aiofiles
from concurrent.futures import ThreadPoolExecutor

logger_config = LoggerConfig(base_dir=Path(r"/home/beginin-ov/search_filenames"), log_file='_search_filenames.log', console_output=False)
logger_config.setup_logger()
logger = logging.getLogger(__name__)
logger.info("Поиск имен файлов аудиозаписей из архивов ")

INPUT_FILE = "tar_list_2025_new_2.txt"
OUTPUT_FILE_ARCH = "_files_in_archives.txt"
OUTPUT_FILE_MP3 = "_files_mp3.txt"

'''Два класса ArchiveProcessor и MP3Finder для поиска аудиофалов в архивах и в папках на сервере'''


class ArchiveProcessor:
    '''
    Класс для поиска имен аудиофайлов из архивов
    arg:  start - с какой строки начинать
    '''
    def __init__(self, input_file, output_file, start_from: int = 0):

        self.INPUT_FILE = input_file
        self.OUTPUT_FILE_ARCH = output_file
        self.START_FROM = start_from

        # Создаем семафор и блокировку
        self.semaphore = asyncio.Semaphore(10)
        self.file_lock = asyncio.Lock()

        # Логгер (предполагается, что он уже определен)
        base_logger = logging.getLogger(__name__)
        self.logger = base_logger.getChild(f"ArchiveProcessor{id(self)}")

    def _sync_process_tar(self, archive_path):
        """Синхронная обработка tar-архива (выполняется в отдельном потоке)"""
        try:
            with tarfile.open(archive_path, 'r') as t:
                return [member.name for member in t.getmembers() if member.isfile()]
        except Exception as e:
            self.logger.info(f"Ошибка при чтении tar {archive_path}: {e}")
            return None

    async def process_tar(self, archive_path):
        """Асинхронно обрабатывает tar-архив и возвращает список файлов"""
        loop = asyncio.get_event_loop()

        # Запускаем синхронную операцию в отдельном потоке
        with ThreadPoolExecutor() as executor:
            try:
                result = await loop.run_in_executor(
                    executor,
                    self._sync_process_tar,
                    archive_path
                )
                return result
            except Exception as e:
                self.logger.info(f"Ошибка при обработке tar {archive_path}: {e}")
                return None

    async def process_single_archive(self, arch):
        """Обрабатывает один архив и записывает результат"""
        async with self.semaphore:
            try:
                if not os.path.exists(arch):
                    self.logger.info(f"[ERROR] Archive not found: {arch}")
                    return

                files = await self.process_tar(arch)

                if files is None:
                    self.logger.info(f"[SKIPPED] Unsupported format или ошибка: {arch}")
                    return

                self.logger.info(f"=== Archive: {arch} === (найдено {len(files)} файлов)")

                # Используем глобальную блокировку для записи
                async with self.file_lock:
                    async with aiofiles.open(self.OUTPUT_FILE_ARCH, "a") as out_async:
                        for filename in files:
                            await out_async.write(filename + "\n")
                        await out_async.write("\n")
                        await out_async.flush()  # Принудительно записываем в файл

            except Exception as e:
                self.logger.info(f"Исключение с архивом {arch}: {str(e)}")

    async def process_all_archives(self):
        """Основной метод для обработки всех архивов"""
        if not os.path.exists(self.INPUT_FILE):
            self.logger.info(f"Файл {self.INPUT_FILE} не найден")
            return False

        with open(self.INPUT_FILE, "r") as f:
            archive_paths = [line.strip() for line in f if line.strip()]

        # Создаем и запускаем задачи для всех архивов
        tasks = [
            self.process_single_archive(arch)
            for arch in archive_paths[self.START_FROM:]
        ]

        self.logger.info(f"Начата обработка {len(tasks)} архивов (с {self.START_FROM} позиции)...")

        # Обрабатываем ВСЕ задачи сразу - семафор сам ограничит параллелизм
        await asyncio.gather(*tasks, return_exceptions=True)

        self.logger.info(f"Готово! Результат записан в {self.OUTPUT_FILE_ARCH}")
        return True

    async def get_statistics(self):
        """Возвращает статистику по обработанным данным"""
        if not os.path.exists(self.OUTPUT_FILE_ARCH):
            return {"status": "output_file_not_exists"}

        try:
            async with aiofiles.open(self.OUTPUT_FILE_ARCH, "r") as f:
                content = await f.read()
                lines = content.splitlines()
                archives_count = content.count("===")
                files_count = len([line for line in lines if line and not line.startswith("===")])

            return {
                "status": "success",
                "archives_processed": archives_count,
                "total_files": files_count,
                "output_file": self.OUTPUT_FILE_ARCH
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}



async def task_1():
    """Функция для поиска всех аудиофайлов из архивов"""
    processor = ArchiveProcessor(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE_ARCH,
    )

    success = await processor.process_all_archives()

    if success:
        stats = await processor.get_statistics()
        processor.logger.info(f"Статистика поиска mp3 файлов в архивах: {stats}")

    return success


class MP3Finder:
    def __init__(self, base_dir, year, output_file, follow_symlinks=False, max_concurrent=10):
        self.base_dir = Path(base_dir)
        self.year = year
        self.output_file = Path(output_file)
        self.follow_symlinks = follow_symlinks
        self.max_concurrent = max_concurrent

        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.file_lock = asyncio.Lock()
        self.found_count = 0

        base_logger = logging.getLogger(__name__)
        self.logger = base_logger.getChild(f"MP3Finder_{id(self)}")

    def scan_dirs(self):
        '''Функция итератор по поддиректориям первой глубины'''
        try:
            with os.scandir(self.base_dir) as it:
                return [Path(entry.path) for entry in it if entry.is_dir(follow_symlinks=False)]
        except (FileNotFoundError, PermissionError) as e:
            self.logger.exception(f"Ошибка доступа к {self.base_dir}: {e}")
            return []

    async def iter_depth1_subdirs_async(self):
        """Асинхронный обертка"""
        loop = asyncio.get_event_loop()

        with ThreadPoolExecutor() as executor:
            dirs = await loop.run_in_executor(executor, self.scan_dirs)
            for dir_path in dirs:
                yield dir_path

    def scan_directory(self, directory):
        """ищет MP3 файлы в директории"""
        mp3_files = []
        try:
            for dirpath, dirnames, filenames in os.walk(directory, followlinks=self.follow_symlinks):
                for filename in filenames:
                    if filename.lower().endswith(".mp3"):
                        full_path = str(Path(dirpath) / filename)
                        mp3_files.append((full_path, Path(full_path).name))
        except Exception as e:
            self.logger.exception(f"Ошибка при сканировании {directory}: {e}")
        return mp3_files

    async def find_mp3_in_directory_async(self, directory):
        '''Асинхронная обертка'''
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(executor, self.scan_directory, directory)

    async def process_single_directory(self, sub_dir):
        """Обрабатывает одну директорию и записывает найденные MP3"""
        async with self.semaphore:
            try:
                target = sub_dir / self.year
                if not target.exists() or not target.is_dir():
                    self.logger.info(f"пропускаем, нет папки year: {target}")
                    return 0

                self.logger.info(f"обрабатываем папку: {target}")
                mp3_files = await self.find_mp3_in_directory_async(target)

                if mp3_files:
                    async with self.file_lock:
                        async with aiofiles.open(self.output_file, "a", encoding="utf-8") as f:
                            for full_path, filename in mp3_files:
                                await f.write(filename + "\n")

                    return len(mp3_files)
                return 0

            except Exception as e:
                self.logger.exception(f"Ошибка при обработке {sub_dir}: {e}")
                return 0

    async def find_all_mp3(self, to_stdout=False):
        """Основной метод для поиска всех MP3 файлов"""
        if not self.base_dir.exists() or not self.base_dir.is_dir():
            self.logger.error(f"Базовая директория не существует или не является директорией: {self.base_dir}")
            return False

        # Очищаем или создаем выходной файл
        async with aiofiles.open(self.output_file, "w", encoding="utf-8") as f:
            await f.write("")

        tasks = []
        async for sub_dir in self.iter_depth1_subdirs_async():
            task = asyncio.create_task(self.process_single_directory(sub_dir))
            tasks.append(task)

        # Ждем завершения всех задач и суммируем результаты
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Считаем общее количество найденных файлов
        total_found = 0
        for result in results:
            if isinstance(result, int):
                total_found += result
            elif isinstance(result, Exception):
                self.logger.error(f"Ошибка в задаче: {result}")

        self.found_count = total_found

        # Если нужно вывести в stdout, читаем и выводим результаты
        if to_stdout:
            await self.print_results_to_stdout()

        self.logger.info(f"Готово. Найдено {total_found} .mp3 файлов. Результаты записаны в: {self.output_file}")
        return True

    async def print_results_to_stdout(self):
        """Выводит найденные пути в stdout"""
        try:
            async with aiofiles.open(self.output_file, "r", encoding="utf-8") as f:
                content = await f.read()
                for line in content.strip().split('\n'):
                    if line.strip():
                        print(line.strip())
        except Exception as e:
            self.logger.error(f"Ошибка при чтении результатов для stdout: {e}")

    async def get_statistics(self):
        """Возвращает статистику поиска"""
        return {
            "base_directory": str(self.base_dir),
            "year": self.year,
            "output_file": str(self.output_file),
            "files_found": self.found_count,
            "follow_symlinks": self.follow_symlinks
        }


async def task_2():
    """Асинхронная версия поиска MP3 файлов"""
    p = argparse.ArgumentParser()
    p.add_argument("--base", type=str, default="/storage/records")
    p.add_argument("--year", type=str, default="2025")
    p.add_argument("--output", type=str, default=OUTPUT_FILE_MP3)
    p.add_argument("--follow-symlinks", action="store_true")
    p.add_argument("--stdout", action="store_true")
    args = p.parse_args()

    # Создаем экземпляр finder
    finder = MP3Finder(
        base_dir=args.base,
        year=args.year,
        output_file=args.output,
        follow_symlinks=args.follow_symlinks,
        max_concurrent=10
    )

    # Запускаем поиск
    success = await finder.find_all_mp3(to_stdout=args.stdout)

    if success:
        stats = await finder.get_statistics()
        finder.logger.info(f"Статистика поиска mp3 файлов в папках: {stats}")

    return success


async def main():
    await asyncio.gather(task_1(), task_2())
    #await task_1()
asyncio.run(main())
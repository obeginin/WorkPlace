import time
import zipfile
import tarfile
import base64
import os
import argparse
from pathlib import Path
import sys
from pathlib import Path
from typing import List, Union, Generator, Any
import asyncio
import aiofiles
import json
import logging
from ClassLogger import LoggerConfig
# logger_config = LoggerConfig(log_file='ClassFiles.log', log_level= "INFO")
# logger_config.setup_logger()
# logger = logger_config.get_logger(__name__)
# logger.info("Класс ClassFiles")




# Настраиваем логгер
'''logger_cfg = LoggerConfig(base_dir=Path(__file__).resolve().parent, log_file="files.log", use_json=False)
logger_cfg.setup_logger()
logger = logger_cfg.get_logger("FileManager")
fm = FileManager(logger=logger)'''






class Files:
    def __init__(self):
        self.file_lock = asyncio.Lock()

    async def save_file(self, *, filename: str, file_base64: str, input_format: str) -> bool:
        """Сохраняет файл на диск"""
        try:
            file_bytes = base64.b64decode(file_base64)  # Декодирование из base64
            if not os.path.exists(f"./{self.directory}"):
                os.makedirs(f"./{self.directory}")
            with open(f"{self.directory}/{filename}.{input_format}", "wb") as file:
                file.write(file_bytes)
            #print(f"Сохранили файл {filename}.{input_format} в каталог {self.directory}")
            return True
        except Exception as e:
            print(f"Ошибка сохранения файла {filename}: {e}")
            raise

    async def save_file_async(self, filename: str):
        async with self.file_lock:
            async with aiofiles.open(self.not_files, 'a', encoding='utf-8') as f:
                await f.write(f"{filename}\n")

'''Готовы функции:
new_dir_exists, write_json_async, read_large_file, read_large_file_chunked
_resolve_path, _log_info'''
class FileManager:
    """Универсальный класс для безопасной работы с файлами и логированием"""
    _json_lock = asyncio.Lock()  # общий асинхронный замок
    def __init__(self, base_dir: str | Path | None = None, logger: logging.Logger | None = None):
        project_root = Path(__file__).resolve().parent
        self.base_dir = Path(base_dir) if base_dir else project_root / "files"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)

    def new_dir_exists(self, path: Path) -> None:
        """Создает директорию, если её нет"""
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._log_error(f"Не удалось создать директорию для {path}: {e}")
            raise

    async def write_json_async(self, file_path: str | Path, data_list: list[Any], append: bool = False, indent: int = None, encoding: str = "utf-8") -> bool:
        """Асинхронная запись JSON файла с блокировкой
        :arg
            file_path имя для файла
            data_list список сообщение для записи (записываем каждый элемент построчно)
            indent отступы задать числом (None - в строку
            append - добавять в файл или перезаписываться
        """
        path = self._resolve_path(file_path) # получаем абсолютный путь
        self.new_dir_exists(path)   # создаем директорию
        async with self._json_lock:
            try:
                mode = "a" if append else "w"
                async with aiofiles.open(path, mode, encoding=encoding) as f:
                    lines = (json.dumps(item, ensure_ascii=False , indent=indent) + "\n" for item in data_list)
                    await f.writelines(lines)
                self._log_info(f"Добавлено {len(data_list)} записей в {path}")
                return True
            except Exception as e:
                self._log_error(f"Ошибка при асинхронной записи JSON в {path}: {e}")
                return False

    #  Чтение большого файла как итератора

    def read_large_file(self, file_path: str | Path, encoding: str = "utf-8") -> Generator[str, None, None]:
        """
        Безопасно читает большой файл построчно (генератор).
        Пример:
            for line in fm.read_large_file("big.log"):
                process(line)
        """
        path = self._resolve_path(file_path)
        if not path.exists() or not path.is_file():
            self._log_error(f"Файл {path} не найден или это не файл.")
            yield from ()  # Возвращаем пустой генератор
            return

        try:
            self._log_info(f"Открытие файла для построчного чтения: {path}")
            with open(path, "r", encoding=encoding) as f:
                for i, line in enumerate(f, start=1):
                    yield line.rstrip("\n")

                # Можно добавить отладочную информацию о количестве строк
                self._log_info(f"Файл {path} успешно прочитан, всего строк: {i}")
        except Exception as e:
            self._log_error(f"Ошибка при чтении большого файла {path}: {e}")
            yield from ()

    def read_large_file_chunked(
            self,
            file_path: str | Path,
            chunk_size: int = 10_000,
            encoding: str = "utf-8",
    ) -> Generator[list[str], None, None]:
        """
        Читает большой файл чанками (пакетами строк) с прогрессом.
        Пример:
            for chunk in fm.read_large_file_chunked("big.log", chunk_size=5000):
                process(chunk)
        """
        path = self._resolve_path(file_path)

        if not path.exists() or not path.is_file():
            self._log_error(f"Файл {path} не найден или это не файл.")
            yield from ()
            return

        try:
            self._log_info(f"Подсчёт строк в файле: {path}")
            with open(path, "r", encoding=encoding) as f:
                total_lines = sum(1 for _ in f)
            self._log_info(f"Всего строк в файле: {total_lines}")

            self._log_info(f"Начато чтение файла чанками: {path} (chunk_size={chunk_size})")

            processed_lines = 0
            chunk: list[str] = []

            with open(path, "r", encoding=encoding) as f:
                for line in f:
                    chunk.append(line.rstrip("\n"))
                    processed_lines += 1

                    if len(chunk) >= chunk_size:
                        percent = processed_lines / total_lines * 100
                        self._log_info(f"Прогресс: {processed_lines}/{total_lines} строк ({percent:.1f}%)")
                        yield chunk
                        chunk = []

                # оставшиеся строки
                if chunk:
                    self._log_info(f"Прогресс: {processed_lines}/{total_lines} строк (100%)")
                    yield chunk

            self._log_info(f"Чтение файла {path.name} завершено успешно.")

        except Exception as e:
            self._log_error(f"Ошибка при чтении файла {path}: {e}")
            yield from ()


    # -------------------------------------------------------
    #  Запись строк в TXT
    # -------------------------------------------------------
    def write_lines(self, file_path: Union[str, Path], lines: List[str], encoding: str = "utf-8") -> bool:
        """Записывает список строк построчно в txt-файл"""
        path = self._resolve_path(file_path)
        self.new_dir_exists(path)
        try:
            with open(path, "w", encoding=encoding) as f:
                for line in lines:
                    f.write(f"{line}\n")
            self._log_info(f"Файл {path} успешно записан ({len(lines)} строк).")
            return True
        except Exception as e:
            self._log_error(f"Ошибка при записи в {path}: {e}")
            return False
    # -------------------------------------------------------
    #  Добавление строк в файл
    # -------------------------------------------------------
    def append_lines(self, file_path: Union[str, Path], lines: List[str], encoding: str = "utf-8") -> bool:
        """Добавляет строки в конец файла"""
        path = self._resolve_path(file_path)
        self.new_dir_exists(path)
        try:
            with open(path, "a", encoding=encoding) as f:
                for line in lines:
                    f.write(f"{line}\n")
            self._log_info(f"Добавлено {len(lines)} строк в {path}.")
            return True
        except Exception as e:
            self._log_error(f"Ошибка при добавлении строк в {path}: {e}")
            return False
    # -------------------------------------------------------
    #  Запись в JSON
    # -------------------------------------------------------
    def write_json(self, file_path: Union[str, Path], data: Any, encoding: str = "utf-8", indent: int = 4) -> bool:
        """Сохраняет данные в JSON"""
        path = self._resolve_path(file_path)
        self.new_dir_exists(path)
        try:
            with open(path, "w", encoding=encoding) as f:
                json.dump(data, f, ensure_ascii=False, indent=indent)
            self._log_info(f"JSON файл {path} успешно записан.")
            return True
        except Exception as e:
            self._log_error(f"Ошибка при записи JSON в {path}: {e}")
            return False


        #  Чтение JSON
        # -------------------------------------------------------
    def read_json(self, file_path: Union[str, Path], encoding: str = "utf-8") -> Any:
        """Синхронное чтение JSON"""
        path = self._resolve_path(file_path)
        if not path.exists():
            self._log_error(f"Файл {path} не найден.")
            return None
        try:
            with open(path, "r", encoding=encoding) as f:
                data = json.load(f)
            self._log_info(f"JSON файл {path} успешно прочитан.")
            return data
        except Exception as e:
            self._log_error(f"Ошибка при чтении JSON из {path}: {e}")
            return None

        # -------------------------------------------------------
    #  Асинхронное чтение JSON
    # -------------------------------------------------------
    async def read_json_async(self, file_path: Union[str, Path], encoding: str = "utf-8") -> Any:
        """Асинхронное чтение JSON"""
        path = self._resolve_path(file_path)
        if not path.exists():
            self._log_error(f"Файл {path} не найден.")
            return None
        try:
            async with aiofiles.open(path, "r", encoding=encoding) as f:
                content = await f.read()
                data = json.loads(content)
            self._log_info(f"JSON файл {path} успешно прочитан (async).")
            return data
        except Exception as e:
            self._log_error(f"Ошибка при асинхронном чтении JSON из {path}: {e}")
            return None




    #  Вспомогательные методы
    def _resolve_path(self, file_path: Union[str, Path]) -> Path:
        """Возвращает абсолютный путь"""
        p = Path(file_path)
        return p if p.is_absolute() else self.base_dir / p

    def _log_info(self, message: str) -> None:
        self.logger.info(f"FileManager - {message}")

    def _log_error(self, message: str) -> None:
        self.logger.error(f"FileManager - {message}")

        #  Запись большого файла построчно (стриминг)
    def write_large_file(
            self,
            file_path: Union[str, Path],
            lines: Union[List[str], Generator[str, None, None], Any],
            mode: str = "a",
            encoding: str = "utf-8"
    ) -> bool:
        """
        Потоковая запись большого файла построчно.
        Можно передавать как список строк, так и генератор.
        Пример:
            fm.write_large_file("output.log", ("строка" for строка in data))
        """
        path = self._resolve_path(file_path)
        self.new_dir_exists(path)
        try:
            with open(path, mode, encoding=encoding) as f:
                for line in lines:
                    f.write(f"{line}\n")
            self._log_info(f"Файл {path} успешно записан (write_large_file, mode={mode}).")
            return True
        except Exception as e:
            self._log_error(f"Ошибка при записи большого файла {path}: {e}")
            return False
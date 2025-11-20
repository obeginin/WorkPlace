import datetime
import logging
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, Union
import json
import threading

class JsonFormatter(logging.Formatter):
    """Json формат логов"""
    def __init__(self):
        # Передаем None чтобы избежать проблем с форматом
        super().__init__(fmt=None, datefmt=None)

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.datetime.now().isoformat(timespec='milliseconds')  # Используем текущее время с миллисекундами

        log_record = {
            "timestamp": timestamp,
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage(),
        }

        # Добавляем trace_id если есть
        trace_id = getattr(record, "trace_id", None)
        if trace_id:
            log_record["trace_id"] = trace_id

        # Добавляем информацию об исключении если есть
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record, ensure_ascii=False, default=str)

class SmartTimedRotatingFileHandler(TimedRotatingFileHandler):
    """Ротирует логи в формате app.YYYY-MM-DD.log"""
    def __init__(self, filename, when="midnight", interval=1, backupCount=30, encoding="utf-8"):
        base, ext = Path(filename).stem, Path(filename).suffix
        self.baseFilenameNoExt = str(Path(filename).parent / base)
        self.ext = ext
        super().__init__(filename, when=when, interval=interval, backupCount=backupCount, encoding=encoding)
        self.suffix = "%Y-%m-%d"

    def rotation_filename(self, default_name: str) -> str:
        date_str = datetime.now().strftime(self.suffix)
        return f"{self.baseFilenameNoExt}.{date_str}{self.ext}"

class LoggerConfig:
    """Универсальный класс для настройки логирования"""
    _lock = threading.Lock()  # защита от гонок при многопоточном вызове
    def __init__(
        self,
        base_dir: Optional[Union[str, Path]] = None,
        log_dir: Optional[Union[str, Path]] = None,
        log_file: str = "app.log",
        log_level: str = "INFO",
        log_format: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        when: str = "midnight",
        interval: int = 1,
        backup_count: int = 30,
        encoding: str = "utf-8",
        console_output: bool = True,
        use_json: bool = False
    ):
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).resolve().parent
        self.log_dir = self._resolve_log_dir(log_dir)
        self.log_file = log_file
        self.log_level = log_level.upper()
        self.log_format = log_format
        self.when = when
        self.interval = interval
        self.backup_count = backup_count
        self.encoding = encoding
        self.console_output = console_output    # флаг для вывода в консоль
        self.use_json = use_json                # флаг для json логов
        self.app_logger_name = self.log_file.replace(".log", "")    # имя для файла с логами
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Не удалось создать директорию для логов {self.log_dir}: {e}")

    def _resolve_log_dir(self, log_dir: Optional[Union[str, Path]] = None) -> Path:
        'определям путь к директории для логов'
        if log_dir is None:
            return self.base_dir / "logs"
        elif Path(log_dir).is_absolute():
            return Path(log_dir)
        else:
            return self.base_dir / log_dir

    def setup_logger(self) -> None:
        """Настройка логирования с защитой от повторной инициализации"""
        with self._lock:
            root = logging.getLogger()
            if root.handlers:
                return  # уже настроено
            log_path = self.log_dir / self.log_file
            # Выбираем форматтер
            formatter = JsonFormatter() if self.use_json else logging.Formatter(self.log_format)
            # Файловый обработчик с ротацией
            file_handler = SmartTimedRotatingFileHandler(
                filename=str(log_path),
                when=self.when,
                interval=self.interval,
                backupCount=self.backup_count,
                encoding=self.encoding
            )
            file_handler.setFormatter(formatter)
            handlers = [file_handler]
            if self.console_output:
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(formatter)
                handlers.append(console_handler)
            for h in handlers:
                root.addHandler(h)
            root.setLevel(getattr(logging, self.log_level, logging.INFO))
            logging.info(f"Логирование инициализировано. Запущен файл: {self.app_logger_name} Лог: {log_path}")

    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """Именованный логгер (по умолчанию __name__)"""
        logger = logging.getLogger(name or self.app_logger_name)
        #logger.propagate = False  # избегаем дублирования в root
        return logger

    def update_level(self, new_level: str) -> None:
        """Динамическое изменение уровня логирования"""
        self.log_level = new_level.upper()
        logging.getLogger().setLevel(getattr(logging, self.log_level, logging.INFO))
        logging.info(f"Уровень логирования изменен на: {self.log_level}")

    def get_log_path(self) -> Path:
        'путь '
        return self.log_dir / self.log_file

    def __repr__(self) -> str:
        return (f"LoggerConfig(log_dir={self.log_dir}, log_file={self.log_file}, "
                f"level={self.log_level}, console={self.console_output}, use_json={self.use_json})")




'''
для запуска
logger_config = LoggerConfig(log_file='ClassFiles.log', console_output=False)
logger_config.setup_logger()
logger = logger_config.get_logger(__name__)
logger.info("Класс ClassFiles")
'''

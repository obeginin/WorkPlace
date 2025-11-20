import logging
import time
import aiohttp
import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, Literal
from json import JSONDecodeError
import socket

@dataclass
class ErrorInfo:
    """Структура результата обработки ошибки"""
    message: str
    type: str
    level: str  # info | warning | error | critical
    context: str | None = None


class ErrorHandler:
    """Централизованная обработка ошибок и формирование сообщений."""
    def __init__(self, logger: logging.Logger | None = None):
        self.logger = logger

    async def handle(self, e: Exception, context: str = "") -> ErrorInfo:
        """Главный метод обработки ошибок"""
        # --- Классификация по типу ---
        if isinstance(e, asyncio.TimeoutError): info = ErrorInfo("Сервер не ответил вовремя (TimeoutError).", "TimeoutError", "warning", context)
        elif isinstance(e, aiohttp.ClientConnectorError): info = ErrorInfo("Ошибка подключения к серверу (ClientConnectorError).", "ClientConnectorError", "error", context)
        elif isinstance(e, aiohttp.ClientResponseError): info = ErrorInfo(f"Ошибка HTTP-ответа: {e.status} {e.message}", "ClientResponseError", "error", context)
        elif isinstance(e, aiohttp.ClientPayloadError): info = ErrorInfo("Ошибка чтения тела ответа (ClientPayloadError).", "ClientPayloadError", "error", context)
        elif isinstance(e, aiohttp.ClientError): info = ErrorInfo(f"Ошибка клиента aiohttp: {e}", "ClientError", "error", context)
        elif isinstance(e, JSONDecodeError): info = ErrorInfo("Ошибка декодирования JSON-ответа.", "JSONDecodeError", "error", context)
        elif isinstance(e, UnicodeDecodeError): info = ErrorInfo("Ошибка декодирования текста (UnicodeDecodeError).", "UnicodeDecodeError", "error", context)
        elif isinstance(e, ValueError): info = ErrorInfo(f"Некорректное значение: {e}", "ValueError", "warning", context)
        elif isinstance(e, OSError): info = ErrorInfo(f"Системная ошибка ввода-вывода: {e}", "OSError", "error", context)
        elif isinstance(e, socket.gaierror): info = ErrorInfo("Ошибка DNS или сети (gaierror).", "SocketError", "error", context)
        elif isinstance(e, AssertionError): info = ErrorInfo(f"Ошибка проверки данных (AssertionError): {e}", "AssertionError", "error", context)
        elif isinstance(e, KeyboardInterrupt): info = ErrorInfo("Процесс прерван пользователем.", "KeyboardInterrupt", "critical", context)
        else: info = ErrorInfo(f"Непредвиденная ошибка: {type(e).__name__} — {e}", "UnexpectedError", "error", context)

        # --- Логирование ---
        log_message = f"[{info.context}] {info.message}" if info.context else info.message

        match info.level:
            case "info": self.logger.info(log_message)
            case "warning": self.logger.warning(log_message)
            case "error": self.logger.error(log_message)
            case "critical": self.logger.critical(log_message)

        return info

# Формат запроса
@dataclass
class RequestFormat:
    """Хранит параметры HTTP-запроса"""
    method: str
    endpoint: str
    params: Dict[str, Any] | None = None
    json: Dict[str, Any] | None = None
    data: dict[str, Any] | str | bytes | None = None
    headers: dict[str, str] | None = None
    return_type: Literal["json", "text", "bytes"] = "json"

    def __post_init__(self):
        """Просто приводим метод к верхнему регистру"""
        self.method = self.method.upper()

    def __repr__(self):
        return f"<RequestFormat method={self.method} endpoint={self.endpoint} params={self.params} json={self.json} data={self.data} headers={self.headers} return_type={self.return_type}>"

# Формат ответа
@dataclass
class ResponseFormat:
    """Хранит результат HTTP-запроса"""
    status: int | None
    data: Any
    url: str
    error: str | None = None
    success: bool = field(init=False)
    execute_time: float | None = None
    used_attempts: int = 0

    def __post_init__(self):
        """Автоматически определяем успешность запроса"""
        self.success = self.status is not None and 200 <= self.status < 300

    def __repr__(self):
        """для логов"""
        return f"<ResponseFormat status={self.status} success={self.success} url={self.url} data={self.data}>"

    @property
    def is_json(self) -> bool:
        """True, если data является JSON (dict)"""
        return isinstance(self.data, dict)

    @property
    def is_text(self) -> bool:
        """True, если data является строкой"""
        return isinstance(self.data, str)

    @property
    def is_bytes(self) -> bool:
        """True, если data является байтами"""
        return isinstance(self.data, bytes)

    @property
    def error_message(self) -> str | None:
        """Возвращает сообщение ошибки из стандартных полей ответа"""
        if isinstance(self.data, dict):
            return self.data.get("message") or self.data.get("msg") or self.data.get("error")
        return None

    
# Основной класс клиента
# ---------------------------
class AsyncHttpClient:
    """Асинхронный HTTP клиент с пулом соединений и гибким форматом ответа"""
    def __init__(
        self,
        url: str = "",
        timeout: float = 3.0,
        max_retries: int = 2,
        headers: dict[str, str] | None = None,
        limit: int = 100,
        limit_per_host: int = 10,
        verify_ssl: bool = True,
    ):
        self.url = url.rstrip("/")                # убирает лишний слэш в конце
        self.timeout = aiohttp.ClientTimeout(total=timeout) # Таймаут для всех HTTP-запросов в секундах.
        self.max_retries = max_retries                      # Количество повторных попыток при ошибках или таймаутах.
        self.default_headers = headers or {}                # Словарь заголовков по умолчанию для всех запросов.
        
        #  Явный пул соединений
        self.connector = aiohttp.TCPConnector(
            limit=limit,                        # максимум соединений одновременно (для всего клиента).
            limit_per_host=limit_per_host,      # максимум соединений на один хост.
            ttl_dns_cache=300,                  # кеширование DNS 300 секунд.
            ssl=verify_ssl,                     # проверять ли SSL-сертификаты
        )
        self.session: aiohttp.ClientSession | None = None
        self.logger = logging.getLogger(__name__)
        self.error_handler = ErrorHandler(self.logger)

    """
    Преимущественнее КМ
    async with AsyncHttpClient(url="") as client:
        response = await client.request_async(request)
    # Сессия автоматически закрыта после выхода из блока
    """
    async def __aenter__(self):
        """гарантирует что сессия создана (через КМ async with)"""
        await self._ensure_session()
        return self

    async def __aexit__(self, *args):
        """автоматическое закрытие сессии (через КМ async with)"""
        await self.close()
        
    
    """
    client = AsyncHttpClient(url="")
    await client._ensure_session()         # создаём сессию
    response = await client.request_async(request)
    await client.close()   
    """
    # Управление сессией
    async def _ensure_session(self):
        """создание сессии, если её ещё нет или она закрыта. (без КМ, явное открытие)"""
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                headers=self.default_headers,
                timeout=self.timeout,
            )

    async def close(self):
        """Закрывает сессию вручную (без КМ, явное закрытие)"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def request_async(self, request: RequestFormat) -> ResponseFormat:
        """Основной универсальный метод для HTTP-запросов"""
        await self._ensure_session()
        url = request.endpoint if request.endpoint.startswith("http") else f"{self.url}{request.endpoint}"
        merged_headers = {**self.default_headers, **(request.headers or {})}
        status: int | None = None
        error: str | None = None  # инициализация перед циклом
        content: Any = None
        start_time = time.perf_counter()
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.debug(f"Request attempt {attempt + 1}: {request.method} {url} | params={request.params} json={request.json} data={request.data}")

                async with self.session.request(
                        method=request.method,
                        url=url,
                        params=request.params,
                        data=request.data,
                        json=request.json,
                        headers=merged_headers,
                ) as response:
                    status = response.status

                    if request.return_type == "json":
                        try:
                            content = await response.json(content_type=None)
                        except (aiohttp.ContentTypeError, JSONDecodeError):
                            content = await response.text()
                    elif request.return_type == "text":
                        content = await response.text()
                    elif request.return_type == "bytes":
                        content = await response.read()
                    else:
                        error = f"Unsupported return_type: {request.return_type}"
                        end_time = time.perf_counter() - start_time
                        self.logger.error(f"Unsupported return_type in request: {request}")
                        return ResponseFormat(status=None, data=None,url=url, error=error, execute_time=end_time, used_attempts=attempt)
                    # если успешный ответ или ошибка return_type, выходим
                    self.logger.debug(f"Response received: status={status} content_type={type(content)}")
                    break


            except Exception as e:
                if self.error_handler:
                    err_info = await self.error_handler.handle(e, context=f"{request.method} {url}")
                    error = err_info.message
                else:
                    error = str(e)
                    #self.logger.warning(f"Attempt {attempt + 1} failed for {request.method} {url}: {error}")

            if attempt < self.max_retries:
                await asyncio.sleep(2 ** attempt)

        end_time = time.perf_counter() - start_time
        if error:
            self.logger.error(f"Request failed after {attempt + 1} attempts: {request.method} {url} | Error: {error}")
        else:
            self.logger.debug(f"Request successful: {request.method} {url} | Attempts: {attempt + 1} | Time: {end_time:.3f}s")

        return ResponseFormat(
            status=status if error is None else None,
            data=content,
            url=url,
            error=error,
            execute_time=end_time,
            used_attempts=attempt,
        )




async def async_test_http(logger: logging.Logger):
    """Образец для использования"""
    payload  = {"identifier": "obeginin", "password": "111111"}
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    async with AsyncHttpClient(url="https://info-run.ru") as client:
        request = RequestFormat(method="POST", endpoint='/api/auth/login', json=payload, headers=headers, return_type="json")
        response = await client.request_async(request)
        logger.info(f"request: {request}")
        logger.info(f"response: {response}")
        #logger.info(f"response: {response.data}")
    return response


async def async_tests_http(logger: logging.Logger):
    """Пример использования для разных типов"""
    async with AsyncHttpClient(url="https://jsonplaceholder.typicode.com") as client:
        # JSON
        request = RequestFormat(method="GET", endpoint='/posts/1',return_type="json")
        resp_json = await client.request_async(request)
        # Text
        request = RequestFormat(method="GET", endpoint='/posts/1', return_type="text")
        resp_text = await client.request_async(request)
        # Bytes
        request = RequestFormat(method="GET", endpoint='/posts/1', return_type="bytes")
        resp_bytes = await client.request_async(request)
        logger.info(resp_json)
        logger.info(resp_text.data[:100])
        logger.info(f"Binary length: {len((resp_bytes.data))} data: {resp_bytes.data}")


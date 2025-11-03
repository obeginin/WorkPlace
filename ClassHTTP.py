import logging

import aiohttp
import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, Literal
from json import JSONDecodeError
from aiohttp import ContentTypeError

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
    ok: bool = field(init=False)

    def __post_init__(self):
        """Автоматически определяем успешность запроса"""
        self.ok = self.status is not None and 200 <= self.status < 300

    def __repr__(self):
        """для логов"""
        return f"<ResponseFormat status={self.status} ok={self.ok} url={self.url} data={self.data}>"

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
    
    
# Основной класс клиента
# ---------------------------
class AsyncHttpClient:
    """Асинхронный HTTP клиент с пулом соединений и гибким форматом ответа"""
    def __init__(
        self,
        url: str = "",
        timeout: float = 10.0,
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
        error: str | None = None  # инициализация перед циклом

        for attempt in range(self.max_retries + 1):
            try:
                async with self.session.request(
                        method=request.method,
                        url=url,
                        params=request.params,
                        data=request.data,
                        json=request.json,
                        headers=merged_headers
                ) as response:
                    status = response.status

                    if request.return_type == "json":
                        try:
                            content = await response.json(content_type=None)
                        except (ContentTypeError, JSONDecodeError):
                            content = await response.text()
                    elif request.return_type == "text":
                        content = await response.text()
                    elif request.return_type == "bytes":
                        content = await response.read()
                    else:
                        error = f"Unsupported return_type: {request.return_type}"
                        return ResponseFormat(status=None, data=None, url=url, error=error)

                    return ResponseFormat(status=status, data=content, url=str(response.url))

            except asyncio.TimeoutError:
                error = f"TimeoutError (attempt {attempt + 1})"
            except aiohttp.ClientError as e:
                error = f"ClientError: {e}"
            except Exception as e:
                error = f"UnexpectedError: {e}"

            if attempt < self.max_retries:
                await asyncio.sleep(2 ** attempt)

        return ResponseFormat(status=None, data=None, url=url, error=error or "Unknown error")



async def async_test_http(logger: logging.Logger):
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

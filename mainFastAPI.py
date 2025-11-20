from fastapi import FastAPI, Request
from ClassError import ErrorHandler
from ClassLogger import LoggerConfig
import uvicorn
#from app.core.exceptions import PermissionDeniedException
# инициализируем основное логирование
logger_config = LoggerConfig(log_file='mainFastAPI.log', log_level="INFO", console_output=True, use_json=False)
logger_config.setup_logger()
logger = logger_config.get_logger(__name__)
logger.info("Основной файл mainFastAPI")

app = FastAPI()
error_handler = ErrorHandler(logger) # подключаем обработчик ошибок

@app.exception_handler(Exception)
async def all_exceptions_handler(request: Request, e: Exception):
    """Глобальный обработчик для FastAPI"""
    return await error_handler.handle_http_exception(e, request)
@app.get("/ok")
async def ok():
    return {"message": "OK"}
# @app.get("/forbidden")
# async def forbidden():
#     raise PermissionDeniedException("You are not allowed here")
@app.get("/crash")
async def crash():
    1 / 0  # неожиданная ошибка


uvicorn.run("Service.main:app", host="0.0.0.0", port=8000, reload=True)
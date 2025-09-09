import traceback
from typing import Callable
from fastapi import Request, Response, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import logging

logger = logging.getLogger(__name__)


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """Middleware для централизованной обработки ошибок"""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable
    ) -> Response:
        try:
            response = await call_next(request)
            return response
        except HTTPException as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "error": exc.detail,
                    "status_code": exc.status_code,
                    "path": str(request.url),
                },
            )
        except Exception as exc:
            logger.error(f"Unhandled error: {exc}")
            logger.error(traceback.format_exc())

            return JSONResponse(
                status_code=500,
                content={
                    "error": "Внутренняя ошибка сервера",
                    "detail": "Произошла неожиданная ошибка",
                    "path": str(request.url),
                },
            )

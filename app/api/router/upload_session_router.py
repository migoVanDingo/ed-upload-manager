# app/api/router/upload_session_router.py
from xml.sax import handler
from fastapi import APIRouter, Depends, Request
from platform_common.logging.logging import get_logger
from platform_common.utils.service_response import ServiceResponse


from app.api.handler.upload_sessions.create_upload_session_handler import (
    CreateUploadSessionHandler,
)
from app.api.handler.upload_sessions.get_upload_session_handler import (
    GetUploadSessionHandler,
)
from app.api.handler.upload_sessions.list_upload_sessions_handler import (
    ListUploadSessionsHandler,
)
from app.api.handler.upload_sessions.update_upload_session_handler import (
    UpdateUploadSessionHandler,
)


router = APIRouter()
logger = get_logger("upload-session")


@router.get("/list")
async def list_upload_sessions(
    request: Request,
    handler: ListUploadSessionsHandler = Depends(ListUploadSessionsHandler),
) -> ServiceResponse:
    return await handler.do_process(request)


@router.get("/")
async def get_upload_session(
    request: Request,
    handler: GetUploadSessionHandler = Depends(GetUploadSessionHandler),
) -> ServiceResponse:
    return await handler.do_process(request)


@router.post("/")
async def create_upload_session(
    request: Request,
    handler: CreateUploadSessionHandler = Depends(CreateUploadSessionHandler),
) -> ServiceResponse:
    return await handler.do_process(request)


@router.put("/{upload_id}")
async def update_upload_session(
    upload_id: str,
    request: Request,
    handler: UpdateUploadSessionHandler = Depends(UpdateUploadSessionHandler),
) -> ServiceResponse:
    return await handler.do_process(request, upload_id)

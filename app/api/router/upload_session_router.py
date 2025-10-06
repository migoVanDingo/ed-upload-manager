# app/api/router/upload_session_router.py
from xml.sax import handler
from fastapi import APIRouter, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from platform_common.logging.logging import get_logger
from platform_common.utils.service_response import ServiceResponse


from api.handler.create_upload_session_handler import (
    CreateUploadSessionHandler,
)
from api.handler.get_upload_session_handler import (
    GetUploadSessionHandler,
)
from api.handler.get_list_upload_session_handler import (
    ListUploadSessionsHandler,
)
from api.handler.update_upload_session_handler import (
    UpdateUploadSessionHandler,
)


router = APIRouter()

ALLOWED_ORIGINS = [
    "http://localhost:5173",
    "https://your-frontend.example.com",
]

router.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],  # or tighten to specific headers you expect
    expose_headers=[  # only affects responses from YOUR API, not GCS
        "Location",
        "Content-Range",
        "x-goog-resumable",
    ],
)
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

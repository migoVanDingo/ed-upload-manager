# app/api/handler/upload_sessions/update_upload_session_handler.py
from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel
from fastapi import Depends, Request, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from platform_common.db.session import get_async_session
from platform_common.db.dal.upload_session_dal import UploadSessionDAL
from platform_common.utils.service_response import ServiceResponse


class UpdateUploadSessionBody(BaseModel):
    status: Optional[str] = None
    error: Optional[str] = None
    tags: Optional[List[str]] = None
    object_key: Optional[str] = None


class UpdateUploadSessionHandler:
    def __init__(self, db: AsyncSession = Depends(get_async_session)):
        self.db = db
        self.dal = UploadSessionDAL(db)

    async def do_process(self, request: Request, upload_id: str) -> ServiceResponse:
        try:
            body = UpdateUploadSessionBody(**(await request.json()))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid body: {e}")

        obj = await self.dal.update_session(
            upload_id,
            status=body.status,
            error=body.error,
            tags=body.tags,
            object_key=body.object_key,
        )
        if not obj:
            raise HTTPException(status_code=404, detail="Upload session not found")

        return ServiceResponse.success(obj.dict())

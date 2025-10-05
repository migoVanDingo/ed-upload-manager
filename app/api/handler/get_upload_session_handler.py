# app/api/handler/upload_sessions/get_upload_session_handler.py
from __future__ import annotations
from typing import Optional
from fastapi import Depends, Request, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from platform_common.db.session import get_async_session
from platform_common.db.dal.upload_session_dal import UploadSessionDAL
from platform_common.utils.service_response import ServiceResponse


class GetUploadSessionHandler:
    def __init__(self, db: AsyncSession = Depends(get_async_session)):
        self.db = db
        self.dal = UploadSessionDAL(db)

    async def do_process(self, request: Request) -> ServiceResponse:
        params = dict(request.query_params)
        upload_id: Optional[str] = params.get("upload_id")
        object_key: Optional[str] = params.get("object_key")

        if not upload_id and not object_key:
            raise HTTPException(
                status_code=400, detail="upload_id or object_key required"
            )

        obj = None
        if upload_id:
            obj = await self.dal.get_by_id(upload_id)
        elif object_key:
            obj = await self.dal.get_by_object_key(object_key)

        if not obj:
            raise HTTPException(status_code=404, detail="Upload session not found")

        return ServiceResponse.success(obj.dict())

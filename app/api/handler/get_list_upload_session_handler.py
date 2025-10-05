# app/api/handler/upload_sessions/list_upload_sessions_handler.py
from __future__ import annotations
from typing import List, Optional
from fastapi import Depends, Request, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from platform_common.db.session import get_async_session
from platform_common.db.dal.upload_session_dal import UploadSessionDAL
from platform_common.utils.service_response import ServiceResponse


class ListUploadSessionsHandler:
    def __init__(self, db: AsyncSession = Depends(get_async_session)):
        self.db = db
        self.dal = UploadSessionDAL(db)

    async def do_process(self, request: Request) -> ServiceResponse:
        qp = request.query_params
        datastore_id = qp.get("datastore_id")
        if not datastore_id:
            raise HTTPException(status_code=400, detail="datastore_id required")

        statuses_q = qp.get("statuses")  # comma-separated
        statuses: Optional[List[str]] = (
            [s.strip() for s in statuses_q.split(",") if s.strip()]
            if statuses_q
            else None
        )
        limit = int(qp.get("limit", 50))
        offset = int(qp.get("offset", 0))

        rows = await self.dal.list_by_datastore(
            datastore_id, statuses=statuses, limit=limit, offset=offset
        )
        data = [r.dict() for r in rows]
        return ServiceResponse.success(
            {"items": data, "count": len(data), "limit": limit, "offset": offset}
        )

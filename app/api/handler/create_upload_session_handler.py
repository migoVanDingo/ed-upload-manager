# ------------------------------------------------------------
# app/api/handler/upload_sessions/create_upload_session_handler.py
# from __future__ import annotations
import os
import json
from typing import List, Optional
from pydantic import BaseModel, Field as PydField
from fastapi import Depends, Request, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from google.cloud import storage

from platform_common.utils.service_response import ServiceResponse
from platform_common.utils.generate_id import generate_id
from platform_common.db.session import get_session
from platform_common.db.dal.upload_session_dal import UploadSessionDAL
from platform_common.models.upload_session import UploadSession

RAW_BUCKET_ENV = "RAW_BUCKET"  # e.g. ed-platform-raw


class CreateUploadSessionBody(BaseModel):
    datastore_id: str
    filename: str
    content_type: str
    size: Optional[int] = None
    tags: Optional[List[str]] = PydField(default_factory=list)


class CreateUploadSessionHandler:
    def __init__(self, db: AsyncSession = Depends(get_session)):
        self.db = db
        self.dal = UploadSessionDAL(db)
        self.storage_client = storage.Client()
        self.raw_bucket = os.getenv(RAW_BUCKET_ENV)
        if not self.raw_bucket:
            raise RuntimeError(f"{RAW_BUCKET_ENV} env var is required")

    async def do_process(self, request: Request) -> ServiceResponse:
        try:
            body = CreateUploadSessionBody(**(await request.json()))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid body: {e}")

        upload_id = generate_id("UPLD")
        object_key = (
            f"raw/datastore={body.datastore_id}/uploads/{upload_id}/{body.filename}"
        )

        # Persist session row first (initiated)
        session_obj = UploadSession(
            id=upload_id,
            datastore_id=body.datastore_id,
            filename=body.filename,
            content_type=body.content_type,
            size_estimate=body.size,
            tags=body.tags or [],
            object_key=object_key,
            status="initiated",
        )
        await self.dal.save(session_obj)

        # Create a GCS resumable upload session
        bucket = self.storage_client.bucket(self.raw_bucket)
        blob = bucket.blob(object_key)
        origin = request.headers.get("origin")  # helps with CORS
        upload_url = blob.create_resumable_upload_session(
            content_type=body.content_type,
            origin=origin,
            metadata={
                "uploadId": upload_id,
                "datastoreId": body.datastore_id,
                "tags": json.dumps(body.tags or []),
            },
        )

        data = {
            "uploadId": upload_id,
            "objectKey": object_key,
            "uploadUrl": upload_url,
        }
        return ServiceResponse(
            message="Upload session created", status_code=201, data=data
        )  # adjust if your ServiceResponse API differs

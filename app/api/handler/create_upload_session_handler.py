# ------------------------------------------------------------
# app/api/handler/upload_sessions/create_upload_session_handler.py
# from __future__ import annotations
import os
import json
from typing import List, Optional, Any
from pydantic import BaseModel, Field as PydField
from fastapi import Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from google.cloud import storage
from platform_common.logging.logging import get_logger
from platform_common.utils.service_response import ServiceResponse
from platform_common.utils.generate_id import generate_id
from platform_common.db.session import get_session
from platform_common.db.dal.upload_session_dal import UploadSessionDAL
from platform_common.models.upload_session import UploadSession
from platform_common.utils.time_helpers import get_current_epoch

RAW_BUCKET_ENV = (
    "RAW_BUCKET"  # can be "ed-lakehouse-test" OR "gs://ed-lakehouse-test/raw"
)
logger = get_logger("create_upload_session_handler")


class CreateUploadSessionBody(BaseModel):
    datastore_id: str
    files: List[dict]
    tags: Optional[List[str]] = PydField(default_factory=list)


def _normalize_bucket_and_prefix(raw: str) -> tuple[str, str]:
    """
    Accepts:
      - "ed-lakehouse-test"
      - "gs://ed-lakehouse-test"
      - "gs://ed-lakehouse-test/raw"
      - "gs://ed-lakehouse-test/raw/extra"
    Returns: (bucket_name, prefix_without_leading_trailing_slashes)
    """
    if raw.startswith("gs://"):
        raw = raw[5:]
    parts = raw.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].strip("/") if len(parts) > 1 else ""
    return bucket, prefix


def _normalize_tags(raw: Any) -> list[str]:
    # If your route already declares tags: list[str] = Form([]), this will just pass-through.
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
            return [str(parsed)]
        except json.JSONDecodeError:
            return [raw]
    if raw is None:
        return []
    return [str(raw)]


class CreateUploadSessionHandler:
    def __init__(self, db: AsyncSession = Depends(get_session)):
        self.db = db
        self.dal = UploadSessionDAL(db)
        self.storage_client = storage.Client()

        raw_bucket_env = os.getenv(RAW_BUCKET_ENV)
        if not raw_bucket_env:
            raise RuntimeError(f"{RAW_BUCKET_ENV} env var is required")

        self.bucket_name, self.base_prefix = _normalize_bucket_and_prefix(
            raw_bucket_env
        )
        logger.info(
            "GCS config: bucket=%s base_prefix=%s",
            self.bucket_name,
            self.base_prefix or "(none)",
        )

    def infer_content_type(self, f: UploadFile) -> str:
        return (
            getattr(f, "content_type", None)
            or getattr(f, "mimetype", None)
            or (
                getattr(f, "headers", {}).get("Content-Type")
                if hasattr(f, "headers")
                else None
            )
            or "application/octet-stream"
        )

    async def do_process(self, datastore_id, tags, files) -> ServiceResponse:
        logger.info("[%s] Processing create upload session request", __class__.__name__)

        # Optional: confirm datastore exists if you have FK constraints
        # (uncomment if needed)
        # from sqlmodel import select
        # from platform_common.models.datastore import Datastore
        # res = await self.db.execute(
        #     select(Datastore).where(Datastore.id == datastore_id, Datastore.is_active == True)
        # )
        # if not res.scalar_one_or_none():
        #     raise HTTPException(404, f"Datastore {datastore_id} not found")

        tags = _normalize_tags(tags)

        uploaded = []
        bucket = self.storage_client.bucket(self.bucket_name)

        for f in files:
            logger.info("Processing file: %s (%s)", f.filename, f.content_type)

            upload_id = generate_id("UPLD")
            ts = get_current_epoch()

            safe_name = (f.filename or "unnamed").replace(" ", "_")
            # object key: <prefix>/org=<datastore_id>/<id>_<ts>_<filename>
            key_parts = [
                self.base_prefix,  # can be ""
                f"org={datastore_id}",
                f"{upload_id}_{ts}_{safe_name}",
            ]
            object_key = "/".join(p for p in key_parts if p)

            # size (UploadFile has no .size)
            f.file.seek(0, os.SEEK_END)
            size_estimate = f.file.tell()
            f.file.seek(0)

            ctype = self.infer_content_type(f)

            # 1) insert upload_session row
            session_row = UploadSession(
                id=upload_id,
                datastore_id=datastore_id,
                filename=f"{ts}_{f.filename}",
                content_type=ctype,
                size_estimate=size_estimate,
                tags=tags,
                object_key=object_key,
                status="initiated",
            )
            await self.dal.save(session_row)
            logger.info(
                "[%s] Created upload session: %s", __class__.__name__, upload_id
            )

            # 2) upload to GCS (ensure stream starts from 0)
            blob = bucket.blob(object_key)
            blob.upload_from_file(f.file, content_type=ctype, rewind=True)
            logger.info("[%s] File uploaded to GCS: %s", __class__.__name__, object_key)

            # 3) optionally mark status advanced
            await self.dal.mark_uploaded(upload_id)

            uploaded.append({"upload_id": upload_id, "object_key": object_key})

        logger.info("[%s] Uploaded files %s", __class__.__name__, uploaded)
        return ServiceResponse(
            message="Upload session created", status_code=201, data=uploaded
        )

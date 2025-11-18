# ------------------------------------------------------------
# app/api/handler/upload_sessions/create_upload_session_handler.py
import os
import json
import datetime as dt
from typing import List, Optional, Any
from urllib.parse import urlparse, parse_qs

from pydantic import BaseModel, Field as PydField
from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession
from google.auth import default as google_auth_default

from platform_common.logging.logging import get_logger
from platform_common.errors.base import PlatformError
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


# ---------- Request/DTOs ----------
class FileSpec(BaseModel):
    client_token: Optional[str] = None  # optional token from client to correlate
    filename: str
    content_type: Optional[str] = None
    size_bytes: Optional[int] = None
    crc32c: Optional[str] = None  # base64-encoded CRC32C (optional, nice-to-have)


class CreateUploadSessionBody(BaseModel):
    datastore_id: str
    files: List[FileSpec]
    tags: Optional[List[str]] = PydField(default_factory=list)


# ---------- Helpers ----------
def _normalize_bucket_and_prefix(raw: str) -> tuple[str, str]:
    if raw.startswith("gs://"):
        raw = raw[5:]
    parts = raw.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].strip("/") if len(parts) > 1 else ""
    return bucket, prefix


def _normalize_tags(raw: Any) -> list[str]:
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


# ---------- Handler ----------
class CreateUploadSessionHandler:
    def __init__(self, db: AsyncSession = Depends(get_session)):
        self.db = db
        self.dal = UploadSessionDAL(db)

        raw_bucket_env = os.getenv(RAW_BUCKET_ENV)
        if not raw_bucket_env:
            raise RuntimeError(f"{RAW_BUCKET_ENV} env var is required")

        self.bucket_name, self.base_prefix = _normalize_bucket_and_prefix(
            raw_bucket_env
        )
        if not self.base_prefix:
            self.base_prefix = "raw"
        logger.info(
            "GCS config: bucket=%s base_prefix=%s",
            self.bucket_name,
            self.base_prefix or "(none)",
        )

        # Storage client (for bucket existence checks, optional)
        self.storage_client = storage.Client()

        # Authorized session for calling the JSON API to INITIATE resumable uploads
        creds, _ = google_auth_default(
            scopes=["https://www.googleapis.com/auth/devstorage.read_write"]
        )
        self.authed = AuthorizedSession(creds)

    def _infer_content_type(self, spec: FileSpec) -> str:
        return spec.content_type or "application/octet-stream"

    def _object_key_for(self, datastore_id: str, upload_id: str, filename: str) -> str:
        safe_name = (filename or "unnamed").replace(" ", "_")
        parts = [
            self.base_prefix,  # may be ""
            f"datastore",
            datastore_id,
            f"session",
            upload_id,
            f"{safe_name}",
        ]
        return "/".join(p for p in parts if p)

    def _initiate_resumable(
        self, bucket: str, object_key: str, ctype: str, size_bytes: Optional[int]
    ) -> str:
        """
        Initiate a GCS resumable upload and return the session URL (Location header).
        Client will upload directly to this URL with chunked PUTs and Content-Range.
        """
        endpoint = f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o"
        params = {"uploadType": "resumable", "name": object_key}

        headers = {
            "X-Upload-Content-Type": ctype,
        }
        if size_bytes is not None:
            headers["X-Upload-Content-Length"] = str(size_bytes)

        # You can add "X-Goog-Hash": "crc32c=<base64crc>" if you want strict integrity.
        resp = self.authed.post(endpoint, params=params, headers=headers)
        if resp.status_code not in (200, 201):
            logger.error(
                "Failed to initiate resumable upload: %s %s",
                resp.status_code,
                resp.text,
            )
            raise HTTPException(502, "Could not initiate upload with GCS")

        upload_url = resp.headers.get("Location")
        if not upload_url:
            raise HTTPException(502, "GCS did not return a resumable session URL")
        return upload_url

    async def do_process(self, datastore_id, tags, files) -> ServiceResponse:
        try:
            logger.info(
                "[%s] Processing create upload session (resumable URLs)",
                __class__.__name__,
            )
            # Optional: verify bucket exists / perms
            _ = self.storage_client.bucket(self.bucket_name)

            tags = _normalize_tags(tags)

            out = []

            for spec in files:
                logger.info(f"Processing file spec: {spec}")
                upload_id = generate_id("UPLD")
                object_key = self._object_key_for(
                    datastore_id, upload_id, spec.filename
                )
                ctype = self._infer_content_type(spec)

                # 1) Create UploadSession row (no bytes uploaded yet)
                session_row = UploadSession(
                    id=upload_id,
                    datastore_id=datastore_id,
                    filename=spec.filename,
                    content_type=ctype,
                    size_estimate=spec.size_bytes,
                    tags=tags,
                    object_key=object_key,
                    status="authorized",  # or "initiated"
                )
                await self.dal.save(session_row)
                logger.info(
                    "[%s] Created upload session: %s", __class__.__name__, upload_id
                )

                # 2) Initiate resumable upload (server-authenticated) → session URL for the browser
                upload_url = self._initiate_resumable(
                    bucket=self.bucket_name,
                    object_key=object_key,
                    ctype=ctype,
                    size_bytes=spec.size_bytes,
                )
                qs = parse_qs(urlparse(upload_url).query)
                gcs_session_id = (qs.get("upload_id") or [None])[0]
                logger.info("GCS resumable Location: %s", upload_url)
                logger.info("GCS resumable session id: %s", gcs_session_id)

                out.append(
                    {
                        "upload_id": upload_id,
                        "client_token": spec.client_token,
                        "object_key": object_key,
                        "upload_url": upload_url,  # use this verbatim on the client
                        "gcs_session_id": gcs_session_id,  # optional, for curl/debug
                        "content_type": ctype,
                        "suggested_chunk_bytes": 8 * 1024 * 1024,
                    }
                )

            return ServiceResponse(
                message="Upload sessions created", status_code=201, data=out
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Error in CreateUploadSessionHandler: %s", str(e))
            raise PlatformError(
                status_code=500, message=f"Internal server error: {str(e)}"
            ) from e

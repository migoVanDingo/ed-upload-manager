# ed_upload_manager/services/gcs_finalize_service.py
# from __future__ import annotations
import base64
import json
from typing import Any, Awaitable, Callable, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from platform_common.db.dal.file_dal import FileDAL
from platform_common.db.dal.upload_session_dal import UploadSessionDAL
from platform_common.utils.time_helpers import get_current_epoch


# Type of your job enqueue function (sync or async supported)
EnqueueFn = Callable[[str, Dict[str, Any]], Any]


def _maybe_await(result: Any):
    if hasattr(result, "__await__"):
        # coroutine/awaitable
        return result
    return None  # sync path; nothing to await


def _decode_pubsub_body(push_body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pub/Sub push body looks like:
    {
      "message": {
        "data": "base64-encoded-json",
        "attributes": { ... },
        "messageId": "...",
        "publishTime": "..."
      },
      "subscription": "..."
    }
    The decoded data for GCS notifications is the *object metadata JSON*.
    """
    try:
        msg = push_body.get("message") or {}
        data_b64 = msg.get("data")
        if not data_b64:
            raise ValueError("Missing message.data")
        raw = base64.b64decode(data_b64).decode("utf-8")
        payload = json.loads(raw)
        return payload
    except Exception as e:
        raise ValueError(f"Invalid Pub/Sub body: {e}")


def _detect_job_topic(content_type: str) -> Optional[str]:
    # Map content types to your processing topics
    if not content_type:
        return None
    ct = content_type.lower()

    if ct.startswith("application/pdf"):
        return "process-pdf"
    if ct.startswith("video/"):
        return "process-video"
    if ct.startswith("image/"):
        return "process-image"
    if ct in ("text/csv", "application/csv"):
        return "process-csv"
    # Add more as you add processors (audio/*, parquet, etc.)

    # If we have no processor, we can mark as ready immediately
    return None


async def handle_gcs_finalize_push(
    push_body: Dict[str, Any],
    db: AsyncSession,
    enqueue_job: EnqueueFn,
) -> None:
    """
    Idempotent handler for GCS OBJECT_FINALIZE Pub/Sub push.

    - Upserts File by object_key
    - Links to UploadSession (if uploadId present)
    - Sets statuses and enqueues processing job by content type
    """
    payload = _decode_pubsub_body(push_body)

    # GCS → Pub/Sub payload commonly includes:
    # bucket, name (object key), contentType, size, md5Hash, crc32c, metadata{...}
    bucket = payload.get("bucket")
    object_key = payload.get("name")
    content_type = payload.get("contentType") or "application/octet-stream"
    size_str = payload.get("size") or "0"
    md5_hash = payload.get("md5Hash")
    crc32c = payload.get("crc32c")
    meta = payload.get("metadata") or {}

    # We rely on *your* custom metadata set during resumable session creation:
    # metadata.uploadId, metadata.datastoreId, metadata.tags (JSON-stringified)
    upload_id = meta.get("uploadId")
    datastore_id = meta.get("datastoreId")
    # filename is the last path segment; use original intent if you store it
    filename = object_key.rsplit("/", 1)[-1] if object_key else "unknown"

    if not (bucket and object_key and datastore_id):
        # We need at least these to proceed; you can choose to log+return instead
        raise ValueError("Missing required fields: bucket/object_key/datastoreId")

    size = 0
    try:
        size = int(size_str)
    except Exception:
        pass

    file_dal = FileDAL(db)
    session_dal = UploadSessionDAL(db)

    # 1) Upsert File
    file_row = await file_dal.create_or_update_from_finalize(
        datastore_id=datastore_id,
        bucket=bucket,
        storage_provider="gcs",
        object_key=object_key,
        filename=filename,
        content_type=content_type,
        size=size,
        checksum_md5=md5_hash,
        checksum_crc32c=crc32c,
        metadata={},  # you can seed with anything from payload here
        upload_id=upload_id,
        status="processing",  # assume we’ll process; may switch to ready below
    )

    # 2) If we have an UploadSession, mark it 'uploaded' and link (idempotent)
    if upload_id:
        # mark_uploaded doesn’t fail if already set
        await session_dal.mark_uploaded(upload_id)
        # ensure the file is linked (idempotent)
        if not file_row.upload_id:
            await file_dal.link_upload(file_row.id, upload_id)

    # 3) Route to a processor (if any); otherwise mark ready
    topic = _detect_job_topic(content_type)
    if topic:
        job_payload = {
            "uploadId": upload_id,
            "fileId": file_row.id,
            "datastoreId": datastore_id,
            "bucket": bucket,
            "objectKey": object_key,
            "contentType": content_type,
            "size": size,
            "receivedAt": get_current_epoch(),
        }
        maybe_awaitable = enqueue_job(topic, job_payload)
        awaitable = _maybe_await(maybe_awaitable)
        if awaitable:
            await awaitable

        # Move session to processing if we have one
        if upload_id:
            await session_dal.mark_processing(upload_id)
        # File status already set to processing in the upsert
    else:
        # No processor? mark file+session ready immediately
        await file_dal.mark_status(file_row.id, "ready")
        if upload_id:
            await session_dal.mark_ready(upload_id)

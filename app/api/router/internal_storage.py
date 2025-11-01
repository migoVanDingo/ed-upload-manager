# ed_upload_manager/api/internal_storage.py
from fastapi import APIRouter, Request, Depends, Response, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from platform_common.db.session import get_session
from services.gcs_finalize import handle_gcs_finalize_push
from infra.pubsub_publisher import enqueue_job

router = APIRouter(prefix="/internal/storage", tags=["internal-storage"])


@router.post("/object-finalized")
async def object_finalized(
    request: Request,
    db: AsyncSession = Depends(get_session),
):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    try:
        await handle_gcs_finalize_push(body, db, enqueue_job)
    except ValueError as ve:
        # Malformed or missing required fields
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # Log e, capture to Sentry, etc.
        raise HTTPException(status_code=500, detail="Finalize handling failed")

    return Response(status_code=204)

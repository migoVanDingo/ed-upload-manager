from fastapi import FastAPI

from app.api.controller.health_check import router as health_router
from strawberry.fastapi import GraphQLRouter
from app.api.router.upload_session_router import router as upload_session_router

app = FastAPI(title="Core Service")

# REST endpoints
app.include_router(health_router, prefix="/health", tags=["Health"])
app.include_router(
    upload_session_router, prefix="/upload-sessions", tags=["Upload Sessions"]
)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.controller.health_check import router as health_router
from strawberry.fastapi import GraphQLRouter
from app.api.router.upload_session_router import router as upload_session_router

ALLOWED_ORIGINS = [
    "http://localhost:5173",
]
app = FastAPI(title="Core Service")
app.add_middleware(
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

# REST endpoints
app.include_router(health_router, prefix="/health", tags=["Health"])
app.include_router(
    upload_session_router, prefix="/upload-sessions", tags=["Upload Sessions"]
)

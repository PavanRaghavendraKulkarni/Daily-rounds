import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import files, search, uploads
from app.config import get_settings

logging.basicConfig(level=logging.INFO)
settings = get_settings()

app = FastAPI(
    title="Large File Processing & Search",
    description=(
        "Upload large text files with resumable, chunked uploads; files are indexed "
        "asynchronously and become searchable via semantic (embedding-based) search."
    ),
    version="1.0.0",
)

# Added to test my local webpage 
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.cors_allow_origins.split(",")],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(uploads.router)
app.include_router(files.router)
app.include_router(search.router)


@app.get("/health")
async def health():
    return {"status": "ok"}

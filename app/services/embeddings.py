from functools import lru_cache

import numpy as np
from sentence_transformers import SentenceTransformer

from app.config import get_settings

settings = get_settings()


@lru_cache
def get_model() -> SentenceTransformer:
    # Loaded once per process (API process and worker process each load their own).
    # CPU-only, small model — keeps steady-state RSS well within the 4 GB budget.
    return SentenceTransformer(settings.embedding_model_name, device="cpu")


def embed_texts(texts: list[str]) -> np.ndarray:
    model = get_model()
    return model.encode(texts, batch_size=settings.embedding_batch_size, convert_to_numpy=True, show_progress_bar=False)


def embed_query(text: str) -> np.ndarray:
    return embed_texts([text])[0]

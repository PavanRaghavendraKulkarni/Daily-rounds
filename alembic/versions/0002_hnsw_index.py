"""switch chunk embedding index from ivfflat to hnsw

ivfflat's clusters are trained (k-means) on whatever rows exist at CREATE INDEX
time. 0001 created the index in the same migration as the table, i.e. on zero
rows, producing degenerate clusters — later inserts got sorted into poorly
calibrated buckets, causing genuine matches to be missed (probes=1 searching
the wrong cluster returns zero candidates rather than an inexact one). HNSW is
built incrementally as rows are inserted, so it doesn't have this failure mode,
and has better recall at a comparable query cost.

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-03

"""
from typing import Sequence, Union

from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_chunks_embedding_cosine")
    op.execute(
        "CREATE INDEX ix_chunks_embedding_cosine ON chunks "
        "USING hnsw (embedding vector_cosine_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_chunks_embedding_cosine")
    op.execute(
        "CREATE INDEX ix_chunks_embedding_cosine ON chunks "
        "USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)"
    )

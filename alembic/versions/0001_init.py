"""init schema

Revision ID: 0001
Revises:
Create Date: 2026-09-02

"""
from typing import Sequence, Union

import pgvector.sqlalchemy
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

EMBEDDING_DIM = 384


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # create_type=False: we create the enum type explicitly below (checkfirst=True,
    # so this is idempotent). Without create_type=False, create_table would try to
    # create the same type again internally and fail with "already exists".
    file_status = postgresql.ENUM(
        "uploading", "uploaded", "processing", "ready", "failed", name="file_status", create_type=False
    )
    file_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "files",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("filename", sa.String(), nullable=False),
        sa.Column("storage_path", sa.String(), nullable=False),
        sa.Column("total_size", sa.BigInteger(), nullable=False),
        sa.Column("bytes_received", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("status", file_status, nullable=False, server_default="uploading"),
        sa.Column("chunks_indexed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "chunks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "file_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("files.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("start_offset", sa.BigInteger(), nullable=False),
        sa.Column("end_offset", sa.BigInteger(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("embedding", pgvector.sqlalchemy.Vector(EMBEDDING_DIM), nullable=False),
    )
    op.create_index("ix_chunks_file_id", "chunks", ["file_id"])
    # IVFFlat index for approximate nearest-neighbor cosine search; requires ANALYZE
    # and some existing rows to be effective, fine for this scale.
    op.execute(
        "CREATE INDEX ix_chunks_embedding_cosine ON chunks "
        "USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)"
    )


def downgrade() -> None:
    op.drop_table("chunks")
    op.drop_table("files")
    postgresql.ENUM(name="file_status").drop(op.get_bind(), checkfirst=True)
    op.execute("DROP EXTENSION IF EXISTS vector")

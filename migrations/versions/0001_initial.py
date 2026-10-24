"""Tablas de resultados y auditoría de requeue.

Revision ID: 0001
Revises:
Create Date: 2026-10-22
"""

from __future__ import annotations

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE transaction_results (
            id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            reference      TEXT        NOT NULL,
            status         TEXT        NOT NULL,
            priority       TEXT        NOT NULL,
            retry_count    INTEGER     NOT NULL DEFAULT 0,
            requeue_count  INTEGER     NOT NULL DEFAULT 0,
            result         JSONB,
            error_reason   TEXT,
            correlation_id TEXT        NOT NULL,
            created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at    TIMESTAMPTZ,
            CONSTRAINT uq_transaction_results_reference UNIQUE (reference),
            CONSTRAINT ck_transaction_results_status
                CHECK (status IN ('Successful', 'Failed', 'Retried', 'Abandoned')),
            CONSTRAINT ck_transaction_results_counts
                CHECK (retry_count >= 0 AND requeue_count >= 0),
            CONSTRAINT ck_transaction_results_result
                CHECK (status <> 'Successful' OR result IS NOT NULL)
        )
        """
    )
    op.execute("CREATE INDEX ix_transaction_results_status ON transaction_results (status)")
    op.execute(
        "CREATE INDEX ix_transaction_results_correlation_id ON transaction_results (correlation_id)"
    )
    op.execute(
        """
        CREATE TABLE requeue_audit (
            requeue_id     UUID        PRIMARY KEY,
            reference      TEXT        NOT NULL,
            operator       TEXT        NOT NULL CHECK (btrim(operator) <> ''),
            reason         TEXT        NOT NULL CHECK (btrim(reason) <> ''),
            dlq_entry_id   TEXT        NOT NULL,
            dlq_reason     TEXT        NOT NULL,
            dlq_last_error TEXT,
            created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX ix_requeue_audit_reference ON requeue_audit (reference)")


def downgrade() -> None:
    op.execute("DROP TABLE requeue_audit")
    op.execute("DROP TABLE transaction_results")

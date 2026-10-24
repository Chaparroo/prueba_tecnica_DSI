"""Núcleo de dominio: entidades, excepciones y políticas. Sin dependencias externas."""

from rpa_worker.domain.exceptions import BusinessException, SystemException, TransactionError
from rpa_worker.domain.transaction import TERMINAL_STATUSES, Transaction, TransactionStatus

__all__ = [
    "TERMINAL_STATUSES",
    "BusinessException",
    "SystemException",
    "Transaction",
    "TransactionError",
    "TransactionStatus",
]

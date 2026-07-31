"""Utilitários de hash usados em deduplicação e encurtamento."""
from __future__ import annotations
import hashlib


def _fp4(s: str) -> str:
    """Hash SHA-256 truncado em 32 chars — fingerprint padrão."""
    return hashlib.sha256(s.encode()).hexdigest()[:32]

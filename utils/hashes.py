"""Utilitários de hash usados em deduplicação e encurtamento."""
from __future__ import annotations
import hashlib


def _fp4(s: str) -> str:
    """Hash SHA-256 truncado em 32 chars — fingerprint padrão."""
    return hashlib.sha256(s.encode()).hexdigest()[:32]


def _fp_benef(id_global: str, plat: str, benef: frozenset) -> str:
    """Fingerprint para conjunto de benefícios de uma oferta."""
    return _fp4(f"{plat}|ben|{id_global}|{'|'.join(sorted(benef))}")

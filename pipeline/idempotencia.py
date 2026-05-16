"""
Camada de Idempotência.

Responsabilidade única: garantir que uma mesma mensagem (identificada
por msg_id) não seja processada duas vezes pela pipeline.

NÃO faz:
  - deduplicação semântica de conteúdo (responsabilidade da deduplicação)
  - coalescing de rajadas (responsabilidade do coalescing)
  - controle de publicação (responsabilidade da publicação)
  - persistência (estado é puramente in-memory, reinicia a cada restart)
"""
from __future__ import annotations

import asyncio
from typing import Set


# ── Parâmetros ────────────────────────────────────────────────────
_LIMITE_MAX  = 5000
_LIMITE_TRIM = 4000

# ── Estado interno (encapsulado neste módulo) ─────────────────────
_ids_processados: Set[int] = set()
_lock = asyncio.Lock()


async def ja_processado(msg_id: int) -> bool:
    """Retorna True se o msg_id já passou pela pipeline."""
    async with _lock:
        return msg_id in _ids_processados


async def marcar_processado(msg_id: int) -> None:
    """
    Registra o msg_id como processado. Aplica trim quando o conjunto
    excede o limite, removendo entradas arbitrárias até retornar ao
    tamanho de trim. O trim é grosseiro por design: o conjunto serve
    apenas como proteção curta contra reprocessamento imediato.
    """
    async with _lock:
        _ids_processados.add(msg_id)
        if len(_ids_processados) > _LIMITE_MAX:
            for _ in range(len(_ids_processados) - _LIMITE_TRIM):
                _ids_processados.pop()


async def contar() -> int:
    """Quantos msg_ids estão registrados como processados."""
    async with _lock:
        return len(_ids_processados)

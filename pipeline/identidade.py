"""
Módulo 1 — Identidade & Idempotência.

Responsabilidade ÚNICA: estabelecer a identidade canônica do chat de
origem e garantir que uma mesma mensagem não seja reprocessada.

Princípios:
  - Identidade canônica interna = id numérico ESTÁVEL do chat
    (event.chat_id, id marcado -100…). Sempre presente, idêntico em
    NewMessage/MessageEdited, imutável. É a única chave de identidade.
  - Username é APENAS verniz de exibição (log). Nunca é chave.
  - Idempotência por chave única (chat_canônico, msg_id), atômica.

NÃO faz:
  - extração de texto/links/mídia (responsabilidade da ingestão)
  - decisão de evolução/score/janela (Módulo 2)
  - publicação/edição/substituição no destino (Módulo 3)
  - persistência (estado in-memory; reinicia a cada restart)
"""
from __future__ import annotations

import asyncio
from typing import Optional, Set

from logger import log_ing

# ── API pública ───────────────────────────────────────────────────
__all__ = [
    "chat_canonico",
    "username_para_log",
    "checar_e_marcar",
    "contar_processados",
]

# ── Identidade canônica ───────────────────────────────────────────
# Cache de USERNAME por canal (id marcado → @username), APENAS para log.
_CHAT_USERNAME: dict[int, str] = {}


def chat_canonico(event) -> str:
    """
    Identidade canônica interna do chat de origem: o id numérico estável.
    Sempre presente e idêntico em new/edit. Retorna "" só se não houver
    chat_id (não esperado em eventos de canal monitorado).
    """
    cid = getattr(event, "chat_id", None)
    return str(cid) if cid is not None else ""


async def username_para_log(event) -> str:
    """
    Resolve o @username do canal SÓ para exibição em log. Best-effort,
    cacheado. NUNCA participa da identidade canônica.
    Retorna "" quando o canal não tem username ou a resolução falha.
    """
    cid = getattr(event, "chat_id", None)
    if cid is None:
        return ""
    cached = _CHAT_USERNAME.get(cid)
    if cached is not None:
        return cached
    username = ""
    try:
        ent = await event.get_chat()
        username = (getattr(ent, "username", None) or "").lower()
    except Exception as e:
        log_ing.warning(f"⚠️ username log {cid}: {e}")
        username = ""
    if username:
        _CHAT_USERNAME[cid] = username   # cacheia só resolução boa
    return username


# ── Idempotência ──────────────────────────────────────────────────
_LIMITE_MAX  = 5000
_LIMITE_TRIM = 4000

_ids_processados: Set[str] = set()
_lock = asyncio.Lock()


async def checar_e_marcar(chave: str) -> bool:
    """
    Atômico: True se `chave` JÁ foi processada; senão registra e retorna
    False. A chave deve ser única por mensagem de origem — use
    f"{chat}:{msg_id}" com chat na forma CANÔNICA (id numérico) —
    evitando colisão de msg_id entre canais e fechando a corrida entre
    updates duplicados (checagem e marcação sob o mesmo lock).

    Trim grosseiro ao exceder o limite: proteção curta contra
    reprocessamento imediato (in-memory, reinicia a cada restart).
    """
    async with _lock:
        if chave in _ids_processados:
            return True
        _ids_processados.add(chave)
        if len(_ids_processados) > _LIMITE_MAX:
            for _ in range(len(_ids_processados) - _LIMITE_TRIM):
                _ids_processados.pop()
        return False


async def contar_processados() -> int:
    async with _lock:
        return len(_ids_processados)

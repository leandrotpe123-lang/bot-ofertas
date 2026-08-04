"""
Camada — Exclusão mútua por chave.

Responsabilidade ÚNICA: entregar um asyncio.Lock dedicado por chave,
criado sob mutex e higienizado por TTL de inatividade.

Dois escopos, pools independentes:
  - identidade : chave = identity de oferta (str)
  - post       : chave = msg_id_dest (int)

NÃO conhece oferta, post, score, Telegram, banco nem regra de negócio.
A chave é opaca — o significado dela pertence a quem chama.

A ordem de aquisição (ORIGEM → IDENTIDADE → POST) é responsabilidade
do chamador. Este módulo não a impõe nem a conhece.

DÍVIDA HERDADA (F2 moveu sem alterar — NÃO corrigir aqui):
  a higiene do escopo identidade emite log; a do escopo post é
  silenciosa. Assimetria preservada byte a byte do código original.
"""
from __future__ import annotations

import asyncio
import time

import globals as g
from logger import log_sys

__all__ = ["lock_identidade", "lock_post"]


_LOCK_TTL = 600.0        # 10 min sem uso → descarta
_TETO     = 200          # acima disso, higiene oportunista

_IDENTITY_LOCKS: dict = {}        # identity -> asyncio.Lock
_IDENTITY_LOCKS_TS: dict = {}     # identity -> last_access_monotonic
_POST_LOCKS: dict = {}            # msg_id_dest -> asyncio.Lock
_POST_LOCKS_TS: dict = {}         # msg_id_dest -> last_access_monotonic


async def _obter(pool: dict, pool_ts: dict, chave, rotulo: str) -> asyncio.Lock:
    async with g._identity_locks_lck:
        lock = pool.get(chave)
        if lock is None:
            lock = asyncio.Lock()
            pool[chave] = lock
        pool_ts[chave] = time.monotonic()

        # Cleanup oportunista
        if len(pool) > _TETO:
            agora = time.monotonic()
            antigos = [
                k for k, ts in pool_ts.items()
                if agora - ts > _LOCK_TTL
            ]
            for k in antigos:
                lk = pool.get(k)
                if lk is not None and not lk.locked():
                    pool.pop(k, None)
                    pool_ts.pop(k, None)
            if antigos and rotulo:
                log_sys.debug(
                    f"🧹 {rotulo} cleanup: removidos {len(antigos)} | "
                    f"restam {len(pool)}"
                )
        return lock


async def lock_identidade(identity: str) -> asyncio.Lock:
    """Lock dedicado por identidade. Garante exclusão mútua entre
    tasks processando o mesmo cupom/produto/evento."""
    return await _obter(_IDENTITY_LOCKS, _IDENTITY_LOCKS_TS,
                        identity, "identity_locks")


async def lock_post(msg_id_dest: int) -> asyncio.Lock:
    """Camada 2: lock dedicado por post de destino. Protege a mutação
    concorrente do MESMO post quando duas mensagens de famílias por
    ofertas DISTINTAS convergem para ele (ex.: {A} e {B} → post {A,B})."""
    return await _obter(_POST_LOCKS, _POST_LOCKS_TS, msg_id_dest, "")

"""
Camada 4 — Claim atômico in-memory com TTL.

Responsabilidade ÚNICA: o MECANISMO de verificar-e-reservar um
fingerprint dentro de uma janela, sob lock, com limpeza oportunista
das entradas antigas.

É mecanismo PURO, sem política: a janela e o fingerprint chegam por
parâmetro. Não sabe o que é reativação, cupom, produto ou campanha —
quem escolhe a janela e o que fazer com a resposta é
pipeline.deduplicacao.

Único dono do acesso a g._atomic_mem e g._atomic_lck_obj nesta camada.

CONTRATO INTERNO DA CAMADA — LEIA ANTES DE ALTERAR:
  _atomic_check_and_claim tem prefixo `_`, mas NÃO é privado deste
  arquivo: é chamado por pipeline.deduplicacao. O underscore foi
  PRESERVADO na extração para não renomear nada fora de escopo —
  dívida registrada, não descuido. Não é reexportado.

Extraído de pipeline.deduplicacao sem qualquer alteração de
comportamento.
"""
from __future__ import annotations
import time
from typing import Optional, Tuple

import globals as g
from logger import log_ded


# ─────────────────────────────────────────────────────────────────
# Atomic locks (in-memory, evita race entre tasks)
# ─────────────────────────────────────────────────────────────────
async def _get_atomic_lck():
    return g._atomic_lck_obj


_ATOMIC_TTL_MAX = 4 * 60 * 60      # 4h
_ATOMIC_CLEANUP_THRESHOLD = 500


def _cleanup_atomic_mem_locked() -> int:
    """
    Remove entradas antigas de g._atomic_mem.
    DEVE ser chamado com g._atomic_lck_obj já adquirido.
    """
    if len(g._atomic_mem) <= _ATOMIC_CLEANUP_THRESHOLD:
        return 0
    agora = time.monotonic()
    antigos = [
        k for k, ts in g._atomic_mem.items()
        if agora - ts > _ATOMIC_TTL_MAX
    ]
    for k in antigos:
        g._atomic_mem.pop(k, None)
    return len(antigos)


async def _atomic_check_and_claim(fp: str, janela: float) -> Tuple[bool, Optional[float]]:
    """
    Atômico: verifica se fp existe DENTRO da janela e, se não, faz claim.
    Retorna (na_janela, ts_existente).
      - na_janela=True  → identidade já está sendo processada/foi recente
      - na_janela=False → claim feito agora, primeira vez nessa janela

    Tudo em UM lock pra evitar race entre check e claim. Ao reentrar
    dentro da janela, atualiza o timestamp pra estender.
    """
    async with (await _get_atomic_lck()):
        agora = time.monotonic()
        # Cleanup oportunista
        removidos = _cleanup_atomic_mem_locked()
        if removidos:
            log_ded.debug(
                f"🧹 _atomic_mem cleanup: removidos {removidos} | "
                f"restam {len(g._atomic_mem)}"
            )
        ts = g._atomic_mem.get(fp)
        if ts is not None and (agora - ts) < janela:
            # Atualiza pra estender
            g._atomic_mem[fp] = agora
            return True, ts
        # Claim
        g._atomic_mem[fp] = agora
        return False, ts

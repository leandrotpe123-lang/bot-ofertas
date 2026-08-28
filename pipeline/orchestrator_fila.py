"""Camada 2 — Orquestração / Fila e workers.

Responsabilidade ÚNICA: a máquina de concorrência — heap com
prioridade, TTL de fila, pool de workers e o start do loop. Detém os
parâmetros operacionais e é a única a MUTAR g._buf e g._w_ativos.

NÃO conhece a sequência das camadas: delega a _pipeline, em
pipeline.orchestrator_pipeline. Não importa pipeline.orchestrator.

CONTRATO INTERNO DA CAMADA — LEIA ANTES DE ALTERAR:
  _enfileirar tem prefixo `_`, mas é chamado por `processar` em
  pipeline.orchestrator. Underscore PRESERVADO na extração — dívida
  registrada. _iniciar_orchestrator é reexportado pela fachada e
  consumido por main.py.

Extraído de pipeline.orchestrator sem qualquer alteração de
comportamento.
"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import time

import globals as g
from logger import log_sys
from pipeline.orchestrator_pipeline import _pipeline
from pipeline.vida_oferta import VIDA_OFERTA_S


# ── Parâmetros operacionais ───────────────────────────────────────
_WORKERS_MAX = 4
_FILA_MAX    = 200
_TTL_FILA_S  = 60

# Prioridades da heap (menor = mais prioritário).
# Edições são correção do divulgador e processam antes de novas.
_PRIO_EDIT = 0
_PRIO_NOVA = 1


# Desempate estável da heap. `event` (telethon events.NewMessage.Event)
# não implementa __lt__: com prio E ts idênticos, o heapq avançaria para
# ele e levantaria TypeError, perdendo a mensagem silenciosamente. O
# contador é sempre distinto e crescente, então a comparação nunca passa
# dele. Ordem preservada: itens com prio+ts distintos seguem decididos
# por prio e ts, exatamente como antes; o contador só arbitra empates,
# e nesse caso aplica FIFO (o que chegou primeiro sai primeiro).
# next() é chamado sob g._buf_lck, e é atômico em CPython.
_seq = itertools.count()


# ── Fila de entrada ───────────────────────────────────────────────
async def _enfileirar(event, is_edit: bool) -> None:
    async with g._buf_lck:
        if len(g._buf) >= _FILA_MAX:
            log_sys.warning(f"⚠️ Fila cheia | id={event.message.id}")
            return
        prio = _PRIO_EDIT if is_edit else _PRIO_NOVA
        heapq.heappush(
            g._buf, (prio, time.monotonic(), next(_seq), event, is_edit))
    g._buf_evt.set()

# ── Workers ───────────────────────────────────────────────────────
async def _worker_loop() -> None:
    while True:
        await g._buf_evt.wait()
        while True:
            item = None
            async with g._buf_lck:
                if g._buf:
                    item = heapq.heappop(g._buf)
                else:
                    g._buf_evt.clear()
                    break
            if item is None:
                break

            prio, ts, _seq_i, event, is_edit = item

            async with g._w_lck:
                if g._w_ativos >= _WORKERS_MAX:
                    async with g._buf_lck:
                        heapq.heappush(g._buf, item)
                        g._buf_evt.set()
                    await asyncio.sleep(0.5)
                    break
                g._w_ativos += 1

            try:
                if time.monotonic() - ts > _TTL_FILA_S:
                    log_sys.warning(
                        f"⏱ Expirado | id={event.message.id}"
                    )
                    continue
                await _pipeline(event, is_edit)
                
            except Exception as e:
                log_sys.error(f"❌ Worker: {e}", exc_info=True)
            finally:
                async with g._w_lck:
                    g._w_ativos -= 1


async def _iniciar_orchestrator() -> None:
    from config import _MAX_EDITS  # noqa: log only
    log_sys.info(
        f"🎛 Orchestrator | workers={_WORKERS_MAX} fila={_FILA_MAX} "
        f"vida_oferta={VIDA_OFERTA_S}s "
        f"max_edits={_MAX_EDITS}"
    )
    asyncio.create_task(_worker_loop())

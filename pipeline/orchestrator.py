"""
Camada 2 — Orquestração.

Responsabilidade única: coordenar o fluxo entre as camadas da pipeline.
Recebe eventos do Telegram, enfileira com prioridade, gerencia workers
e chama cada camada na ordem correta.

Camadas chamadas em sequência:
    1. Ingestão       → pipeline.ingestao.ingerir
    2. Idempotência   → pipeline.idempotencia.ja_processado
    3. Coalescing     → pipeline.coalescing.deve_coalescer
    4. Shadow reply   → handlers.shadow_reply (condicional)
    5. Normalização   → pipeline.normalizacao.normalizar
    6. Deduplicação   → pipeline.deduplicacao.deve_enviar_async
    7. Saturação      → pipeline.publicacao.delay_saturacao
    8. Montagem       → pipeline.montagem.montar
    9. Publicação     → pipeline.publicacao.enviar (com is_edit)

NÃO faz:
  - leitura de texto do evento (ingestão é a fonte oficial)
  - decisão de coalescing (camada própria)
  - controle de idempotência (camada própria)
  - decisão de edição (publicação resolve)
  - acesso a banco
  - lógica de plataforma
"""
from __future__ import annotations

import asyncio
import heapq
import time

import globals as g
from logger import log_sys, _idade_str
from pipeline.deduplicacao import deve_enviar_async
from pipeline.identidade import checar_e_marcar
from pipeline.ingestao import ingerir
from pipeline.montagem import montar
from pipeline.normalizacao import normalizar
from pipeline.publicacao import delay_saturacao, enviar


# ── Parâmetros operacionais ───────────────────────────────────────
_WORKERS_MAX = 4
_FILA_MAX    = 200
_TTL_FILA_S  = 60

# Prioridades da heap (menor = mais prioritário).
# Edições são correção do divulgador e processam antes de novas.
_PRIO_EDIT = 0
_PRIO_NOVA = 1


# ── Fila de entrada ───────────────────────────────────────────────
async def _enfileirar(event, is_edit: bool) -> None:
    async with g._buf_lck:
        if len(g._buf) >= _FILA_MAX:
            log_sys.warning(f"⚠️ Fila cheia | id={event.message.id}")
            return
        prio = _PRIO_EDIT if is_edit else _PRIO_NOVA
        heapq.heappush(g._buf, (prio, time.monotonic(), event, is_edit))
                log_sys.info(
            f"🧭 TL | id={event.message.id} chat={event.chat_id} | FILA_IN | "
            f"tipo={'edit' if is_edit else 'new'} prio={prio} "
            f"profundidade={len(g._buf)}")
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

            prio, ts, event, is_edit = item

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
                log_sys.info(
                    f"🧭 TL | id={event.message.id} chat={event.chat_id} | "
                    f"FILA_OUT | tipo={'edit' if is_edit else 'new'}")
                log_sys.debug(
                    f"🧭 TL | id={event.message.id} chat={event.chat_id} | "
                    f"espera_fila={time.monotonic() - ts:.1f}s")
                await _pipeline(event, is_edit)
                
            except Exception as e:
                log_sys.error(f"❌ Worker: {e}", exc_info=True)
            finally:
                async with g._w_lck:
                    g._w_ativos -= 1


# ── Pipeline principal ────────────────────────────────────────────
async def _pipeline(event, is_edit: bool = False) -> None:
    """
    Fluxo da pipeline. Chama cada camada na ordem e propaga is_edit
    até a publicação. Não toma decisão de negócio.
    """
    msg_id = event.message.id

    # ── Camada 1: Ingestão (primeiro — fornece o chat canônico) ──
    try:
        bruta = await ingerir(event)
    except Exception as e:
        log_sys.error(f"❌ ingestao: {e}")
        return

    log_sys.info(
        f"🧭 TL | id={msg_id} chat={bruta.chat} | PROC | "
        f"origem={'edit' if is_edit else 'new'}")
    log_sys.debug(
        f"🧭 TL | id={msg_id} chat={bruta.chat} | "
        f"idade_proc={_idade_str(event.message.date)} "
        f"q={len(g._buf)} w={g._w_ativos}")

    # ── Idempotência (somente novas) — chave (chat canônico, msg_id) ──
    if not is_edit and await checar_e_marcar(f"{bruta.chat}:{msg_id}"):
            log_sys.info(
            f"🧭 TL | id={msg_id} chat={bruta.chat} | DESCARTE | "
            f"motivo=JA_PROCESSADO")
        return

    # ── Shadow reply (somente mensagens novas que são reply) ──────
    if bruta.is_reply and bruta.reply_to > 0 and not is_edit:
        from handlers.shadow_reply import processar_shadow_reply
        handled = await processar_shadow_reply(bruta)
        if handled:
            return

    log_sys.info(
        f"{'✏️' if is_edit else '📩'} @{bruta.chat} | "
        f"id={msg_id} | q={len(g._buf)} w={g._w_ativos}"
    )

    # ── Camada 2: Normalização ────────────────────────────────────
    try:
        norm = await normalizar(bruta)
    except Exception as e:
        log_sys.error(f"❌ normalizar: {e}")
        return
    if norm is None:
            log_sys.info(
            f"🧭 TL | id={msg_id} chat={bruta.chat} | DESCARTE | "
            f"motivo=NORMALIZACAO_VAZIA")
        return

    # ── Camada 3: Deduplicação + saturação (somente novas) ───────
    if not is_edit:
        try:
            if not await deve_enviar_async(norm):
                            log_sys.info(
                    f"🧭 TL | id={msg_id} chat={norm.chat} | DESCARTE | "
                    f"motivo=DEDUP")
                return
        except Exception as e:
            log_sys.error(f"❌ deve_enviar: {e}")
            return

        try:
            delay = await delay_saturacao(norm.plat, norm.texto_limpo)
            if delay > 0:
                log_sys.info(
                    f"🧭 TL | id={msg_id} chat={norm.chat} | SAT_DELAY | "
                    f"delay={delay:.1f}s")
                await asyncio.sleep(delay)
        except Exception as e:
            log_sys.error(f"❌ saturacao: {e}")

    # ── Camada 4: Montagem ────────────────────────────────────────
    try:
        montada = await montar(norm)
    except Exception as e:
        log_sys.error(f"❌ montar: {e}")
        return

    # ── Camada 5: Publicação ──────────────────────────────────────
    await enviar(montada, norm=norm, is_edit=is_edit)


# ── Entrypoint público ────────────────────────────────────────────
async def processar(event, is_edit: bool = False) -> None:
    """Chamado pelos handlers do Telethon em `main.py`."""
    await _enfileirar(event, is_edit)


async def _iniciar_orchestrator() -> None:
    from config import _JANELA_DISPUTA_S, _MAX_EDITS  # noqa: log only
    log_sys.info(
        f"🎛 Orchestrator | workers={_WORKERS_MAX} fila={_FILA_MAX} "
        f"janela_disputa={_JANELA_DISPUTA_S}s "
        f"max_edits={_MAX_EDITS}"
    )
    asyncio.create_task(_worker_loop())

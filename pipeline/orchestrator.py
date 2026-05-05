"""
Orchestrator — fila de prioridade, workers e pipeline principal.

Ordem do pipeline (preservada):
  1. Ingestão        (pipeline.ingestao)
  2. Shadow reply    (handlers.shadow_reply)  ← só se is_reply
  3. Pending check   (handlers.pending)       ← via shadow reply
  4. Normalização    (pipeline.normalizacao)
  5. Deduplicação    (pipeline.deduplicacao)
  6. Saturação       (pipeline.publicacao)
  7. Montagem        (pipeline.montagem)
  8. Publicação      (pipeline.publicacao)
"""
from __future__ import annotations

import asyncio
import hashlib
import heapq
import re
import time
from typing import Optional

from config import _EXECUTOR
import globals as g
from globals import _buf, _IDS_LOCK, _IDS_PROC
from logger import log_sys
from pipeline.deduplicacao import deve_enviar_async
from pipeline.ingestao import ingerir
from pipeline.montagem import montar, preparar_imagem_tg
from pipeline.normalizacao import normalizar
from pipeline.publicacao import (
    _foi_processado, delay_saturacao, editar, editar_por_id, enviar,
    _MAX_EDITS,
)
from pipeline.deduplicacao import calcular_score, identidade_canonica
from database import db_get_estado, db_set_estado
from utils.helpers import ler_mapa

# ── Constantes do orchestrator ────────────────────────────────────
_WORKERS_MAX = 4
_FILA_MAX    = 200
_COALESCE_MS = 800


def _prio(texto: str) -> int:
    tl = texto.lower()
    if "amazon" in tl: return 1
    if "shopee" in tl: return 2
    if "magalu" in tl: return 3
    return 9


def _fp_r(texto: str) -> str:
    return hashlib.sha256(
        re.sub(r'\s+', '', texto.lower())[:80].encode()
    ).hexdigest()[:12]


async def _enfileirar(event, is_edit: bool) -> None:
    texto = event.message.text or ""
    if not texto.strip(): return
    fp = _fp_r(texto); agora = time.monotonic()
    async with g._buf_lck:
        from globals import _coal
        if not is_edit and agora - _coal.get(fp, 0.0) < _COALESCE_MS / 1000:
            return
        _coal[fp] = agora
        if len(_buf) >= _FILA_MAX:
            log_sys.warning(f"⚠️ Fila cheia | id={event.message.id}")
            return
        heapq.heappush(_buf, (0 if is_edit else _prio(texto), agora, event, is_edit))
    g._buf_evt.set()


async def _worker_loop() -> None:
    import globals as g
    while True:
        await g._buf_evt.wait()
        while True:
            item = None
            async with g._buf_lck:
                if _buf:
                    item = heapq.heappop(_buf)
                else:
                    g._buf_evt.clear()
                    break
            if item is None: break
            prio, ts, event, is_edit = item
            async with g._w_lck:
                if g._w_ativos >= _WORKERS_MAX:
                    async with g._buf_lck:
                        heapq.heappush(_buf, item)
                        g._buf_evt.set()
                    await asyncio.sleep(0.5)
                    break
                g._w_ativos += 1
            try:
                if time.monotonic() - ts > 60:
                    log_sys.warning(f"⏱ Expirado | id={event.message.id}")
                    continue
                await _pipeline(event, is_edit)
            except Exception as e:
                log_sys.error(f"❌ Worker: {e}", exc_info=True)
            finally:
                async with g._w_lck:
                    g._w_ativos -= 1


async def _pipeline(event, is_edit: bool = False) -> None:
    msg_id = event.message.id

    # Anti-loop
    if not is_edit:
        if await _foi_processado(msg_id): return
    else:
        loop = asyncio.get_running_loop()
        mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        if str(msg_id) not in mp: return

    # ── Camada 1: Ingestão ────────────────────────────────────────
    try:
        bruta = ingerir(event)
    except Exception as e:
        log_sys.error(f"❌ ingestao: {e}"); return

    # ── Shadow reply / pending (só em mensagens novas) ────────────
    if bruta.is_reply and bruta.reply_to > 0 and not is_edit:
        from handlers.shadow_reply import processar_shadow_reply
        handled = await processar_shadow_reply(bruta)
        if handled:
            return   # shadow reply tratado — encerra aqui

    log_sys.info(
        f"{'✏️' if is_edit else '📩'} @{bruta.chat} | "
        f"id={msg_id} | q={len(_buf)} w={_get_w_ativos()}"
    )

    # ── Camadas 2+3: Classificação + Normalização ─────────────────
    try:
        norm = await normalizar(bruta)
    except Exception as e:
        log_sys.error(f"❌ normalizar: {e}"); return
    if norm is None: return

    # ── Camada 4: Deduplicação ────────────────────────────────────
    if not is_edit:
        try:
            if not await deve_enviar_async(norm): return
        except Exception as e:
            log_sys.error(f"❌ deve_enviar: {e}"); return

        try:
            delay = await delay_saturacao(norm.plat, norm.texto_limpo)
            if delay > 0: await asyncio.sleep(delay)
        except Exception as e:
            log_sys.error(f"❌ saturacao: {e}")

    # ── Camada 5: Montagem ────────────────────────────────────────
    try:
        montada = await montar(norm)
    except Exception as e:
        log_sys.error(f"❌ montar: {e}"); return

    # ── Camada 6: Publicação ──────────────────────────────────────
    if is_edit:
        # ┄ EDIÇÃO DA MENSAGEM ORIGINAL no grupo monitorado ┄
        # Esse é o caso onde o divulgador postou bagunçado, depois
        # editou e (geralmente) colocou o código entre crases.
        # → Re-extrai e atualiza o post no @ofertap.
        loop = asyncio.get_running_loop()
        mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        id_d = mp.get(str(msg_id))
        if not id_d:
            return  # mensagem não foi publicada — ignora edição
        id_d = int(id_d)

        identity = identidade_canonica(norm)
        estado   = db_get_estado(identity)

        # Tenta também buscar pelo msg_id de destino (caso identity tenha mudado)
        if not estado:
            try:
                from database import _db
                with _db() as db:
                    row = db.execute(
                        "SELECT identity, score, texto, plat, lider, "
                        "janela_fim, edit_count, shadow_reply_id "
                        "FROM oferta_estado WHERE msg_id_dest=?",
                        (id_d,),
                    ).fetchone()
                if row:
                    estado = dict(zip(
                        ["identity","score","texto","plat","lider",
                         "janela_fim","edit_count","shadow_reply_id"],
                        row,
                    ))
            except Exception as e:
                log_sys.warning(f"⚠️ buscar estado por msg_id: {e}")

        edit_count_atual = (estado or {}).get("edit_count", 0) or 0
        lider_atual      = (estado or {}).get("lider", "") or ""
        score_atual      = (estado or {}).get("score", 0) or 0
        janela_fim       = (estado or {}).get("janela_fim", 0) or 0

        # Edição do mesmo grupo que publicou: SEMPRE permite
        # (correção do divulgador, não conta como edição estética)
        eh_correcao_original = (not lider_atual) or (norm.chat == lider_atual)

        # Se for correção do original: edita SEM contar no _MAX_EDITS
        # Se for de outro grupo (estético): respeita _MAX_EDITS
        if not eh_correcao_original and edit_count_atual >= _MAX_EDITS:
            log_sys.info(
                f"🔒 [EDIT_BLOQ_MAX] msg={id_d} edits={edit_count_atual}"
            )
            return

        # Tenta capturar imagem nova se houver
        img_nova = None
        if norm.tem_midia:
            try:
                img_nova = await preparar_imagem_tg(norm.media_obj)
            except Exception as e:
                log_sys.warning(f"⚠️ preparar_imagem (edit): {e}")

        # Edita post no @ofertap
        try:
            ok = await editar_por_id(id_d, montada.texto, img_nova)
        except Exception as e:
            log_sys.error(f"❌ editar_por_id (edit): {e}")
            return

        if not ok:
            log_sys.warning(f"⚠️ editar_por_id retornou False | msg={id_d}")
            return

        # Atualiza estado no DB com NOVO texto/cupom/identity
        # IMPORTANTE: usa identity NOVA (cupom pode ter mudado!)
        novo_score = max(score_atual, 0)
        novo_edit_count = (
            edit_count_atual if eh_correcao_original
            else edit_count_atual + 1
        )
        try:
            db_set_estado(
                identity, id_d, novo_score, montada.texto,
                montada.plat, lider_atual or norm.chat,
                janela_fim, novo_edit_count,
                (estado or {}).get("shadow_reply_id", 0),
            )
            log_sys.info(
                f"✏️ [EDIT_OK] msg={id_d} identity={identity} "
                f"correcao_original={eh_correcao_original}"
            )
        except Exception as e:
            log_sys.warning(f"⚠️ db_set_estado (edit): {e}")
        return
    else:
        await enviar(montada, norm=norm)


def _get_w_ativos() -> int:
    import globals as g
    return g._w_ativos


async def processar(event, is_edit: bool = False) -> None:
    await _enfileirar(event, is_edit)


async def _iniciar_orchestrator() -> None:
    from config import _JANELA_DISPUTA_S  # noqa — apenas para log
    log_sys.info(
        f"🎛 Orchestrator | workers={_WORKERS_MAX} fila={_FILA_MAX} "
        f"coalesce={_COALESCE_MS}ms "
        f"janela_disputa={_JANELA_DISPUTA_S}s "
        f"max_edits={_MAX_EDITS}"
    )
    asyncio.create_task(_worker_loop())
          

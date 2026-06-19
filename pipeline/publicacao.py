"""Camada 6 — Publicação: envio, edição, controle de saturação e disputa."""
from __future__ import annotations
import asyncio
import time
from typing import Optional

from telethon.errors import FloodWaitError, MessageNotModifiedError

import config
from config import GRUPO_DESTINO, _EXECUTOR, _JANELA_DISPUTA_S, _MAX_EDITS
from database import db_get_estado, db_set_estado, db_registrar_sat
import globals as g
from logger import log_out, log_sys
from pipeline.deduplicacao import calcular_score, identidade_canonica
from pipeline.decisao import decidir
from pipeline.saida import (
    _enviar_msg,
    _editar_inner_no_sem,
    _substituir_post_com_midia,
)
from pipeline.montagem import MensagemMontada
from pipeline.normalizacao import MensagemNormalizada
from pipeline.estado_evento import _KW_EVENTO
from utils.helpers import ler_mapa, salvar_mapa


# ── Constantes de saturação ──────────────────────────────────────
_SAT_MAX_PLAT  = 10
_SAT_BURST_LIM = 6
_SAT_BURST_JAN = 60


# ─────────────────────────────────────────────────────────────────
# Lock pessimista por identidade
# Garante exclusão mútua entre tasks processando a mesma oferta.
# ─────────────────────────────────────────────────────────────────
_IDENTITY_LOCKS: dict = {}        # identity -> asyncio.Lock
_IDENTITY_LOCKS_TS: dict = {}     # identity -> last_access_monotonic
_IDENTITY_LOCK_TTL = 600.0        # 10 min sem uso → descarta


async def _get_identity_lock(identity: str) -> asyncio.Lock:
    """Lock dedicado por identidade. Garante exclusão mútua entre
    tasks processando o mesmo cupom/produto/evento."""
    async with g._identity_locks_lck:
        lock = _IDENTITY_LOCKS.get(identity)
        if lock is None:
            lock = asyncio.Lock()
            _IDENTITY_LOCKS[identity] = lock
        _IDENTITY_LOCKS_TS[identity] = time.monotonic()

        # Cleanup oportunista
        if len(_IDENTITY_LOCKS) > 200:
            agora = time.monotonic()
            antigos = [
                k for k, ts in _IDENTITY_LOCKS_TS.items()
                if agora - ts > _IDENTITY_LOCK_TTL
            ]
            for k in antigos:
                lk = _IDENTITY_LOCKS.get(k)
                if lk is not None and not lk.locked():
                    _IDENTITY_LOCKS.pop(k, None)
                    _IDENTITY_LOCKS_TS.pop(k, None)
            if antigos:
                log_sys.debug(
                    f"🧹 identity_locks cleanup: removidos {len(antigos)} | "
                    f"restam {len(_IDENTITY_LOCKS)}"
                )
        return lock


async def _marcar(msg_id: int):
    async with g._IDS_LOCK:
        g._IDS_PROC.add(msg_id)
        if len(g._IDS_PROC) > 5000:
            for _ in range(len(g._IDS_PROC) - 4000):
                g._IDS_PROC.pop()


async def _foi_processado(msg_id: int) -> bool:
    async with g._IDS_LOCK:
        return msg_id in g._IDS_PROC


async def _burst_add():
    async with g._BURST_LOCK:
        agora = time.monotonic(); g._burst.append(agora)
        while g._burst and agora - g._burst[0] > _SAT_BURST_JAN:
            g._burst.pop(0)


async def _burst_count() -> int:
    async with g._BURST_LOCK:
        agora = time.monotonic()
        return sum(1 for t in g._burst if agora - t <= _SAT_BURST_JAN)


async def delay_saturacao(plat: str, texto: str) -> float:
    from database import db_count_sat
    if _KW_EVENTO.search(texto): return 0.0
    delay = 0.0
    if db_count_sat(plat) >= _SAT_MAX_PLAT: delay += 6.0
    if await _burst_count() >= _SAT_BURST_LIM: delay += 4.0
    return delay


def _midia_grupo_ruim(chat: str) -> bool:
    """Verifica se o chat está na lista de grupos com imagem feia."""
    return (chat or "").lower() in config._GRUPOS_IMG_RUIM


async def enviar(montada: MensagemMontada,
                 norm: Optional[MensagemNormalizada] = None,
                 is_edit: bool = False) -> bool:
    """
    Publica ou edita mensagem no grupo destino.
    Aceita `is_edit` por coerência contratual com o orchestrator.
    Lock por identidade garante serialização entre tasks da mesma oferta.
    """
    identity: Optional[str] = None
    score: int = 0
    if norm is not None:
        identity = identidade_canonica(norm)
        score    = calcular_score(norm)

    if identity is not None:
        ident_lock = await _get_identity_lock(identity)
        async with ident_lock:
            return await _enviar_inner(montada, norm, identity, score)

    return await _enviar_inner(montada, norm, identity, score)


async def _enviar_inner(montada: MensagemMontada,
                        norm: Optional[MensagemNormalizada],
                        identity: Optional[str],
                        score: int) -> bool:
    """Corpo real de enviar() — chamado dentro do lock por identidade."""
    async with config._SEM_ENVIO:
        loop = asyncio.get_running_loop()

        if norm is not None:
            estado = db_get_estado(identity)

            if estado:
                agora = time.time()
                d = decidir(norm, montada, score, estado, agora)

                # ── Log da decisão (rótulos idênticos aos atuais) ──
                if d.motivo == "LIDER_TRAVADO":
                    log_out.info(
                        f"🔒 [LIDER_TRAVADO] {identity} "
                        f"lider={estado.get('lider','')} candidato={norm.chat} "
                        f"score {score}<={d.score_atual}")
                elif d.motivo == "MAX_EDITS":
                    log_out.info(
                        f"🔒 [MAX_EDITS] {identity} "
                        f"edits={estado.get('edit_count', 0) or 0}")
                elif d.motivo == "EVOLUI":
                    log_out.info(
                        f"✳️ [EVOLUI] {identity} "
                        f"score {d.score_atual}→{score} "
                        f"{'(janela)' if d.na_janela else '(lider)'} "
                        f"chat={norm.chat} "
                        f"img_nova={'sim' if montada.imagem else 'não'}")
                elif d.motivo == "TROCA_IMG_BOA":
                    log_out.info(
                        f"🖼 [TROCA_IMG_BOA] {identity} "
                        f"de {estado.get('lider','')} (ruim) → {norm.chat} (bom) "
                        f"delta={d.delta}s")
                elif d.motivo == "DUP_SILENCIOSO":
                    log_out.debug(
                        f"🔁 [DUP_SILENCIOSO] {identity} sim={d.sim:.2f}")
                elif d.motivo == "SCORE_NAO_EVOLUI":
                    log_out.info(
                        f"🔁 [SCORE_NAO_EVOLUI] {identity} "
                        f"atual={score} salvo={d.score_atual} chat={norm.chat}")

                if d.acao != "EVOLUIR":
                    return True

                # ══ Aplicação no destino (vira o Módulo 3) ══════════
                msg_id_dest = estado["msg_id_dest"]
                edit_count  = estado.get("edit_count", 0) or 0

                ok = await _editar_inner_no_sem(
                    msg_id_dest, montada.texto, montada.imagem,
                    exigir_imagem=d.exigir_imagem)
                if ok:
                    db_set_estado(
                        identity, msg_id_dest, d.novo_score, montada.texto,
                        montada.plat, norm.chat,
                        estado.get("janela_fim", 0), edit_count + 1,
                        estado.get("shadow_reply_id", 0))
                    if d.motivo == "CUPOM_ENRIQUECIDO":
                        log_out.info(
                            f"💎 [CUPOM_ENRIQUECIDO] {identity} "
                            f"novos={getattr(norm, '_cupom_novos', 0)} "
                            f"score={d.novo_score} chat={norm.chat}")
                    elif d.motivo == "TROCA_IMG_BOA":
                        log_out.info(f"✅ [IMG_TROCADA_OK] {identity}")
                    else:
                        log_out.info(
                            f"✏️ [EDITADO_OK] {identity} novo_score={d.novo_score}")
                    return True

                if not d.permite_substituir:
                    if d.motivo == "CUPOM_ENRIQUECIDO":
                        log_out.warning(f"⚠️ [CUPOM_ENRIQUECIDO_FALHOU] {identity}")
                    else:
                        log_out.warning(
                            f"⚠️ [EDIT_FALHOU] {identity} motivo={d.motivo}")
                    return True

                log_out.info(
                    f"🔄 [SUBSTITUI_FALLBACK] {identity} "
                    f"score {d.score_atual}→{d.novo_score} chat={norm.chat}")
                sent = await _substituir_post_com_midia(msg_id_dest, montada)
                if sent:
                    mp = await loop.run_in_executor(_EXECUTOR, ler_mapa)
                    mp[str(montada.msg_id)] = sent.id
                    try:
                        await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
                    except Exception as e:
                        log_sys.error(f"❌ salvar_mapa: {e}")
                    db_set_estado(
                        identity, sent.id, d.novo_score, montada.texto,
                        montada.plat, norm.chat,
                        estado.get("janela_fim", 0), edit_count + 1,
                        estado.get("shadow_reply_id", 0))
                    if d.motivo == "TROCA_IMG_BOA":
                        log_out.info(f"✅ [IMG_TROCADA_OK] {identity} (substitui)")
                    else:
                        log_out.info(
                            f"✅ [SUBSTITUIDO_OK] {identity} "
                            f"novo_id={sent.id} score={d.novo_score}")
                else:
                    log_out.warning(f"⚠️ [SUBSTITUI_FALHOU] {identity}")
                return True
                
        # ═════════════════════════════════════════════════════════════
        # NOVO ENVIO (sem estado prévio)
        # ═════════════════════════════════════════════════════════════
        img = montada.imagem
        sent = None
        for t in range(1, 4):
            try:
                sent = await _enviar_msg(montada.texto, img); break
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log_out.error(f"❌ envio t={t}: {e}")
                if t == 1: img = None
                elif t < 3: await asyncio.sleep(2 ** t)

        if not sent:
            log_out.error(f"❌ Envio falhou | @{montada.chat}")
            return False

        # Gravar estado IMEDIATAMENTE após envio. Falhas em operações
        # secundárias (mapa, sat, burst) NÃO devem deixar o estado
        # ausente — senão um próximo post pode duplicar silenciosamente.
        if identity is not None:
            try:
                janela_fim = time.time() + _JANELA_DISPUTA_S
                db_set_estado(
                    identity, sent.id, score, montada.texto,
                    montada.plat, norm.chat if norm else "",
                    janela_fim, 0, 0,
                )
            except Exception as e:
                log_sys.error(
                    f"❌ db_set_estado FALHOU pós-envio: {e}", exc_info=True
                )
                # Não falha o post — já foi enviado. Próximo post pode
                # duplicar (raro), mas é menos pior.

        # Operações secundárias — falhas individuais não revogam estado
        try:
            mp = await loop.run_in_executor(_EXECUTOR, ler_mapa)
            mp[str(montada.msg_id)] = sent.id
            await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
        except Exception as e:
            log_sys.error(f"❌ salvar_mapa: {e}")

        try:
            await _marcar(montada.msg_id)
        except Exception as e:
            log_sys.error(f"❌ _marcar: {e}")

        try:
            db_registrar_sat(montada.plat, montada.sku)
        except Exception as e:
            log_sys.error(f"❌ db_registrar_sat: {e}")

        try:
            await _burst_add()
        except Exception:
            pass

        if norm is not None and norm.is_override:
            log_out.info(f"🔓 [OVERRIDE_OK] Post liberado publicado | id={sent.id}")

        log_out.info(
            f"🚀 [OK] @{montada.chat}→{GRUPO_DESTINO} | "
            f"{montada.msg_id}→{sent.id} | "
            f"{montada.plat.upper()} score={score} sku={montada.sku} "
            f"identity={identity}"
        )
        return True

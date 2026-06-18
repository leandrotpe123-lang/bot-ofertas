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


# ── Envio ─────────────────────────────────────────────────────────
async def _enviar_msg(texto: str, img) -> object:
    from client import client
    if img:
        if len(texto) <= 1024:
            try:
                return await client.send_file(GRUPO_DESTINO, img, caption=texto,
                                              parse_mode="md", force_document=False)
            except Exception as e:
                log_out.warning(f"⚠️ send_file+caption: {e}")
                try:
                    await client.send_file(GRUPO_DESTINO, img, force_document=False)
                    return await client.send_message(GRUPO_DESTINO, texto,
                                                     parse_mode="md", link_preview=True)
                except Exception as e2:
                    log_out.warning(f"⚠️ send_file sem caption: {e2}")
        else:
            try:
                await client.send_file(GRUPO_DESTINO, img, force_document=False)
                return await client.send_message(GRUPO_DESTINO, texto,
                                                 parse_mode="md", link_preview=False)
            except Exception as e:
                log_out.warning(f"⚠️ send_file longo: {e}")
    return await client.send_message(GRUPO_DESTINO, texto,
                                     parse_mode="md", link_preview=True)


# ─────────────────────────────────────────────────────────────────
# Edição sem deadlock de semáforo
#
# asyncio.Semaphore não é reentrante: se uma função que já segura
# _SEM_ENVIO chamar editar_por_id (que tenta o mesmo semáforo),
# trava. Por isso há duas versões:
#   - _editar_inner_no_sem: SEM semáforo, uso interno apenas
#   - editar_por_id:        COM semáforo, callers externos
# ─────────────────────────────────────────────────────────────────
async def _editar_inner_no_sem(msg_id_dest: int, texto_novo: str,
                                imagem_nova=None,
                                exigir_imagem: bool = False) -> bool:
    """Edita mensagem sem adquirir _SEM_ENVIO. Use APENAS dentro de
    funções que já seguram o semáforo.

    Com exigir_imagem=True, se a edição COM imagem falhar (post nasceu
    sem mídia — o Telegram não deixa colar foto por edição), devolve
    False SEM editar só o texto, para o chamador cair no fallback de
    substituição (deletar+repostar)."""
    from client import client
    for t in range(1, 4):
        try:
            if imagem_nova:
                try:
                    await client.edit_message(
                        GRUPO_DESTINO, msg_id_dest, texto_novo,
                        parse_mode="md", file=imagem_nova,
                    )
                except Exception as e_img:
                    if exigir_imagem:
                        log_out.info(
                            f"🖼 imagem não entrou (post sem mídia) "
                            f"dest_id={msg_id_dest}: {e_img}"
                        )
                        return False
                    await client.edit_message(
                        GRUPO_DESTINO, msg_id_dest, texto_novo,
                        parse_mode="md",
                    )
            else:
                await client.edit_message(
                    GRUPO_DESTINO, msg_id_dest, texto_novo,
                    parse_mode="md",
                )
            log_out.info(f"✏️ Editado | dest_id={msg_id_dest}")
            return True
        except MessageNotModifiedError:
            return True
        except FloodWaitError as e:
            if e.seconds > 120:
                log_out.warning(
                    f"⚠️ FloodWait longo {e.seconds}s — abortando edição"
                )
                return False
            await asyncio.sleep(e.seconds)
        except Exception as e:
            log_out.error(f"❌ edit t={t}: {e}")
            if t < 3:
                await asyncio.sleep(2 ** t)
    return False


async def editar_por_id(msg_id_dest: int, texto_novo: str,
                        imagem_nova=None) -> bool:
    """Versão pública (com semáforo) pra callers externos que não
    seguram _SEM_ENVIO. Delega pra _editar_inner_no_sem."""
    async with config._SEM_ENVIO:
        return await _editar_inner_no_sem(
            msg_id_dest, texto_novo, imagem_nova
        )


async def editar(msg_id_origem: int, texto_novo: str) -> bool:
    loop = asyncio.get_running_loop()
    mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
    id_d = mp.get(str(msg_id_origem))
    if not id_d: return False
    return await editar_por_id(id_d, texto_novo)


# ─────────────────────────────────────────────────────────────────
# Substituição com mídia (deletar + reenviar)
#
# Se delete falha, ABORTA (retorna None). O caller cai pra edição
# comum (que não duplica). Sem essa proteção, o canal duplicaria.
# ─────────────────────────────────────────────────────────────────
async def _substituir_post_com_midia(
    msg_id_dest_antigo: int, montada: MensagemMontada,
) -> Optional[object]:
    """Apaga a mensagem antiga e reenvia com a imagem nova."""
    from client import client
    try:
        # 1. Apaga a mensagem antiga
        try:
            await client.delete_messages(GRUPO_DESTINO, msg_id_dest_antigo)
        except FloodWaitError as e:
            # FloodWait curto: vale esperar e tentar de novo
            if e.seconds <= 30:
                await asyncio.sleep(e.seconds)
                try:
                    await client.delete_messages(
                        GRUPO_DESTINO, msg_id_dest_antigo,
                    )
                except Exception as e2:
                    log_out.warning(
                        f"⚠️ delete 2ª tentativa: {e2} — abortando substituição"
                    )
                    return None  # caller cai pra edição comum
            else:
                log_out.warning(
                    f"⚠️ FloodWait {e.seconds}s no delete — abortando substituição"
                )
                return None
        except Exception as e:
            log_out.warning(
                f"⚠️ delete_messages: {e} — abortando substituição "
                f"(caller cai pra edição)"
            )
            return None

        # 2. Reenvia com a imagem nova
        sent = None
        for t in range(1, 4):
            try:
                sent = await _enviar_msg(montada.texto, montada.imagem)
                break
            except FloodWaitError as e:
                if e.seconds > 60:
                    log_out.warning(
                        f"⚠️ FloodWait longo {e.seconds}s — abortando reenvio"
                    )
                    return None
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log_out.error(f"❌ reenvio t={t}: {e}")
                if t < 3:
                    await asyncio.sleep(2 ** t)

        if sent:
            log_out.info(
                f"🔄 [REENVIO_OK] {msg_id_dest_antigo} → {sent.id} "
                f"@{montada.chat}"
            )
        return sent
    except Exception as e:
        log_out.error(f"❌ _substituir_post_com_midia: {e}", exc_info=True)
        return None


def _midia_grupo_ruim(chat: str) -> bool:
    """Verifica se o chat está na lista de grupos com imagem feia."""
    return (chat or "").lower() in config._GRUPOS_IMG_RUIM


def _deve_substituir_post(
    chat_atual: str, chat_novo: str, tem_midia_nova: bool,
    ts_post_antigo: float,
) -> bool:
    """Decide se devemos deletar+reenviar (em vez de apenas editar)."""
    if not tem_midia_nova:
        return False
    delta = time.time() - ts_post_antigo
    if delta > config._JANELA_REENVIO_MIDIA_S:
        return False
    if _midia_grupo_ruim(chat_atual) and not _midia_grupo_ruim(chat_novo):
        return True
    return True


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
                agora       = time.time()
                na_janela   = agora < (estado.get("janela_fim", 0) or 0)
                lider_atual = estado.get("lider", "") or ""
                edit_count  = estado.get("edit_count", 0) or 0
                msg_id_dest = estado["msg_id_dest"]
                texto_atual = estado.get("texto", "") or ""
                ts_anterior = estado.get("ts", 0) or 0
                score_atual = estado["score"]

                # ── CUPOM ENRIQUECIDO (regra 3) ─────────────────────
                # Cupom que trouxe código(s) novo(s) → edita o MESMO post
                # pra mostrar todos, mesmo de outro grupo e com score
                # igual. Independe do peso do score. O texto manda (mostra
                # os códigos); a imagem é best-effort — o realce de imagem
                # por score melhor continua sendo a DECISÃO 1. Respeita
                # _MAX_EDITS fora da janela. Vem ANTES do override de líder
                # de propósito: post mais rico de outro grupo não trava.
                novos_cup = getattr(norm, "_cupom_novos", 0)
                if novos_cup > 0 and (na_janela or edit_count < _MAX_EDITS):
                    novo_score = max(score, score_atual)
                    ok = await _editar_inner_no_sem(
                        msg_id_dest, montada.texto, montada.imagem)
                    if ok:
                        db_set_estado(
                            identity, msg_id_dest, novo_score, montada.texto,
                            montada.plat, norm.chat,
                            estado.get("janela_fim", 0), edit_count + 1,
                            estado.get("shadow_reply_id", 0))
                        log_out.info(
                            f"💎 [CUPOM_ENRIQUECIDO] {identity} "
                            f"novos={novos_cup} score={novo_score} "
                            f"chat={norm.chat}")
                    else:
                        log_out.warning(
                            f"⚠️ [CUPOM_ENRIQUECIDO_FALHOU] {identity}")
                    return True

                # Override de líder por score: outro grupo só substitui
                # se trouxer cupom/preço melhor (score MAIOR). Score
                # igual ou menor fora da janela é bloqueado.
                if (not na_janela and lider_atual and norm.chat != lider_atual
                        and score <= score_atual):
                    log_out.info(
                        f"🔒 [LIDER_TRAVADO] {identity} "
                        f"lider={lider_atual} candidato={norm.chat} "
                        f"score {score}<={score_atual}"
                    )
                    return True

                # Limite de edições (apenas FORA da janela)
                if edit_count >= _MAX_EDITS and not na_janela:
                    log_out.info(f"🔒 [MAX_EDITS] {identity} edits={edit_count}")
                    return True

                # ── DECISÃO 1: Score MAIOR — sempre processa ────────
                if score > score_atual:
                    log_out.info(
                        f"✳️ [EVOLUI] {identity} "
                        f"score {score_atual}→{score} "
                        f"{'(janela)' if na_janela else '(lider)'} "
                        f"chat={norm.chat} "
                        f"img_nova={'sim' if montada.imagem else 'não'}"
                    )
                    # PRIORIDADE: editar o MESMO post (texto + imagem).
                    ok = await _editar_inner_no_sem(
                        msg_id_dest, montada.texto, montada.imagem,
                        exigir_imagem=bool(montada.imagem),
                    )
                    if ok:
                        db_set_estado(
                            identity, msg_id_dest, score, montada.texto,
                            montada.plat, norm.chat,
                            estado.get("janela_fim", 0), edit_count + 1,
                            estado.get("shadow_reply_id", 0))
                        log_out.info(f"✏️ [EDITADO_OK] {identity} novo_score={score}")
                        return True

                    # PLANO B: edição falhou OU imagem não entrou (post
                    # nasceu sem mídia) → deletar+repostar pra imagem entrar.
                    log_out.info(
                        f"🔄 [SUBSTITUI_FALLBACK] {identity} "
                        f"score {score_atual}→{score} chat={norm.chat}"
                    )
                    sent = await _substituir_post_com_midia(msg_id_dest, montada)
                    if sent:
                        mp = await loop.run_in_executor(_EXECUTOR, ler_mapa)
                        mp[str(montada.msg_id)] = sent.id
                        try:
                            await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
                        except Exception as e:
                            log_sys.error(f"❌ salvar_mapa: {e}")
                        db_set_estado(
                            identity, sent.id, score, montada.texto,
                            montada.plat, norm.chat,
                            estado.get("janela_fim", 0), edit_count + 1,
                            estado.get("shadow_reply_id", 0),
                        )
                        log_out.info(
                            f"✅ [SUBSTITUIDO_OK] {identity} "
                            f"novo_id={sent.id} score={score}"
                        )
                    else:
                        log_out.warning(f"⚠️ [SUBSTITUI_FALHOU] {identity}")
                    return True

                # ── DECISÃO 2: Score IGUAL ──────────────────────────
                if score == score_atual:
                    # Sub-caso: troca de imagem feia → boa. PRIORIDADE:
                    # editar a imagem NO LUGAR; se não entrar (post sem
                    # mídia) ou a edição falhar → PLANO B: deletar+repostar.
                    if (_midia_grupo_ruim(lider_atual)
                            and not _midia_grupo_ruim(norm.chat)
                            and montada.imagem
                            and (agora - ts_anterior) < config._JANELA_REENVIO_MIDIA_S):
                        log_out.info(
                            f"🖼 [TROCA_IMG_BOA] {identity} "
                            f"de {lider_atual} (ruim) → {norm.chat} (bom) "
                            f"delta={int(agora - ts_anterior)}s"
                        )
                        ok = await _editar_inner_no_sem(
                            msg_id_dest, montada.texto, montada.imagem,
                            exigir_imagem=True,
                        )
                        if ok:
                            db_set_estado(
                                identity, msg_id_dest, score, montada.texto,
                                montada.plat, norm.chat,
                                estado.get("janela_fim", 0), edit_count + 1,
                                estado.get("shadow_reply_id", 0),
                            )
                            log_out.info(f"✅ [IMG_TROCADA_OK] {identity}")
                            return True

                        sent = await _substituir_post_com_midia(msg_id_dest, montada)
                        if sent:
                            mp = await loop.run_in_executor(_EXECUTOR, ler_mapa)
                            mp[str(montada.msg_id)] = sent.id
                            try:
                                await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
                            except Exception as e:
                                log_sys.error(f"❌ salvar_mapa: {e}")
                            db_set_estado(
                                identity, sent.id, score, montada.texto,
                                montada.plat, norm.chat,
                                estado.get("janela_fim", 0), edit_count + 1,
                                estado.get("shadow_reply_id", 0),
                            )
                            log_out.info(f"✅ [IMG_TROCADA_OK] {identity} (substitui)")
                            return True

                    # Texto quase igual → ignora silenciosamente
                    from utils.textos import _alma, _sim
                    sim_v = _sim(_alma(montada.texto), _alma(texto_atual))
                    if sim_v > 0.85:
                        log_out.debug(
                            f"🔁 [DUP_SILENCIOSO] {identity} sim={sim_v:.2f}")
                        return True

                # ── DECISÃO 3: Score igual com baixa similaridade / menor ──
                log_out.info(
                    f"🔁 [SCORE_NAO_EVOLUI] {identity} "
                    f"atual={score} salvo={score_atual} chat={norm.chat}")
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

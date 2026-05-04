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
# g._IDS_PROC, g._burst acessados via g.
from logger import log_out, log_sys
from pipeline.deduplicacao import calcular_score, identidade_canonica
from pipeline.montagem import MensagemMontada, preparar_imagem_tg
from pipeline.normalizacao import MensagemNormalizada, _KW_EVENTO
from utils.helpers import ler_mapa, salvar_mapa

# ── Constantes de disputa — definidas em config.py ───────────────
_SAT_MAX_PLAT  = 10
_SAT_BURST_LIM = 6
_SAT_BURST_JAN = 60


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
    from client import client as _client
    return await _client.send_message(GRUPO_DESTINO, texto,
                                      parse_mode="md", link_preview=True)

async def editar_por_id(msg_id_dest: int, texto_novo: str,
                        imagem_nova=None) -> bool:
    from client import client
    async with config._SEM_ENVIO:
        for t in range(1, 4):
            try:
                if imagem_nova:
                    try:
                        await client.edit_message(GRUPO_DESTINO, msg_id_dest,
                                                  texto_novo, parse_mode="md",
                                                  file=imagem_nova)
                    except Exception:
                        await client.edit_message(GRUPO_DESTINO, msg_id_dest,
                                                  texto_novo, parse_mode="md")
                else:
                    await client.edit_message(GRUPO_DESTINO, msg_id_dest,
                                              texto_novo, parse_mode="md")
                log_out.info(f"✏️ Editado | dest_id={msg_id_dest}")
                return True
            except MessageNotModifiedError:
                return True
            except FloodWaitError as e:
                if e.seconds > 120:
                    log_out.warning(f"⚠️ FloodWait longo {e.seconds}s — abortando edição")
                    return False
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log_out.error(f"❌ edit t={t}: {e}")
                if t < 3: await asyncio.sleep(2 ** t)
    return False

async def editar(msg_id_origem: int, texto_novo: str) -> bool:
    loop = asyncio.get_running_loop()
    mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
    id_d = mp.get(str(msg_id_origem))
    if not id_d: return False
    return await editar_por_id(id_d, texto_novo)

async def enviar(montada: MensagemMontada,
                 norm: Optional[MensagemNormalizada] = None) -> bool:
    async with config._SEM_ENVIO:
        loop     = asyncio.get_running_loop()
        identity = None; score = 0

        if norm is not None:
            identity = identidade_canonica(norm)
            score    = calcular_score(norm)
            estado   = db_get_estado(identity)

            if estado:
                agora       = time.time()
                na_janela   = agora < (estado.get("janela_fim", 0) or 0)
                lider_atual = estado.get("lider", "") or ""
                edit_count  = estado.get("edit_count", 0) or 0
                msg_id_dest = estado["msg_id_dest"]
                texto_atual = estado.get("texto", "") or ""

                # Líder travado fora da janela
                if not na_janela and lider_atual and norm.chat != lider_atual:
                    log_out.info(f"🔒 [LIDER_TRAVADO] {identity} "
                                 f"lider={lider_atual} candidato={norm.chat}")
                    return True

                # Limite de edições
                if edit_count >= _MAX_EDITS and not na_janela:
                    log_out.info(f"🔒 [MAX_EDITS] {identity} edits={edit_count}")
                    return True

                # Score maior → EDITA (texto + imagem nova se houver)
                if score > estado["score"]:
                    log_out.info(
                        f"✳️ [EVOLUI] {identity} "
                        f"score {estado['score']}→{score} "
                        f"{'(janela)' if na_janela else '(lider)'} "
                        f"chat={norm.chat} "
                        f"img_nova={'sim' if montada.imagem else 'não'}"
                    )
                    ok = await editar_por_id(
                        msg_id_dest, montada.texto, montada.imagem)
                    if ok:
                        db_set_estado(
                            identity, msg_id_dest, score, montada.texto,
                            montada.plat, norm.chat,
                            estado.get("janela_fim", 0), edit_count + 1,
                            estado.get("shadow_reply_id", 0))
                        log_out.info(f"✏️ [EDITADO_OK] {identity} novo_score={score}")
                    else:
                        log_out.warning(f"⚠️ [EDIT_FALHOU] {identity}")
                    return ok

                # Score igual + texto quase igual → ignora silenciosamente
                if score == estado["score"]:
                    from utils.textos import _alma, _sim
                    sim_v = _sim(_alma(montada.texto), _alma(texto_atual))
                    if sim_v > 0.85:
                        log_out.debug(
                            f"🔁 [DUP_SILENCIOSO] {identity} sim={sim_v:.2f}")
                        return True

                # Score menor ou igual mas texto diferente → ignora
                log_out.info(
                    f"🔁 [SCORE_IGUAL/MENOR] {identity} "
                    f"atual={score} salvo={estado['score']} chat={norm.chat}")
                return True

        # Novo envio (não existe estado prévio)
        img = montada.imagem; sent = None
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
            log_out.error(f"❌ Envio falhou | @{montada.chat}"); return False

        mp = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        mp[str(montada.msg_id)] = sent.id
        try:
            await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
        except Exception as e:
            log_sys.error(f"❌ salvar_mapa: {e}")

        await _marcar(montada.msg_id)
        db_registrar_sat(montada.plat, montada.sku)
        try:
            await _burst_add()
        except Exception:
            pass

        if identity is not None:
            janela_fim = time.time() + _JANELA_DISPUTA_S
            db_set_estado(identity, sent.id, score, montada.texto,
                          montada.plat, norm.chat if norm else "",
                          janela_fim, 0, 0)

        if norm is not None and norm.is_override:
            log_out.info(f"🔓 [OVERRIDE_OK] Post liberado publicado | id={sent.id}")

        if montada.plat == "magalu" and montada.mapa:
            from plataformas.magalu import _cuttly_background
            for orig, conv in montada.mapa.items():
                if "partner_id" in conv and "leoind.com.br" not in conv:
                    try:
                        asyncio.create_task(_cuttly_background(conv, montada.msg_id))
                    except Exception:
                        pass

        log_out.info(
            f"🚀 [OK] @{montada.chat}→{GRUPO_DESTINO} | "
            f"{montada.msg_id}→{sent.id} | "
            f"{montada.plat.upper()} score={score} sku={montada.sku} "
            f"identity={identity}"
        )
        return True
                

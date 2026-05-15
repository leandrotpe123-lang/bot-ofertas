"""Posts bloqueados aguardando confirmação de bom preço via comentário."""
from __future__ import annotations
import asyncio
import re
import time
from dataclasses import dataclass
from typing import Dict, Optional

import globals as g
from logger import log_cls, log_out

_RE_COMENTARIO_BOM_PRECO = re.compile(
    r'\b(?:precin|precinho|barato|bom\s*pre[cç]o|pre[cç]o\s*bom|'
    r'imperd[ií]vel|absurdo|relâmpago|relampago|tá\s*voando|'
    r'queimando|tá\s*barato|muito\s*bom|excelente|ótimo|show|'
    r'top\s*de|mds|caramba|nossa|que\s*desconto|que\s*pre[cç]o|'
    r'durou|e\s*foi|foi\s*r[aá]pido)\b',
    re.I,
)


@dataclass
class PendingPost:
    bruta: object
    ts:    float
    TTL:   float = 60.0


_pending_bloqueados: Dict[int, PendingPost] = {}


async def _registrar_pending(bruta) -> None:
    async with g._pending_lock:
        _pending_bloqueados[bruta.msg_id] = PendingPost(
            bruta=bruta, ts=time.monotonic())
        log_cls.debug(f"⏳ Pending | id={bruta.msg_id} chat={bruta.chat}")
    asyncio.create_task(_limpar_pending_expirados())


async def _limpar_pending_expirados() -> None:
    await asyncio.sleep(65)
    agora = time.monotonic()
    async with g._pending_lock:
        expirados = [k for k, v in _pending_bloqueados.items()
                     if agora - v.ts > v.TTL]
        for k in expirados:
            del _pending_bloqueados[k]
            log_cls.debug(f"🗑 Pending expirado | id={k}")


async def _tentar_liberar_pending(msg_id_reply_to: int,
                                   texto_comentario: str) -> Optional[object]:
    async with g._pending_lock:
        pending = _pending_bloqueados.get(msg_id_reply_to)
        if not pending:
            return None
        if time.monotonic() - pending.ts > pending.TTL:
            del _pending_bloqueados[msg_id_reply_to]
            log_cls.debug(f"🗑 Pending expirado na confirmação | id={msg_id_reply_to}")
            return None
        if not _RE_COMENTARIO_BOM_PRECO.search(texto_comentario):
            log_cls.debug(f"💬 Comentário não confirma preço: {texto_comentario!r}")
            return None
        bruta_original = pending.bruta
        del _pending_bloqueados[msg_id_reply_to]
        log_out.info(
            f"✅ Pending LIBERADO | id={msg_id_reply_to} "
            f"comentário={texto_comentario!r}")
        return bruta_original


async def _processar_post_liberado(bruta, texto_comentario: str) -> None:
    """Processa post bloqueado liberado por comentário de bom preço."""
    import random
    from config import GRUPO_DESTINO
    from globals import _get_session
    from pipeline.normalizacao import normalizar
    from pipeline.deduplicacao import deve_enviar_async
    from pipeline.montagem import montar
    from pipeline.publicacao import enviar
    from handlers.shadow_reply import _RE_SHADOW_BLOCK
    from config import _EXECUTOR
    from utils.helpers import ler_mapa
    try:
        norm = await normalizar(bruta, is_override=True)
        if norm is None: return
        if not await deve_enviar_async(norm): return
        montada = await montar(norm)
        ok = await enviar(montada, norm=norm)
        if ok:
            log_out.info(f"🔓 [POST_LIBERADO_OK] id={bruta.msg_id}")
            loop     = asyncio.get_running_loop()
            mp       = await loop.run_in_executor(_EXECUTOR, ler_mapa)
            msg_dest = mp.get(str(bruta.msg_id))
            if msg_dest:
                await asyncio.sleep(random.uniform(5, 15))
                linhas_limpas = [
                    l for l in texto_comentario.strip().splitlines()
                    if not _RE_SHADOW_BLOCK.search(l)
                ]
                texto_reply = "\n".join(linhas_limpas).strip()
                if texto_reply:
                    from client import client
                    try:
                        await client.send_message(
                            GRUPO_DESTINO, texto_reply,
                            reply_to=int(msg_dest), parse_mode="md")
                        log_out.info(f"💬 [LIBERADO_REPLY] {texto_reply!r}")
                    except Exception as e:
                        log_out.warning(f"⚠️ Reply pós-liberação: {e}")
    except Exception as e:
        log_out.error(f"❌ _processar_post_liberado: {e}", exc_info=True)
      

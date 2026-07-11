"""
Shadow Reply Engine v81.0 — Master Edition (Final)
"""

from __future__ import annotations
import asyncio
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Dict, Any

import globals as g
from config import GRUPO_DESTINO, _EXECUTOR
from database import db_get_estado, db_set_estado, _db
from logger import log_out
from pipeline.saida import editar_por_id
from config import _MAX_EDITS
from utils.helpers import ler_mapa
from utils.hashes import _fp4

# ==================== MEMÓRIA DE CONTEXTO ====================
_context_memory: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
    "last_restock": 0.0,
    "last_cupons": 0.0,
    "reply_count": 0,
    "last_activity": 0.0,
    "ignored_count": 0
})

def _get_context(identity: str) -> Dict[str, Any]:
    return _context_memory[identity]


def _update_context(identity: str, **kwargs) -> None:
    ctx = _get_context(identity)
    ctx.update(kwargs)
    ctx["last_activity"] = time.time()


# ==================== PIPELINE PLUGÁVEL ====================
@dataclass(frozen=True)
class ShadowContext:
    texto: str
    identity: str
    msg_dest: int
    row: tuple
    score: int
    tipo: str
    bruta: Any


HandlerFunc = Callable[[ShadowContext], Any]
PIPELINE: Dict[str, HandlerFunc] = {}


def register_handler(tipo: str):
    def decorator(func: HandlerFunc):
        PIPELINE[tipo] = func
        return func
    return decorator


# ==================== SCORING ====================
_BLOCK_KEYWORDS = {"grupo", "canal", "comunidade", "pix", "link", "t.me", "http"}
_POSITIVE_WORDS = {"funcionou", "funciona", "consegui", "conseguiram", "testem", "deu certo",
                   "ótimo", "top", "melhor", "bom", "barato", "precin", "imperdível", "absurdo",
                   "relâmpago", "voando", "queimando", "valeu", "ativo ainda"}

_RESTOCK_WORDS = {"voltando", "voltou", "reativou", "reabasteceu", "restock", "de volta",
                  "voltei", "ativo de novo", "reabastecido", "retornou", "volta"}


def _calcular_score_semantico(texto: str) -> tuple[str, int]:
    if not texto or len(texto.strip()) < 4:
        return 'bloquear', 0

    t = texto.strip()
    t_lower = t.lower()
    palavras = [p.lower().strip() for p in t.split()]
    num_palavras = len(palavras)
    num_linhas = len([l for l in t.splitlines() if l.strip()])

    if any(kw in t_lower for kw in _BLOCK_KEYWORDS) or re.search(r'https?://|t\.me/', t_lower):
        return 'bloquear', -999

    if re.search(r'cupom|cupons?|r\$\s*\d+\s*off', t_lower):
        return 'edicao_cupons', 100

    if any(word in t_lower for word in _RESTOCK_WORDS):
        return 'edicao_restock', 95

    score = sum(18 for p in palavras if p in _POSITIVE_WORDS)
    score += 25 if 5 <= num_palavras <= 16 and num_linhas <= 5 else -10
    score += 15 if "ainda" in t_lower and "ativo" in t_lower else 0

    if num_palavras > 25 or num_linhas > 8:
        score -= 45

    return ('humanizado' if score >= 48 else 'bloquear', max(score, 0))


# ==================== HANDLERS ====================
@register_handler("edicao_cupons")
async def handle_cupons(ctx: ShadowContext):
    # ... (mesma lógica da versão anterior)
    texto_atual = ctx.row[3] or ""
    edit_count = ctx.row[7] or 0
    if edit_count >= _MAX_EDITS:
        return

    bloco_novo = "\n".join(line.strip() for line in ctx.texto.splitlines() if line.strip() and not re.search(r'https?://', line)).strip()
    if not bloco_novo:
        return

    texto_final = f"{texto_atual.rstrip()}\n\n{bloco_novo}"
    ok = await editar_por_id(int(ctx.msg_dest), texto_final)
    if ok:
        db_set_estado(ctx.identity, int(ctx.msg_dest), (ctx.row[2] or 0) + 1, texto_final,
                      ctx.row[4] or "", ctx.row[5] or "", ctx.row[6] or 0.0, edit_count + 1, ctx.row[1] or 0)
        _update_context(ctx.identity, last_cupons=time.time())
        log_out.info(f"✏️ [CUPONS_OK] {ctx.identity}")


@register_handler("edicao_restock")
async def handle_restock(ctx: ShadowContext):
    await _safe_restock(ctx.identity)
    _update_context(ctx.identity, last_restock=time.time())


@register_handler("humanizado")
async def handle_humanizado(ctx: ShadowContext):
    if ctx.row[1] and ctx.row[1] > 0:
        return
    await _postar_reply_original(ctx.texto, ctx.msg_dest, ctx.identity, ctx.row)


# ==================== AUXILIARES ====================
async def _safe_restock(identity: str):
    try:
        fp = _fp4(f"identity|{identity}")
        # Destrava o claim que DECIDE (g._atomic_mem). dedupe_temp saiu do
        # caminho operacional (Frente B): o DELETE ali era no-op.
        if hasattr(g, '_atomic_lck_obj') and g._atomic_lck_obj:
            async with g._atomic_lck_obj:
                g._atomic_mem.pop(fp, None)
        log_out.info(f"♻️ [RESTOCK] {identity}")
    except Exception as e:
        log_out.warning(f"Restock falhou {identity}: {e}")


async def _postar_reply_original(texto: str, msg_dest: int, identity: str, row: tuple) -> bool:
    from client import client
    try:
        linhas = [l.strip() for l in texto.splitlines() if l.strip() and not re.search(r'https?://', l)]
        texto_final = "\n".join(linhas).strip()
        if not texto_final: return False

        sent = await client.send_message(GRUPO_DESTINO, texto_final, reply_to=msg_dest, parse_mode="md")
        db_set_estado(identity, msg_dest, row[2] or 0, row[3] or "", row[4] or "", row[5] or "", row[6] or 0.0, row[7] or 0, sent.id)
        _update_context(identity, reply_count=_get_context(identity)["reply_count"] + 1)
        log_out.info(f"💬 [SHADOW_OK] {identity}")
        return True
    except Exception as e:
        log_out.error(f"Shadow reply falhou {identity}: {e}")
        return False


# ==================== MAIN ====================
async def processar_shadow_reply(bruta) -> bool:
    if not getattr(bruta, 'is_reply', False) or not getattr(bruta, 'reply_to', None):
        return False

    try:
        tipo, score = _calcular_score_semantico(bruta.texto)
        log_out.debug(f"[SHADOW] Score={score:3d} | Tipo={tipo:12s} | {bruta.texto[:65]!r}")

        if tipo == 'bloquear':
            return False

        loop = asyncio.get_running_loop()
        mp = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        msg_dest = mp.get(str(bruta.reply_to))
        if not msg_dest:
            return False

        with _db() as db:
            row = db.execute(
                "SELECT identity,shadow_reply_id,score,texto,plat,lider,janela_fim,edit_count "
                "FROM oferta_estado WHERE msg_id_dest=?", (int(msg_dest),)
            ).fetchone()

        if not row:
            return False

        ctx = ShadowContext(bruta.texto, row[0], int(msg_dest), row, score, tipo, bruta)

        handler = PIPELINE.get(tipo)
        if handler:
            await handler(ctx) if asyncio.iscoroutinefunction(handler) else handler(ctx)
            return True

        return False

    except Exception as e:
        log_out.error(f"💥 Erro no Shadow Reply: {e}", exc_info=True)
        return False

"""Shadow reply engine — captura e filtra comentários dos grupos monitorados."""
from __future__ import annotations
import asyncio
import re
from typing import Optional

from config import GRUPO_DESTINO, _EXECUTOR
from database import db_get_estado, db_set_estado, _db
from logger import log_out
from pipeline.publicacao import editar_por_id, _MAX_EDITS
from utils.helpers import ler_mapa

# ── Filtros de bloco / positivo ───────────────────────────────────
_RE_SHADOW_BLOCK = re.compile(
    r'https?://|t\.me/|telegram\.me/|telegram\.org/|'
    r'whatsapp\.com|wa\.me|'
    r'\bgrupo\b|\bcanal\b|\bcomunidade\b|\blink\b|\bencaminhado\b|'
    r'@\w+|#\w+',
    re.I,
)
_RE_SHADOW_POSITIVO = re.compile(
    r'\b(?:precin|barato|bom\s*pre[cç]o|pre[cç]o\s*bom|imperd[ií]vel|'
    r'absurdo|relâmpago|relampago|voando|queimando|escald|'
    r'dá\s*pra\s*usar|voltando|testem|conseguiram|'
    r'ainda\s*ativo|ativo\s*ainda|'
    r'durou\s*em|foi\s*r[aá]pido|corr[ae]|cupom\s*ativo|'
    r'que\s*desconto|que\s*pre[cç]o|mds|caramba|nossa)\b',
    re.I,
)
_RE_LINHA_CUPOM_LISTA = re.compile(
    r'r\$\s*\d+\s+off\s+em\s+r\$\s*\d+\s*:\s*[A-Z0-9]{4,}|'
    r'cupons?\s+(?:ainda\s+)?ativos?\s*:|ainda\s+ativos?\s*:',
    re.I,
)


def _classificar_shadow(texto: str) -> str:
    t = texto.strip()
    if not t: return 'bloquear'
    if _RE_SHADOW_BLOCK.search(t): return 'bloquear'
    linhas = [l.strip() for l in t.splitlines() if l.strip()]
    cupons_na_lista = sum(1 for l in linhas if _RE_LINHA_CUPOM_LISTA.search(l))
    if cupons_na_lista >= 1: return 'edicao_cupons'
    if re.search(r'\b(?:voltando|voltou|reativou|ativo\s+de\s+novo|de\s+volta|'
                 r'testem|conseguiram\s+usar)\b', t, re.I):
        return 'edicao_restock'
    palavras = t.split()
    if len(palavras) <= 8 and len(linhas) <= 2:
        if _RE_SHADOW_POSITIVO.search(t): return 'humanizado'
        if len(t) < 5: return 'bloquear'
        if re.fullmatch(r'[\s\U0001F300-\U0001FAFF\U00002600-\U000027BF'
                        r'\U0001F900-\U0001F9FF\u2B50\u2B55]+', t):
            return 'humanizado'
        return 'humanizado'
    return 'bloquear'


async def _postar_reply_original(texto: str, msg_dest: int,
                                  identity: str, row) -> bool:
    from client import client
    linhas_limpas = [l for l in texto.strip().splitlines()
                     if not _RE_SHADOW_BLOCK.search(l)]
    texto_final = "\n".join(linhas_limpas).strip()
    if not texto_final: return False
    try:
        sent = await client.send_message(
            GRUPO_DESTINO, texto_final,
            reply_to=msg_dest, parse_mode="md")
        db_set_estado(
            identity, msg_dest,
            row[2] or 0, row[3] or "",
            row[4] or "", row[5] or "",
            row[6] or 0.0, row[7] or 0,
            shadow_reply_id=sent.id)
        log_out.info(f"💬 [SHADOW_OK] {texto_final!r} → reply {msg_dest}")
        return True
    except Exception as e:
        log_out.error(f"❌ Shadow reply: {e}"); return False


async def processar_shadow_reply(bruta) -> bool:
    if not bruta.is_reply or not bruta.reply_to: return False

    tipo = _classificar_shadow(bruta.texto)
    if tipo == 'bloquear':
        log_out.debug(f"🔇 Shadow bloqueado | {bruta.texto[:50]!r}")
        return False

    # Verifica se libera post pendente
    if tipo in ('humanizado', 'edicao_restock'):
        from handlers.pending import _tentar_liberar_pending, _processar_post_liberado
        from handlers.pending import _RE_COMENTARIO_BOM_PRECO
        if _RE_COMENTARIO_BOM_PRECO.search(bruta.texto):
            bruta_liberada = await _tentar_liberar_pending(bruta.reply_to, bruta.texto)
            if bruta_liberada:
                log_out.info(f"🔓 Post bloqueado liberado | id={bruta_liberada.msg_id}")
                asyncio.create_task(_processar_post_liberado(bruta_liberada, bruta.texto))

    loop     = asyncio.get_running_loop()
    mp       = await loop.run_in_executor(_EXECUTOR, ler_mapa)
    msg_dest = mp.get(str(bruta.reply_to))
    if not msg_dest: return False

    try:
        with _db() as db:
            row = db.execute(
                "SELECT identity,shadow_reply_id,score,texto,plat,"
                "lider,janela_fim,edit_count "
                "FROM oferta_estado WHERE msg_id_dest=?",
                (int(msg_dest),)).fetchone()
    except Exception:
        return False
    if not row: return False

    identity   = row[0]; shadow_id  = row[1] or 0
    score_at   = row[2] or 0; texto_at = row[3] or ""
    plat_at    = row[4] or ""; lider   = row[5] or ""
    janela_fim = row[6] or 0.0; edit_ct = row[7] or 0

    if tipo == 'edicao_cupons':
        log_out.info(f"📋 [SHADOW_CUPONS] Enriquecendo | identity={identity}")
        bloco_limpo = "\n".join(
            l for l in bruta.texto.strip().splitlines()
            if not _RE_SHADOW_BLOCK.search(l)
        ).strip()
        if not bloco_limpo: return True
        texto_novo = texto_at.rstrip() + "\n\n" + bloco_limpo
        if edit_ct < _MAX_EDITS:
            ok = await editar_por_id(int(msg_dest), texto_novo)
            if ok:
                db_set_estado(identity, int(msg_dest), score_at + 1,
                              texto_novo, plat_at, lider, janela_fim,
                              edit_ct + 1, shadow_id)
                log_out.info(f"✏️ [CUPONS_OK] identity={identity}")
        return True

    if tipo == 'edicao_restock':
        log_out.info(f"♻️ [SHADOW_RESTOCK] identity={identity}")
        try:
            chave = identity.split('|', 1)[-1][:20]
            with _db() as db:
                db.execute("DELETE FROM dedupe_temp WHERE fp LIKE ?",
                           (f"%{chave}%",))
        except Exception:
            pass
        palavras = bruta.texto.strip().split()
        if len(palavras) <= 6 and shadow_id == 0:
            return await _postar_reply_original(bruta.texto, int(msg_dest), identity, row)
        return True

    # humanizado
    if shadow_id and shadow_id > 0:
        log_out.debug(f"🔇 Shadow já enviado para {identity}"); return False
    palavras = bruta.texto.strip().split()
    if len(palavras) > 12:
        log_out.debug(f"🔇 Shadow longo: {len(palavras)} palavras"); return False
    return await _postar_reply_original(bruta.texto, int(msg_dest), identity, row)

"""Camada 4 — Deduplicação, score evolutivo e identidade canônica."""
from __future__ import annotations
import asyncio
import re
import time
from typing import Optional

from database import db_get_dedupe, db_set_dedupe, db_buscar_janela_rapida
from globals import _atomic_lck_obj, _atomic_mem
from logger import log_ded
from pipeline.normalizacao import EstadoEvento, MensagemNormalizada
from utils.hashes import _fp4, _fp_benef
from utils.textos import _alma, _cupons_set, _benef_set, _janela, _normalizar_valor, _sim, _SIM_FORTE

# ── Atomic locks ─────────────────────────────────────────────────
async def _get_atomic_lck():
    return _atomic_lck_obj

async def _atomic_check(fp: str) -> Optional[float]:
    async with (await _get_atomic_lck()):
        return _atomic_mem.get(fp)

async def _atomic_claim(fp: str) -> bool:
    async with (await _get_atomic_lck()):
        if fp in _atomic_mem: return False
        _atomic_mem[fp] = time.monotonic(); return True

async def _atomic_release(fp: str):
    async with (await _get_atomic_lck()):
        _atomic_mem.pop(fp, None)

# ── Score evolutivo ───────────────────────────────────────────────
def calcular_score(norm: MensagemNormalizada) -> int:
    texto = norm.texto_limpo; score = 0
    if norm.mapa:                                          score += 3
    if re.search(r'r\$\s*[\d.,]+', texto, re.I):          score += 2
    if norm.cupom:                                         score += 2
    if re.search(r'\d+\s*%\s*off', texto, re.I):          score += 2
    if re.search(r'r\$\s*[\d.,]+\s*off', texto, re.I):    score += 2
    if re.search(r'(acima|mínimo|min)\s+de\s+r\$', texto, re.I): score += 1
    if re.search(r'frete\s+gr[aá]t', texto, re.I):        score += 1
    if norm.tem_midia:                                     score += 1
    if norm.sku:                                           score += 1
    return score

def identidade_canonica(norm: MensagemNormalizada) -> str:
    """
    Chave estável da oferta. Prioridade:
      1. ids_globais (ASIN, promotion_id, sku Shopee/Magalu)
      2. cupom
      3. URL canônica do primeiro link convertido
      4. hash do texto normalizado (último recurso)
    """
    if norm.ids_globais:
        return f"{norm.plat}|{norm.ids_globais[0]}"
    if norm.cupom:
        return f"{norm.plat}|cup|{norm.cupom}"
    # URL convertida como identidade estável
    # Captura o caso "mesma URL, texto levemente diferente"
    if norm.mapa:
        from utils.urls import _cache_key
        primeira_url = next(iter(norm.mapa.values()), None)
        if primeira_url:
            return f"{norm.plat}|url|{_cache_key(primeira_url)}"
    alma_v = _alma(norm.texto_limpo)
    return f"{norm.plat}|txt|{_fp4(alma_v)}"

# ── Deduplicação assíncrona ───────────────────────────────────────
async def deve_enviar_async(norm: MensagemNormalizada) -> bool:
    """
    Dedupe da camada 4 — coopera com o sistema de score do enviar().

    Comportamento:
      • Mesma oferta + mesmo grupo (chat) em janela curta → BLOQUEIA
      • Mesma oferta + grupo diferente → DEIXA PASSAR (enviar() decide via score)
      • Oferta nova → registra e passa
      • Restock / benefício novo → passa (lógica original preservada)
    """
    try:
        texto       = norm.texto_limpo; plat = norm.plat
        estado      = norm.estado_evento; ids_globais = norm.ids_globais
        cupons      = _cupons_set(texto); alma_v = _alma(texto)
        benef       = _benef_set(texto);  valores = _normalizar_valor(texto)
        janela      = _janela(plat)
        chat        = (norm.chat or "").lower()

        if estado == EstadoEvento.RESTOCKED:
            if ids_globais:
                fp_rst = _fp4(f"{plat}|{ids_globais[0]}|restock|{int(time.time()//60)}")
                ok = await _atomic_claim(fp_rst)
                if not ok: log_ded.info(f"🔁 [BLOQ_RESTOCK] {ids_globais[0]}"); return False
                db_set_dedupe(_fp4(f"{plat}|{ids_globais[0]}"), plat, list(cupons),
                              alma_v, "restock", ids_globais[0], ids_globais[0], list(benef))
                log_ded.info(f"♻️ [PASSOU_RESTOCK] {ids_globais[0]}")
            return True

        if estado == EstadoEvento.SEEN:
            log_ded.info(f"🔁 [BLOQ_SEEN] ids={ids_globais}"); return False

        if estado == EstadoEvento.EXPIRED:
            if ids_globais:
                fp_base = _fp4(f"{plat}|{ids_globais[0]}")
                entrada = db_get_dedupe(fp_base)
                if entrada:
                    benef_ant = frozenset(entrada.get("benef", []))
                    if benef and benef != benef_ant:
                        fp_ben = _fp_benef(ids_globais[0], plat, benef)
                        ok = await _atomic_claim(fp_ben)
                        if not ok: return False
                        db_set_dedupe(fp_base, plat, list(cupons), alma_v, "benef",
                                      ids_globais[0], ids_globais[0], list(benef))
                        log_ded.info(f"✳️ [PASSOU_BENEF_NOVO] {ids_globais[0]}")
                        return True
            return False

        # Dedupe por (id_global + chat) — permite outros grupos passarem para o enviar() decidir via score
        for id_global in ids_globais:
            fp_chat = _fp4(f"{plat}|{id_global}|{chat}")
            ts_mem = await _atomic_check(fp_chat)
            if ts_mem is not None and (time.monotonic() - ts_mem) < janela:
                log_ded.info(f"🔁 [BLOQ_MESMO_GRUPO] {id_global} chat={chat}")
                return False
            ok = await _atomic_claim(fp_chat)
            if not ok:
                log_ded.info(f"🔁 [BLOQ_RACE] {id_global} chat={chat}"); return False
            entrada_db = db_get_dedupe(fp_chat)
            if entrada_db:
                delta = time.time() - entrada_db.get("ts", 0)
                if delta < janela:
                    await _atomic_release(fp_chat)
                    log_ded.info(f"🔁 [BLOQ_DB_MESMO_GRUPO] {id_global} chat={chat}")
                    return False
            db_set_dedupe(fp_chat, plat, list(cupons), alma_v, "id",
                          id_global, id_global, list(benef))
            log_ded.info(f"✅ [PASSOU] {id_global} chat={chat} → enviar() decide")
            return True

        if norm.cupom:
            fp_cup = _fp4(f"{plat}|cup|{norm.cupom}|{chat}")
            ts_mem = await _atomic_check(fp_cup)
            if ts_mem is not None and (time.monotonic() - ts_mem) < janela:
                log_ded.info(f"🔁 [BLOQ_CUP_MESMO_GRUPO] {norm.cupom} chat={chat}")
                return False
            ok = await _atomic_claim(fp_cup)
            if not ok: log_ded.info(f"🔁 [BLOQ_CUP_RACE] {norm.cupom}"); return False
            entrada_db = db_get_dedupe(fp_cup)
            if entrada_db:
                delta = time.time() - entrada_db.get("ts", 0)
                if delta < janela:
                    await _atomic_release(fp_cup)
                    log_ded.info(f"🔁 [BLOQ_CUP_DB_MESMO_GRUPO]")
                    return False
            db_set_dedupe(fp_cup, plat, list(cupons), alma_v, "cup",
                          "", "", list(benef))
            log_ded.info(f"✅ [PASSOU_CUP] {norm.cupom} chat={chat} → enviar() decide")
            return True

        fp_txt = _fp4(f"{plat}|{alma_v}|{chat}|{'|'.join(sorted(benef))}|{valores}")
        ok = await _atomic_claim(fp_txt)
        if not ok: log_ded.info(f"🔁 [BLOQ_TEXTO_RACE]"); return False
        for e in db_buscar_janela_rapida(plat, janela=max(janela, 900)):
            alma_ant = e.get("alma", "")
            if not alma_ant: continue
            if _sim(alma_v, alma_ant) > _SIM_FORTE:
                fp_ant = e.get("fp", "")
                if chat and chat in (fp_ant or ""):
                    await _atomic_release(fp_txt)
                    log_ded.info(f"🔁 [BLOQ_SIM_MESMO_GRUPO]"); return False
        db_set_dedupe(fp_txt, plat, list(cupons), alma_v, "gen",
                      "", "", list(benef))
        log_ded.info(f"✅ [PASSOU_TEXTO] chat={chat} → enviar() decide")
        return True

    except Exception as e:
        log_ded.error(f"❌ ERRO DEDUPE: {e}"); return True
    

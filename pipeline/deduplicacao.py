"""
Camada 4 — Deduplicação inteligente, score evolutivo e identidade canônica (SEM dependência de plataforma)
"""

from __future__ import annotations
import asyncio
import re
import time
from typing import Optional, Tuple

import config
from database import (
    db_get_dedupe, db_set_dedupe, db_buscar_janela_rapida, db_get_estado,
)
import globals as g
from logger import log_ded
from pipeline.normalizacao import (
    MensagemNormalizada,
    extrair_todos_cupons,
    _KW_CUPOM,
)
from utils.hashes import _fp4
from utils.textos import _alma, _cupons_set, _benef_set
from utils.urls import _cache_key, _netloc


# ─────────────────────────────────────────────────────────────
# TIPOS
# ─────────────────────────────────────────────────────────────
def _eh_post_cupom(texto: str) -> bool:
    return bool(re.search(r'\b(cupom|código|coupon)\b', texto, re.I))


def _eh_post_cashback(texto: str) -> bool:
    return bool(re.search(r'cashback', texto, re.I))


def _eh_post_evento(texto: str) -> bool:
    return bool(re.search(r'roleta|sorteio|desafio|evento|promoção', texto, re.I))


# ─────────────────────────────────────────────────────────────
# IDENTIDADE CANÔNICA (AGORA SEM PLATAFORMA)
# ─────────────────────────────────────────────────────────────
def identidade_canonica(norm: MensagemNormalizada) -> str:
    """
    Identidade GLOBAL (independente de Amazon/Shopee/ML)

    ✔ Mesma oferta = mesma chave
    ✔ Plataforma não interfere mais
    """

    texto = norm.texto_limpo

    # 1. lista de cupons
    if norm.cupons:
        cupons = sorted(set(norm.cupons))
        return f"cuplist|{_fp4('|'.join(cupons))}"

    # 2. cupom explícito
    if _eh_post_cupom(texto) and norm.cupom:
        return f"cup|{norm.cupom.upper()}"

    # 3. cashback
    if _eh_post_cashback(texto):
        return f"cash|{_fp4(texto)}"

    # 4. produto (ID global)
    if norm.ids_globais:
        return f"prod|{norm.ids_globais[0]}"

    # 5. evento/campanha
    if _eh_post_evento(texto):
        return f"event|{_fp4(texto)}"

    # 6. fallback texto
    return f"txt|{_fp4(_alma(texto))}"


# ─────────────────────────────────────────────────────────────
# JANELA
# ─────────────────────────────────────────────────────────────
def _janela_por_tipo(tipo: str) -> float:
    return float(config._JANELA_EVENTO_S)


# ─────────────────────────────────────────────────────────────
# ATÔMICO
# ─────────────────────────────────────────────────────────────
async def _atomic_check_and_claim(fp: str, janela: float) -> Tuple[bool, Optional[float]]:
    async with g._atomic_lck_obj:
        agora = time.monotonic()
        ts = g._atomic_mem.get(fp)

        if ts and (agora - ts) < janela:
            return True, ts

        g._atomic_mem[fp] = agora
        return False, ts


# ─────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────
async def deve_enviar_async(norm: MensagemNormalizada) -> bool:
    try:
        texto = norm.texto_limpo
        identidade = identidade_canonica(norm)
        janela = _janela_por_tipo("global")

        fp = _fp4(f"identity|{identidade}")

        na_janela, _ = await _atomic_check_and_claim(fp, janela)

        if na_janela:
            log_ded.info(f"🔄 DUPLICADO: {identidade}")
            return True

        db_set_dedupe(
            fp,
            norm.plat,  # ← plataforma fica só como METADADO, não regra
            list(_cupons_set(texto)),
            _alma(texto),
            "",
            "",
            list(_benef_set(texto)),
        )

        log_ded.info(f"✅ NOVO: {identidade}")
        return True

    except Exception as e:
        log_ded.error(f"ERRO: {e}", exc_info=True)
        return True

"""Utilitários de texto: normalização, alma, similaridade, benefícios."""
from __future__ import annotations
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Optional

from config import _JANELA_C3  # importado de pipeline.normalizacao via config

# ── Constantes de deduplicação ────────────────────────────────────
_RUIDO_NORM = frozenset({
    "promo","promocao","promoção","oferta","desconto","cupom","corre","aproveita",
    "urgente","gratis","grátis","frete","hoje","agora","imperdivel","imperdível",
    "exclusivo","limitado","corra","ative","use","saiu","vazou","resgate","acesse",
    "confira","link","clique","app","relampago","relâmpago","click","veja","novo",
    "nova","valido","válido","somente","apenas","ate","até","partir","ainda","volta",
    "ativo","disponivel","disponível","pix","parcelas","unidades","estoque",
    "shopee","amazon","magalu","magazineluiza","magazine",
})
_RE_EMJ_NORM = re.compile(r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF]+", re.UNICODE)
_SIM_FORTE   = 0.82
_SIM_MEDIO   = 0.70


def _rm_acentos(t: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", t)
        if unicodedata.category(c) != "Mn"
    )


def _alma(t: str) -> str:
    """Representação canônica do texto para similaridade semântica."""
    t = _rm_acentos(t.lower())
    t = re.sub(r'https?://\S+', ' ', t)
    t = _RE_EMJ_NORM.sub(' ', t)
    t = re.sub(r'(\d+\s?(gb|tb|mah|v|w|hz|fps))', r' ATTR_\1 ', t)
    t = re.sub(r'r\$\s*[\d.,]+', ' VALOR ', t)
    t = re.sub(r'\b\d+%', ' PCT ', t)
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return ' '.join(sorted(
        w for w in t.split()
        if w not in _RUIDO_NORM and (len(w) > 2 or "attr_" in w)
    ))


def _cupons(t: str) -> frozenset:
    return frozenset(re.findall(r'\b([A-Z0-9_-]{4,20})\b', t))


def _cupons_set(t: str) -> frozenset:
    return _cupons(t)


def _benef(t: str) -> frozenset:
    b = set()
    if re.search(r'frete\s+gr[aá]t', t, re.I):
        b.add("frete_gratis")
    for m in re.findall(r'(\d+)\s*%?\s*off', t, re.I):
        b.add(f"off_{m}")
    for m in re.findall(r'r\$\s*([\d.,]+)\s*off', t, re.I):
        b.add(f"valor_off_{m.replace('.','').replace(',','')}")
    return frozenset(b)


def _benef_set(t: str) -> frozenset:
    return _benef(t)


def _janela(plat: str) -> float:
    # _JANELA_C3 definido em pipeline/normalizacao.py e reexportado
    from pipeline.normalizacao import _JANELA_C3
    return _JANELA_C3.get(plat, _JANELA_C3["default"])


def _sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if min(len(a), len(b)) / max(len(a), len(b)) < 0.6:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _normalizar_valor(t: str) -> str:
    vals = re.findall(r'r\$\s*([\d.,]+)', t, re.I)
    return "|".join(sorted(v.replace('.', '').replace(',', '.') for v in vals))

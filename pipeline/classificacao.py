"""Camada 2 — Classificação central (core fixo e orientado a plugins)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Set
from urllib.parse import urlparse

from globals import _cls_cache, _cls_lock, _CACHE_LIMIT
from logger import log_cls
from utils.urls import _cache_key, _netloc


# ==================== VARIÁVEIS EXPORTADAS (compatibilidade) ====================

_MUNDIAIS = frozenset({
    "store.epicgames.com",
    "epicgames.com",
    "store.steampowered.com",
    "steampowered.com",
    "gaming.amazon.com",
    "twitch.tv",
    "gog.com",
    "humblebundle.com",
    "itch.io",
})

_BLOQUEADOS = frozenset({
    "pelando.com.br",
    "promobit.com.br",
    "cuponomia.com.br",
    "zoom.com.br",
    "buscape.com.br",
    "bondfaro.com.br",
    "ofertasbrasil.com.br",
})

_ENCURTADORES = frozenset({
    "bit.ly",
    "cutt.ly",
    "tinyurl.com",
    "t.co",
    "ow.ly",
    "goo.gl",
    "rb.gy",
    "is.gd",
    "tiny.cc",
    "buff.ly",
    "short.io",
    "bl.ink",
    "rebrand.ly",
    "shorturl.at",
    "tidd.ly",
    "meli.la",
})

_PRESERVE = frozenset({
    "wa.me",
    "api.whatsapp.com",
})

_DELETAR = frozenset({
    "t.me",
    "telegram.me",
    "telegram.org",
    "chat.whatsapp.com",
})


# Compatibilidade externa (mantidos)
_FORCA_GET = frozenset({
    "amzlink.to",
    "amzn.to",
    "a.co",
    "amzn.com",
    "maga.lu",
    "meli.la",
    "bit.ly",
    "cutt.ly",
    "tinyurl.com",
})


# ==================== DATACLASS ====================

@dataclass
class LinkClassificado:
    url_original: str
    plat: Optional[str]
    tipo: Optional[str]
    sku: str = ""
    id_global: str = ""


# ==================== REGISTRY DE PLATAFORMAS ====================

_CLASSIFICADORES = {}


def registrar_classificador(plataforma: str, fn):
    """
    Registra classificador por plataforma.
    Contrato:
      fn(url) -> LinkClassificado | None
    """
    _CLASSIFICADORES[plataforma] = fn


def _bootstrap_classificadores():
    """
    Lazy import:
    evita acoplamento forte e mantém pipeline limpo.
    """
    if _CLASSIFICADORES:
        return

    from plataformas.amazon.classificador import classificar_amazon
    from plataformas.shopee.classificador import classificar_shopee
    from plataformas.magalu.classificador import classificar_magalu
    from plataformas.mercadolivre.classificador import classificar_mercadolivre

    registrar_classificador("amazon", classificar_amazon)
    registrar_classificador("shopee", classificar_shopee)
    registrar_classificador("magalu", classificar_magalu)
    registrar_classificador("mercadolivre", classificar_mercadolivre)


# ==================== CORE FIXO ====================

def _classificacao_core(url: str) -> Optional[LinkClassificado]:
    """
    Core fixo.
    Não conhece regras internas das lojas.
    Apenas delega.
    """
    _bootstrap_classificadores()

    for _, classificador in _CLASSIFICADORES.items():
        resultado = classificador(url)

        if resultado:
            return resultado

    return None


# ==================== CLASSIFICAÇÃO PRINCIPAL ====================

def classificar_url(url: str) -> LinkClassificado:
    if not url or len(url) > 4000 or "://" not in url:
        return LinkClassificado(url, None, "invalido", "")

    p = urlparse(url)
    nl = _netloc(url)

    if not nl:
        return LinkClassificado(url, None, "invalido", "")

    # Mundiais
    if any(nl == d or nl.endswith("." + d) for d in _MUNDIAIS):
        return LinkClassificado(
            url,
            "mundial",
            "mundial",
            ""
        )

    # Bloqueados
    if any(nl == d or nl.endswith("." + d) for d in _BLOQUEADOS):
        return LinkClassificado(
            url,
            None,
            "bloqueado",
            ""
        )

    # Grupo externo
    if any(nl == d or nl.endswith("." + d) for d in _DELETAR):
        return LinkClassificado(
            url,
            None,
            "grupo_externo",
            ""
        )

    # Preservar
    if any(nl == d or nl.endswith("." + d) for d in _PRESERVE):
        return LinkClassificado(
            url,
            "preservar",
            "preservar",
            ""
        )

    # Encurtadores genéricos
    if any(nl == d or nl.endswith("." + d) for d in _ENCURTADORES):
        return LinkClassificado(
            url,
            "expandir",
            "encurtado",
            ""
        )

    # Plugins (cada loja resolve sua vida)
    resultado = _classificacao_core(url)

    if resultado:
        return resultado

    return LinkClassificado(
        url,
        None,
        "desconhecido",
        ""
    )


# ==================== CACHE ====================

def _classificar_cached(url: str) -> LinkClassificado:
    key = _cache_key(url)

    with _cls_lock:
        if key in _cls_cache:
            lc = _cls_cache[key]
            _cls_cache.move_to_end(key)
            return lc

    lc = classificar_url(url)

    with _cls_lock:
        _cls_cache[key] = lc
        _cls_cache.move_to_end(key)

        if len(_cls_cache) > _CACHE_LIMIT:
            _cls_cache.popitem(last=False)

    return lc


# ==================== BATCH ====================

def classificar_links(links: List[str]) -> List[LinkClassificado]:
    vistos: Set[str] = set()
    result: List[LinkClassificado] = []

    for u in links:
        key = _cache_key(u)

        if key in vistos:
            continue

        vistos.add(key)
        result.append(_classificar_cached(u))

    validos = sum(
        1 for r in result
        if r.plat is not None
    )

    log_cls.debug(
        f"🔍 {validos}/{len(links)} classificados"
    )

    return result

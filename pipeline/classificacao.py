"""Camada 2 — Classificação de links por plataforma.
Versão Sênior: alta performance, extensível e robusta.
"""

from __future__ import annotations
import re
from dataclasses import dataclass
from threading import Lock
from typing import List, Optional, Set
from urllib.parse import urlparse

from globals import _cls_cache, _cls_lock, _CACHE_LIMIT
from logger import log_cls
from utils.urls import _cache_key, _netloc


# ==================== CONFIGURAÇÃO DE PLATAFORMAS ====================
class PlatformConfig:
    """Configuração centralizada de plataformas."""

    MUNDIAIS = frozenset({
        "store.epicgames.com", "epicgames.com", "store.steampowered.com",
        "steampowered.com", "gaming.amazon.com", "twitch.tv", "gog.com",
        "humblebundle.com", "itch.io"
    })

    BLOQUEADOS = frozenset({
        "pelando.com.br", "promobit.com.br", "cuponomia.com.br",
        "zoom.com.br", "buscape.com.br", "bondfaro.com.br", "ofertasbrasil.com.br"
    })

    AMAZON = frozenset({
        "amazon.com.br", "amazon.com", "amzn.to", "amzn.com",
        "a.co", "amzlink.to", "amzn.eu"
    })

    SHOPEE = frozenset({
        "shopee.com.br", "s.shopee.com.br", "shopee.com", "shope.ee", "flapremios.com.br"
    })

    MAGALU = frozenset({
        "magazineluiza.com.br", "sacola.magazineluiza.com.br",
        "magazinevoce.com.br", "maga.lu", "divulgador.magalu.com", "m.magazineluiza.com.br"
    })

    MERCADOLIVRE = frozenset({
        "mercadolivre.com.br", "www.mercadolivre.com.br", "produto.mercadolivre.com.br",
        "lista.mercadolivre.com.br", "mercadolivre.com", "mercadolibre.com", "meli.la"
    })

    ENCURTADORES = frozenset({
        "bit.ly", "cutt.ly", "tinyurl.com", "t.co", "ow.ly", "goo.gl",
        "rb.gy", "is.gd", "tiny.cc", "buff.ly", "short.io", "bl.ink",
        "rebrand.ly", "shorturl.at", "tidd.ly"
    })

    PRESERVE = frozenset({"wa.me", "api.whatsapp.com"})
    DELETAR = frozenset({"t.me", "telegram.me", "telegram.org", "chat.whatsapp.com"})


# ==================== PADRÕES DE EXTRAÇÃO ====================
_AMZ_PATHS_SEM_TAG = re.compile(
    r'^/(?:gaming|claims|gp/yourstore|gp/css|gp/help|gp/cart|wishlist|hz/|ap/|gp/registry)', re.I)

_P_SHP = [
    re.compile(r'/product/(\d+)/(\d+)'),
    re.compile(r'/item/(\d+)/(\d+)'),
    re.compile(r'/i\.(\d+)\.(\d+)'),
]

_P_MGL = re.compile(r'/(?:[^/]+/)?p/([a-z0-9]{5,})(?:/|$|[?#])', re.I)

_P_AMZ_ASIN = [
    re.compile(r'/dp/([A-Z0-9]{10})', re.I),
    re.compile(r'/gp/product/([A-Z0-9]{10})', re.I),
    re.compile(r'[?&]asin=([A-Z0-9]{10})', re.I),
]


@dataclass
class LinkClassificado:
    url_original: str
    plat:         Optional[str]
    tipo:         Optional[str]
    sku:          str = ""
    id_global:    str = ""


# ==================== FUNÇÕES AUXILIARES ====================
def _eh_magalu_url(url: str) -> bool:
    """Usado por outros módulos (normalizacao.py)."""
    if not url:
        return False
    nl = _netloc(url)
    return any(nl == d or nl.endswith("." + d) for d in PlatformConfig.MAGALU)


def _eh_mercadolivre_url(url: str) -> bool:
    """Suporte ao Mercado Livre."""
    if not url:
        return False
    nl = _netloc(url)
    return any(nl == d or nl.endswith("." + d) for d in PlatformConfig.MERCADOLIVRE)


def _extrair_asin(p) -> str:
    text = p.path + "?" + p.query
    for pat in _P_AMZ_ASIN:
        m = pat.search(text)
        if m:
            return m.group(1).upper()
    return ""


def _extrair_sku_shopee(p) -> str:
    text = p.path + "?" + p.query
    for pat in _P_SHP:
        m = pat.search(text)
        if m:
            return f"{m.group(1)}.{m.group(2)}"
    return ""


def _extrair_sku_magalu(p) -> str:
    m = _P_MGL.search(p.path)
    return m.group(1) if m else ""


# ==================== CLASSIFICAÇÃO PRINCIPAL ====================
def classificar_url(url: str) -> LinkClassificado:
    if not url or len(url) > 4000 or "://" not in url:
        return LinkClassificado(url, None, "invalido", "")

    p = urlparse(url)
    nl = _netloc(url)
    if not nl:
        return LinkClassificado(url, None, "invalido", "")

    # Mundiais
    if any(nl == d or nl.endswith("." + d) for d in PlatformConfig.MUNDIAIS):
        return LinkClassificado(url, "mundial", "mundial", "")

    # Bloqueados (sem Mercado Livre)
    if any(nl == d or nl.endswith("." + d) for d in PlatformConfig.BLOQUEADOS):
        return LinkClassificado(url, None, "bloqueado", "")

    # Deletar / Preservar
    if any(nl == d or nl.endswith("." + d) for d in PlatformConfig.DELETAR):
        return LinkClassificado(url, None, "grupo_externo", "")
    if any(nl == d or nl.endswith("." + d) for d in PlatformConfig.PRESERVE):
        return LinkClassificado(url, "preservar", "preservar", "")

    # Magalu
    if _eh_magalu_url(url):
        sku = _extrair_sku_magalu(p)
        tipo = "produto" if sku else "lista" if "/l/" in p.path else "selecao" if "/selecao/" in p.path else "campanha"
        return LinkClassificado(url, "magalu", tipo, sku, f"mgl:{sku}" if sku else "")

    # Mercado Livre
    if _eh_mercadolivre_url(url):
        return LinkClassificado(url, "mercadolivre", "produto", "", f"ml:{url[:60]}")

    # Amazon
    if any(nl == d or nl.endswith("." + d) for d in PlatformConfig.AMAZON):
        asin = _extrair_asin(p)
        if _AMZ_PATHS_SEM_TAG.match(p.path):
            return LinkClassificado(url, "amazon", "claims", "")
        tipo = "produto" if asin else "busca" if re.search(r'/s[/?]|/deals|/b[/?]', p.path) else "campanha"
        return LinkClassificado(url, "amazon", tipo, asin, f"amz:{asin}" if asin else "")

    # Shopee
    if any(nl == d or nl.endswith("." + d) for d in PlatformConfig.SHOPEE):
        sku = _extrair_sku_shopee(p)
        return LinkClassificado(url, "shopee", "produto" if sku else "busca", sku, f"shp:{sku}" if sku else "")

    # Encurtadores
    if any(nl == d or nl.endswith("." + d) for d in PlatformConfig.ENCURTADORES):
        return LinkClassificado(url, "expandir", "encurtado", "")

    return LinkClassificado(url, None, "desconhecido", "")


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


def classificar_links(links: List[str]) -> List[LinkClassificado]:
    vistos: Set[str] = set()
    result: List[LinkClassificado] = []

    for u in links:
        key = _cache_key(u)
        if key in vistos:
            continue
        vistos.add(key)
        result.append(_classificar_cached(u))

    validos = sum(1 for r in result if r.plat is not None)
    log_cls.debug(f"🔍 Classificados: {validos}/{len(links)} links")
    return result

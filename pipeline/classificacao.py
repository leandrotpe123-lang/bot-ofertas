"""Camada 2 — Classificação de links por plataforma."""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import List, Optional, Set
from urllib.parse import urlparse

from globals import _cls_cache, _cls_lock, _CACHE_LIMIT
from logger import log_cls
from utils.urls import _cache_key, _netloc

# ── Domínios ────────────────────────────────────────────────
_MUNDIAIS = frozenset({
    "store.epicgames.com","epicgames.com","store.steampowered.com",
    "steampowered.com","gaming.amazon.com","twitch.tv","gog.com",
    "humblebundle.com","itch.io"
})

_BLOQUEADOS = frozenset({
    "pelando.com.br","promobit.com.br","cuponomia.com.br",
    "zoom.com.br","buscape.com.br","bondfaro.com.br","ofertasbrasil.com.br"
})

_AMZ_DOMINIOS = frozenset({
    "amazon.com.br","amazon.com","amzn.to","amzn.com","a.co","amzlink.to","amzn.eu"
})

_SHP_DOMINIOS = frozenset({
    "shopee.com.br","s.shopee.com.br","shopee.com","shope.ee","flapremios.com.br"
})

_MGL_DOMINIOS = frozenset({
    "magazineluiza.com.br","sacola.magazineluiza.com.br",
    "magazinevoce.com.br","maga.lu","divulgador.magalu.com"
})

_MGL_DOMINIOS_SET = frozenset({
    *_MGL_DOMINIOS,
    "m.magazineluiza.com.br"
})

_ML_DOMINIOS = frozenset({
    "mercadolivre.com.br","www.mercadolivre.com.br","produto.mercadolivre.com.br",
    "lista.mercadolivre.com.br","articulo.mercadolivre.com.br","item.mercadolivre.com.br",
    "mercadolivre.com","mercadolibre.com"
})

_ENCURTADORES = frozenset({
    "bit.ly","meli.la","cutt.ly","tinyurl.com","t.co","ow.ly","goo.gl",
    "rb.gy","is.gd","tiny.cc","buff.ly","short.io","bl.ink","rebrand.ly",
    "shorturl.at","tidd.ly"
})

_PRESERVE = frozenset({"wa.me","api.whatsapp.com"})
_DELETAR = frozenset({"t.me","telegram.me","telegram.org","chat.whatsapp.com"})

_FORCA_GET = frozenset({
    "amzlink.to","amzn.to","meli.la","a.co","amzn.com","bit.ly","cutt.ly",
    "tinyurl.com","rb.gy","is.gd","ow.ly","buff.ly","maga.lu","tidd.ly"
})

# ── Regex ────────────────────────────────────────────────
_AMZ_PATHS_SEM_TAG = re.compile(
    r'^/(?:gaming(?:/|$)|claims(?:/|$)|gp/yourstore(?:/|$)|gp/css(?:/|$)|'
    r'gp/help(?:/|$)|gp/cart(?:/|$)|wishlist(?:/|$)|hz/|ap/|gp/registry(?:/|$))',
    re.I
)

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

_P_AMZ_PROMO = re.compile(r'/promotion/psp/([A-Z0-9]{8,16})', re.I)

_P_ML = re.compile(
    r'/(?:p/)?(?:MLB|mlb)[-]?\d{6,12}(?:/|$|[?#])',
    re.I
)

@dataclass
class LinkClassificado:
    url_original: str
    plat: Optional[str]
    tipo: Optional[str]
    sku: str
    id_global: str = ""

# ── EXTRATORES ───────────────────────────────────────────

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


def _extrair_sku_mercadolivre(p) -> str:
    text = p.path + "?" + p.query
    m = _P_ML.search(text)
    if m:
        return m.group(1).upper()
    return ""


def _extrair_sku_magalu(p) -> str:
    m = _P_MGL.search(p.path)
    return m.group(1) if m else ""


# ── CLASSIFICAÇÃO ───────────────────────────────────────

def classificar_url(url: str) -> LinkClassificado:
    if not url or len(url) > 4000 or "://" not in url:
        return LinkClassificado(url, None, "invalido", "")

    p = urlparse(url)
    nl = _netloc(url)

    if not nl:
        return LinkClassificado(url, None, "invalido", "")

    # mundiais
    for d in _MUNDIAIS:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, "mundial", "mundial", "")

    # bloqueados
    for d in _BLOQUEADOS:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, None, "bloqueado", "")

    # deletar
    for d in _DELETAR:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, None, "grupo_externo", "")

    # preserve
    for d in _PRESERVE:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, "preservar", "preservar", "")

    # encurtadores
    for d in _ENCURTADORES:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, "expandir", "encurtado", "")

    # magalu
    for d in _MGL_DOMINIOS:
        if nl == d or nl.endswith("." + d):
            sku = _extrair_sku_magalu(p)

            if "sacola" in nl and not p.path.strip("/"):
                return LinkClassificado(url, "magalu", "invalido", sku)

            tipo = (
                "produto" if sku else
                "lista" if "/l/" in p.path else
                "selecao" if "/selecao/" in p.path else
                "campanha"
            )

            return LinkClassificado(url, "magalu", tipo, sku, f"mgl:{sku}" if sku else "")

    # amazon
    for d in _AMZ_DOMINIOS:
        if nl == d or nl.endswith("." + d):
            asin = _extrair_asin(p)

            if _AMZ_PATHS_SEM_TAG.match(p.path):
                return LinkClassificado(url, "amazon", "claims", "")

            mp = _P_AMZ_PROMO.search(p.path)
            if mp and not asin:
                promo_id = mp.group(1).upper()
                return LinkClassificado(url, "amazon", "promocao",
                                        promo_id, f"amz:promo_{promo_id}")

            tipo = (
                "produto" if asin else
                "busca" if re.search(r'/s[/?]|/deals|/b[/?]', p.path) else
                "evento" if re.search(r'/events/|/stores/', p.path) else
                "campanha"
            )

            return LinkClassificado(url, "amazon", tipo, asin, f"amz:{asin}" if asin else "")

    # shopee
    for d in _SHP_DOMINIOS:
        if nl == d or nl.endswith("." + d):
            if nl == "flapremios.com.br":
                return LinkClassificado(url, "shopee", "campanha", "")

            sku = _extrair_sku_shopee(p)
            return LinkClassificado(url, "shopee",
                                    "produto" if sku else "busca",
                                    sku, f"shp:{sku}" if sku else "")

    # mercado livre
    for d in _ML_DOMINIOS:
        if nl == d or nl.endswith("." + d):
            sku = _extrair_sku_mercadolivre(p)

            if "/p/" in p.path and not sku:
                return LinkClassificado(url, "mercadolivre", "invalido", "")

            tipo = (
                "produto" if sku else
                "busca" if "/busca" in p.path or "/search" in p.path else
                "campanha"
            )

            return LinkClassificado(
                url,
                "mercadolivre",
                tipo,
                sku,
                f"ml:{sku}" if sku else ""
            )

    return LinkClassificado(url, None, "desconhecido", "")


# ─────────────────────────────────────────────
# COMPATIBILIDADE LEGACY TOTAL (PIPELINE ANTIGO)
# ─────────────────────────────────────────────

def _classificar_cached(url: str):
    """Compatibilidade com pipeline antigo."""
    return classificar_url(url)


def classificar_links(links: list):
    """
    Compatibilidade com módulos antigos que ainda esperam batch classification.
    """
    return [_classificar_cached(url) for url in links]

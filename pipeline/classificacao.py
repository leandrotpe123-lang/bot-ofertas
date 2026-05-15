"""Camada 2 — Classificação de links por plataforma."""
from __future__ import annotations
import re
from dataclasses import dataclass, replace
from threading import Lock
from typing import List, Optional, Set
from urllib.parse import urlparse

from globals import _cls_cache, _cls_lock, _CACHE_LIMIT
from logger import log_cls
from utils.urls import _cache_key, _netloc

# ── Domínios por categoria ────────────────────────────────────────
_MUNDIAIS   = frozenset({"store.epicgames.com","epicgames.com","store.steampowered.com",
    "steampowered.com","gaming.amazon.com","twitch.tv","gog.com","humblebundle.com","itch.io"})
# Mercado Livre será integrado como plataforma própria no futuro — não bloqueado aqui.
_BLOQUEADOS = frozenset({"pelando.com.br","promobit.com.br","cuponomia.com.br",
    "zoom.com.br","buscape.com.br","bondfaro.com.br","ofertasbrasil.com.br"})
_AMZ_DOMINIOS = frozenset({"amazon.com.br","amazon.com","amzn.to","amzn.com",
    "a.co","amzlink.to","amzn.eu"})
_SHP_DOMINIOS = frozenset({"shopee.com.br","s.shopee.com.br","shopee.com",
    "shope.ee","flapremios.com.br"})
_MGL_DOMINIOS = frozenset({"magazineluiza.com.br","sacola.magazineluiza.com.br",
    "magazinevoce.com.br","maga.lu","divulgador.magalu.com"})
_MGL_DOMINIOS_SET = frozenset({*_MGL_DOMINIOS, "m.magazineluiza.com.br"})
ML_DOMINIOS = {
    "mercadolivre.com.br",
    "www.mercadolivre.com.br",
    "produto.mercadolivre.com.br",
    "lista.mercadolivre.com.br",
    "meli.la",
}
_ENCURTADORES = frozenset({"bit.ly","cutt.ly","meli.la","tinyurl.com","t.co","ow.ly","goo.gl",
    "rb.gy","is.gd","tiny.cc","buff.ly","short.io","bl.ink","rebrand.ly","shorturl.at",
    "tidd.ly"})
_PRESERVE = frozenset({"wa.me","api.whatsapp.com"})
_DELETAR  = frozenset({"t.me","telegram.me","telegram.org","chat.whatsapp.com"})
_FORCA_GET = frozenset({"amzlink.to","amzn.to","a.co","amzn.com","bit.ly","cutt.ly",
    "tinyurl.com","rb.gy","is.gd","ow.ly","meli.la","buff.ly","maga.lu","tidd.ly"})

# ── Padrões de extração ───────────────────────────────────────────
_AMZ_PATHS_SEM_TAG = re.compile(
    r'^/(?:gaming(?:/|$)|claims(?:/|$)|gp/yourstore(?:/|$)|gp/css(?:/|$)|'
    r'gp/help(?:/|$)|gp/cart(?:/|$)|wishlist(?:/|$)|hz/|ap/|gp/registry(?:/|$))', re.I)
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

# ── Mercado Livre ────────────────────────────────────────────────

_P_ML_SHORT = [
    re.compile(r'^https?://(?:www\.)?meli\.la/', re.I),
    re.compile(r'^https?://(?:www\.)?mercadolivre\.com\.br/sec/', re.I),
]

_P_ML_SOCIAL = re.compile(
    r'/social/([a-zA-Z0-9_\-]+)',
    re.I
)

_P_ML_PRODUCT = [
    re.compile(r'/MLB-?(\d+)', re.I),
    re.compile(r'/p/MLB(\d+)', re.I),
]

_P_ML_CAMPAIGN = [
    re.compile(r'/_Container_([0-9\-]+)', re.I),
    re.compile(r'[?&]coupon_campaign_id=(\d+)', re.I),
]

@dataclass
class LinkClassificado:
    url_original: str
    plat:         Optional[str]
    tipo:         Optional[str]
    sku:          str
    id_global:    str = ""


def _extrair_asin(p) -> str:
    text = p.path + "?" + p.query
    for pat in _P_AMZ_ASIN:
        m = pat.search(text)
        if m: return m.group(1).upper()
    return ""


def _extrair_sku_shopee(p) -> str:
    text = p.path + "?" + p.query
    for pat in _P_SHP:
        m = pat.search(text)
        if m: return f"{m.group(1)}.{m.group(2)}"
    return ""


def _extrair_sku_magalu(p) -> str:
    m = _P_MGL.search(p.path)
    return m.group(1) if m else ""


def _eh_magalu_url(url: str) -> bool:
    nl = _netloc(url)
    return any(nl == d or nl.endswith("." + d) for d in _MGL_DOMINIOS_SET)

def _extrair_sku_mercadolivre(url: str) -> str | None:
    """
    Extrai identificador principal do Mercado Livre.

    Retorna:
        MLB1234567890   -> produto
        CONTAINER_xxx   -> campanha/lista
        social_username -> social
        None            -> não identificado
    """

    if not url:
        return None

    u = url.lower()

    # ── Produto ───────────────────────────────────────────────
    for p in _P_ML_PRODUCT:
        m = p.search(u)
        if m:
            return f"MLB{m.group(1)}"

    # ── Campanha / Lista ─────────────────────────────────────
    for p in _P_ML_CAMPAIGN:
        m = p.search(u)
        if m:
            return f"CONTAINER_{m.group(1)}"

    # ── Social ───────────────────────────────────────────────
    m = _P_ML_SOCIAL.search(u)
    if m:
        return f"social_{m.group(1)}"

    # ── Short links ──────────────────────────────────────────
    if any(p.search(u) for p in _P_ML_SHORT):
        return "SHORT_LINK"

    return None


def classificar_url(url: str) -> LinkClassificado:
    if not url or len(url) > 4000 or "://" not in url:
        return LinkClassificado(url, None, "invalido", "")
    p = urlparse(url); nl = _netloc(url)
    if not nl: return LinkClassificado(url, None, "invalido", "")
    for d in _MUNDIAIS:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, "mundial", "mundial", "")
    for d in _BLOQUEADOS:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, None, "bloqueado", "")
    for d in _DELETAR:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, None, "grupo_externo", "")
    for d in _PRESERVE:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, "preservar", "preservar", "")
    for d in _MGL_DOMINIOS:
        if nl == d or nl.endswith("." + d):
            sku = _extrair_sku_magalu(p)
            if "sacola" in nl and not p.path.strip("/"):
                return LinkClassificado(url, "magalu", "invalido", sku)
            tipo = ("produto" if sku else "lista" if "/l/" in p.path else
                    "selecao" if "/selecao/" in p.path else "campanha")
            return LinkClassificado(url, "magalu", tipo, sku, f"mgl:{sku}" if sku else "")
    for d in _AMZ_DOMINIOS:
        if nl == d or nl.endswith("." + d):
            asin = _extrair_asin(p)
            if _AMZ_PATHS_SEM_TAG.match(p.path):
                return LinkClassificado(url, "amazon", "claims", "")
            # Detecta promotion ID Amazon como identidade estável
            mp = _P_AMZ_PROMO.search(p.path)
            if mp and not asin:
                promo_id = mp.group(1).upper()
                return LinkClassificado(url, "amazon", "promocao",
                                        promo_id, f"amz:promo_{promo_id}")
            tipo = ("produto" if asin else
                    "busca" if re.search(r'/s[/?]|/deals|/b[/?]', p.path) else
                    "evento" if re.search(r'/events/|/stores/', p.path) else "campanha")
            return LinkClassificado(url, "amazon", tipo, asin, f"amz:{asin}" if asin else "")
    for d in _SHP_DOMINIOS:
        if nl == d or nl.endswith("." + d):
            if nl == "flapremios.com.br":
                return LinkClassificado(url, "shopee", "campanha", "")
            sku = _extrair_sku_shopee(p)
            return LinkClassificado(url, "shopee", "produto" if sku else "busca",
                                    sku, f"shp:{sku}" if sku else "")
    for d in _ENCURTADORES:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, "expandir", "encurtado", "")
    return LinkClassificado(url, None, "desconhecido", "")


def _classificar_cached(url: str) -> LinkClassificado:
    key = _cache_key(url)
    with _cls_lock:
        lc = _cls_cache.get(key)
        if lc is not None:
            _cls_cache.move_to_end(key)
            # url_original reflete a URL DESTA chamada (não a primeira cacheada)
            return lc if lc.url_original == url else replace(lc, url_original=url)
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
        if key in vistos: continue
        vistos.add(key)
        result.append(_classificar_cached(u))
    validos = [r for r in result if r.plat is not None]
    log_cls.debug(f"🔍 {len(validos)}/{len(links)} classificados")
    return result
            

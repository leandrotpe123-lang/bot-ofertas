"""Camada 2 — Classificação de links por plataforma."""
from __future__ import annotations
import re
from dataclasses import dataclass
from threading import Lock
from typing import List, Optional, Set
from urllib.parse import urlparse

from globals import _cls_cache, _cls_lock, _CACHE_LIMIT
from logger import log_cls
from utils.urls import _cache_key, _netloc

# ── Domínios por categoria ────────────────────────────────────────
_MUNDIAIS   = frozenset({"store.epicgames.com","epicgames.com","store.steampowered.com",
    "steampowered.com","gaming.amazon.com","twitch.tv","gog.com","humblebundle.com","itch.io"})
_BLOQUEADOS = frozenset({"mercadolivre.com.br","mercadopago.com.br","mercadolivre.com",
    "meli.com","ml.com.br","pelando.com.br","promobit.com.br","cuponomia.com.br",
    "zoom.com.br","buscape.com.br","bondfaro.com.br","ofertasbrasil.com.br"})
_AMZ_DOMINIOS = frozenset({"amazon.com.br","amazon.com","amzn.to","amzn.com",
    "a.co","amzlink.to","amzn.eu"})
_SHP_DOMINIOS = frozenset({"shopee.com.br","s.shopee.com.br","shopee.com",
    "shope.ee","flapremios.com.br"})
_MGL_DOMINIOS = frozenset({"magazineluiza.com.br","sacola.magazineluiza.com.br",
    "magazinevoce.com.br","maga.lu","divulgador.magalu.com"})
_MGL_DOMINIOS_SET = frozenset({*_MGL_DOMINIOS, "m.magazineluiza.com.br"})
_ENCURTADORES = frozenset({"bit.ly","cutt.ly","tinyurl.com","t.co","ow.ly","goo.gl",
    "rb.gy","is.gd","tiny.cc","buff.ly","short.io","bl.ink","rebrand.ly","shorturl.at",
    "tidd.ly"})
_PRESERVE = frozenset({"wa.me","api.whatsapp.com"})
_DELETAR  = frozenset({"t.me","telegram.me","telegram.org","chat.whatsapp.com"})
_FORCA_GET = frozenset({"amzlink.to","amzn.to","a.co","amzn.com","bit.ly","cutt.ly",
    "tinyurl.com","rb.gy","is.gd","ow.ly","buff.ly","maga.lu","tidd.ly"})

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
        if key in vistos: continue
        vistos.add(key)
        result.append(_classificar_cached(u))
    validos = [r for r in result if r.plat is not None]
    log_cls.debug(f"🔍 {len(validos)}/{len(links)} classificados")
    return result

"""Camada 2 — Classificação de links por plataforma."""
from __future__ import annotations
import re
from dataclasses import dataclass, replace
from typing import Dict, FrozenSet, List, Optional, Set
from urllib.parse import urlparse

from globals import _cls_cache, _cls_lock, _CACHE_LIMIT
from logger import log_cls
from utils.urls import _cache_key, _netloc


# ── Domínios genéricos da pipeline ───────────────────────────────
_MUNDIAIS = frozenset({
    "store.epicgames.com", "epicgames.com",
    "store.steampowered.com", "steampowered.com",
    "gaming.amazon.com", "twitch.tv",
    "gog.com", "humblebundle.com", "itch.io",
})
_BLOQUEADOS = frozenset({
    "pelando.com.br", "promobit.com.br", "cuponomia.com.br",
    "zoom.com.br", "buscape.com.br", "bondfaro.com.br",
    "ofertasbrasil.com.br",
})
_PRESERVE = frozenset({"wa.me", "api.whatsapp.com"})
_DELETAR = frozenset({
    "t.me", "telegram.me", "telegram.org", "chat.whatsapp.com",
})
_ENCURTADORES = frozenset({
    "bit.ly", "cutt.ly", "tinyurl.com", "t.co", "ow.ly", "goo.gl",
    "rb.gy", "is.gd", "tiny.cc", "buff.ly", "short.io", "bl.ink",
    "rebrand.ly", "shorturl.at", "tidd.ly",
})
_FORCA_GET = frozenset({
    "amzlink.to", "amzn.to", "a.co", "amzn.com",
    "bit.ly", "cutt.ly", "tinyurl.com",
    "rb.gy", "is.gd", "ow.ly", "buff.ly",
    "maga.lu", "tidd.ly",
})


# ── Domínios e encurtadores por plataforma ───────────────────────
_AMZ_DOMINIOS = frozenset({
    "amazon.com.br", "amazon.com",
})
_ENCURTADORES_AMAZON = frozenset({
    "amzn.to", "a.co", "amzn.com", "amzlink.to",
})

_SHP_DOMINIOS = frozenset({
    "shopee.com.br", "s.shopee.com.br", "shopee.com",
    "shope.ee", "flapremios.com.br",
})
_ENCURTADORES_SHOPEE = frozenset({
    "s.shopee.com.br", "shope.ee", "s.shopee.com",
})

_MGL_DOMINIOS = frozenset({
    "magazineluiza.com.br", "sacola.magazineluiza.com.br",
    "magazinevoce.com.br", "maga.lu", "divulgador.magalu.com",
})
_MGL_DOMINIOS_SET = frozenset({*_MGL_DOMINIOS, "m.magazineluiza.com.br"})
_ENCURTADORES_MAGALU = frozenset({
    "maga.lu", "divulgador.magalu.com",
})

_ENCURTADORES_POR_PLAT: Dict[str, FrozenSet[str]] = {
    "amazon": _ENCURTADORES_AMAZON,
    "shopee": _ENCURTADORES_SHOPEE,
    "magalu": _ENCURTADORES_MAGALU,
}


# ── Padrões de extração ──────────────────────────────────────────
_AMZ_PATHS_SEM_TAG = re.compile(
    r'^/(?:gaming(?:/|$)|claims(?:/|$)|gp/yourstore(?:/|$)|gp/css(?:/|$)|'
    r'gp/help(?:/|$)|gp/cart(?:/|$)|wishlist(?:/|$)|hz/|ap/|gp/registry(?:/|$))',
    re.I,
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


@dataclass
class LinkClassificado:
    url_original:  str
    plat:          Optional[str]
    tipo:          Optional[str]
    sku:           str
    id_global:     str  = ""
    eh_encurtador: bool = False


def _bate_dominio(nl: str, dominios: FrozenSet[str]) -> bool:
    for d in dominios:
        if nl == d or nl.endswith("." + d):
            return True
    return False


def _eh_encurtador_plat(nl: str, plat: str) -> bool:
    return nl in _ENCURTADORES_POR_PLAT.get(plat, frozenset())


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


def _eh_magalu_url(url: str) -> bool:
    return _bate_dominio(_netloc(url), _MGL_DOMINIOS_SET)


def classificar_url(url: str) -> LinkClassificado:
    if not url or len(url) > 4000 or "://" not in url:
        return LinkClassificado(url, None, "invalido", "")
    p = urlparse(url)
    nl = _netloc(url)
    if not nl:
        return LinkClassificado(url, None, "invalido", "")

    if _bate_dominio(nl, _MUNDIAIS):
        return LinkClassificado(url, "mundial", "mundial", "")
    if _bate_dominio(nl, _BLOQUEADOS):
        return LinkClassificado(url, None, "bloqueado", "")
    if _bate_dominio(nl, _DELETAR):
        return LinkClassificado(url, None, "grupo_externo", "")
    if _bate_dominio(nl, _PRESERVE):
        return LinkClassificado(url, "preservar", "preservar", "")

    if _bate_dominio(nl, _MGL_DOMINIOS):
        if nl in _ENCURTADORES_MAGALU:
            return LinkClassificado(
                url, "magalu", "encurtado", "", "", True,
            )
        sku = _extrair_sku_magalu(p)
        if "sacola" in nl and not p.path.strip("/"):
            return LinkClassificado(url, "magalu", "invalido", sku)
        tipo = (
            "produto" if sku
            else "lista"   if "/l/" in p.path
            else "selecao" if "/selecao/" in p.path
            else "campanha"
        )
        return LinkClassificado(
            url, "magalu", tipo, sku,
            f"mgl:{sku}" if sku else "",
        )

    if _bate_dominio(nl, _AMZ_DOMINIOS) or nl in _ENCURTADORES_AMAZON:
        if nl in _ENCURTADORES_AMAZON:
            return LinkClassificado(
                url, "amazon", "encurtado", "", "", True,
            )
        asin = _extrair_asin(p)
        if _AMZ_PATHS_SEM_TAG.match(p.path):
            return LinkClassificado(url, "amazon", "claims", "")
        mp = _P_AMZ_PROMO.search(p.path)
        if mp and not asin:
            promo_id = mp.group(1).upper()
            return LinkClassificado(
                url, "amazon", "promocao", promo_id,
                f"amz:promo_{promo_id}",
            )
        tipo = (
            "produto" if asin
            else "busca"  if re.search(r'/s[/?]|/deals|/b[/?]', p.path)
            else "evento" if re.search(r'/events/|/stores/', p.path)
            else "campanha"
        )
        return LinkClassificado(
            url, "amazon", tipo, asin,
            f"amz:{asin}" if asin else "",
        )

    if _bate_dominio(nl, _SHP_DOMINIOS):
        if nl == "flapremios.com.br":
            return LinkClassificado(url, "shopee", "campanha", "")
        if nl in _ENCURTADORES_SHOPEE:
            return LinkClassificado(
                url, "shopee", "encurtado", "", "", True,
            )
        sku = _extrair_sku_shopee(p)
        return LinkClassificado(
            url, "shopee", "produto" if sku else "busca", sku,
            f"shp:{sku}" if sku else "",
        )

    if _bate_dominio(nl, _ENCURTADORES):
        return LinkClassificado(
            url, "expandir", "encurtado", "", "", True,
        )

    return LinkClassificado(url, None, "desconhecido", "")


def _classificar_cached(url: str) -> LinkClassificado:
    key = _cache_key(url)
    with _cls_lock:
        lc = _cls_cache.get(key)
        if lc is not None:
            _cls_cache.move_to_end(key)
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
        if key in vistos:
            continue
        vistos.add(key)
        result.append(_classificar_cached(u))
    validos = [r for r in result if r.plat is not None]
    log_cls.debug(f"🔍 {len(validos)}/{len(links)} classificados")
    return result


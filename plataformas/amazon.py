"""Afiliação Amazon: limpeza, tag e validação."""
from __future__ import annotations
from typing import Optional
from urllib.parse import urlparse, urlunparse

import aiohttp

from config import _AMZ_TAG, _SEM_HTTP
from database import db_get_link, db_set_link
from globals import _get_final, _set_final
from logger import log_nrm
from pipeline.classificacao import _AMZ_PATHS_SEM_TAG, _classificar_cached
from pipeline.normalizacao import desencurtar
from plataformas.cleaners import _limpar_params_amazon
from utils.urls import _netloc, _sanitizar_url


def _limpar_url_amazon(url: str) -> Optional[str]:
    try:
        from urllib.parse import urlencode
        p    = urlparse(url)
        from pipeline.classificacao import _extrair_asin
        asin = _extrair_asin(p)
        if _AMZ_PATHS_SEM_TAG.match(p.path):
            return urlunparse(p._replace(query="", fragment=""))
        if asin:
            return urlunparse(p._replace(
                path=f"/dp/{asin}", query=f"tag={_AMZ_TAG}", fragment=""))
        if "/promotion/" in p.path:
            return urlunparse(p._replace(query=f"tag={_AMZ_TAG}", fragment=""))
        params = _limpar_params_amazon(p)
        params["tag"] = _AMZ_TAG
        return urlunparse(p._replace(scheme="https", netloc=p.netloc,
                                     query=urlencode(params), fragment=""))
    except Exception:
        return None


async def _afiliar_amazon(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    url = _sanitizar_url(url)
    log_nrm.debug(f"▶ AMZ: {url[:80]}")
    cached = _get_final(url)
    if cached: return cached
    cached = db_get_link(url)
    if cached: return cached
    lc_pre = _classificar_cached(url)
    if lc_pre.tipo == "claims": return url
    if lc_pre.plat == "mundial": return url
    nl = _netloc(url)
    if nl in {"amzn.to", "a.co", "amzn.com", "amzlink.to"}:
        try:
            async with _SEM_HTTP:
                url = await desencurtar(url, sessao)
        except Exception:
            return None
    lc_exp = _classificar_cached(url)
    if lc_exp.tipo == "claims" or lc_exp.plat == "mundial": return url
    final = _limpar_url_amazon(url)
    if not final:
        p = urlparse(url)
        final = (urlunparse(p._replace(query="", fragment=""))
                 if _AMZ_PATHS_SEM_TAG.match(p.path)
                 else f"{url.split('?',1)[0]}?tag={_AMZ_TAG}")
    if not final or "amazon" not in _netloc(final):
        log_nrm.warning(f"  ⚠️ AMZ validação falhou: {final}"); return None
    _set_final(url, final); db_set_link(url, final, "amazon")
    log_nrm.info(f"  ✅ AMZ: {final[:70]}")
    return final
          

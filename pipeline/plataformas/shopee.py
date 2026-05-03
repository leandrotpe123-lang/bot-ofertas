"""Afiliação Shopee: expansão, API com retry e fallback."""
from __future__ import annotations
import hashlib
import json
import time
from typing import Optional

import aiohttp

from config import _SHP_APP_ID, _SHP_SECRET, _SEM_HTTP
from database import db_get_link, db_set_link
from globals import _get_final, _set_final
from logger import log_nrm
from pipeline.classificacao import _ENCURTADORES, _FORCA_GET, _P_SHP
from pipeline.normalizacao import desencurtar
from plataformas.cleaners import _limpar_params_shopee
from utils.urls import _netloc, _sanitizar_url

_SHP_REPASSE_DIRETO = frozenset({"flapremios.com.br"})


async def _expandir_shopee(url: str, sessao: aiohttp.ClientSession) -> str:
    nl = _netloc(url)
    precisa = (nl in {"s.shopee.com.br", "shope.ee", "s.shopee.com"}
               or nl in _ENCURTADORES or nl in _FORCA_GET)
    if precisa:
        try:
            async with _SEM_HTTP:
                expandida = await desencurtar(url, sessao)
                log_nrm.debug(f"  SHP expandida: {expandida[:80]}")
                return expandida
        except Exception as e:
            log_nrm.warning(f"  ⚠️ SHP expandir falhou: {e}")
    return url


def _extrair_url_produto_shopee(url: str) -> Optional[str]:
    from urllib.parse import urlparse
    try:
        p = urlparse(url)
        for pat in _P_SHP:
            m = pat.search(p.path + "?" + p.query)
            if m: return f"https://shopee.com.br/product/{m.group(1)}/{m.group(2)}"
    except Exception:
        pass
    return None


async def _chamar_api_shopee(url_produto: str,
                              sessao: aiohttp.ClientSession) -> Optional[str]:
    for tentativa in range(1, 4):
        try:
            ts      = str(int(time.time()))
            payload = json.dumps({"query": (
                f'mutation {{ generateShortLink(input: '
                f'{{ originUrl: "{url_produto}" }}) {{ shortLink }} }}'
            )}, separators=(",", ":"))
            sig = hashlib.sha256(
                f"{_SHP_APP_ID}{ts}{payload}{_SHP_SECRET}".encode()
            ).hexdigest()
            hdrs = {
                "Authorization": (f"SHA256 Credential={_SHP_APP_ID},"
                                  f"Timestamp={ts},Signature={sig}"),
                "Content-Type": "application/json",
            }
            async with _SEM_HTTP:
                async with sessao.post(
                    "https://open-api.affiliate.shopee.com.br/graphql",
                    data=payload, headers=hdrs,
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as r:
                    res  = await r.json()
                    link = (res.get("data", {})
                               .get("generateShortLink", {})
                               .get("shortLink"))
                    if link:
                        log_nrm.info(f"  ✅ SHP t={tentativa}: {link}")
                        return link
                    log_nrm.warning(
                        f"  ⚠️ SHP API t={tentativa}: "
                        f"{res.get('errors') or res.get('error')}")
        except Exception as e:
            log_nrm.warning(f"  ⚠️ SHP t={tentativa}: {e}")
        if tentativa < 3:
            import asyncio
            await asyncio.sleep(tentativa * 1.5)
    return None


async def _afiliar_shopee(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    url = _sanitizar_url(url)
    log_nrm.debug(f"▶ SHP: {url[:80]}")
    nl = _netloc(url)
    for d in _SHP_REPASSE_DIRETO:
        if nl == d or nl.endswith("." + d):
            log_nrm.info(f"  ↩️ SHP repasse direto: {url[:60]}"); return url
    cached = _get_final(url)
    if cached: return cached
    cached = db_get_link(url)
    if cached: return cached
    url_expandida = await _expandir_shopee(url, sessao)
    url_limpa     = _limpar_params_shopee(url_expandida)
    link = await _chamar_api_shopee(url_limpa, sessao)
    if not link:
        url_prod = _extrair_url_produto_shopee(url_expandida)
        if url_prod and url_prod != url_limpa:
            log_nrm.info(f"  🔄 SHP fallback produto: {url_prod[:60]}")
            link = await _chamar_api_shopee(url_prod, sessao)
    if not link:
        log_nrm.warning(f"  ❌ SHP sem afiliação → descarta: {url[:60]}")
        return None
    if "shopee" not in _netloc(link):
        log_nrm.warning(f"  ⚠️ SHP validação falhou: {link}"); return None
    _set_final(url, link); db_set_link(url, link, "shopee")
    return link
          

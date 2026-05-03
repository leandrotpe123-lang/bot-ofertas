"""Afiliação Magalu: parâmetros, encurtador próprio e Cuttly."""
from __future__ import annotations
import asyncio
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp

from config import (
    _MGL_PARTNER, _MGL_PROMOTER, _MGL_PID,
    _CUTTLY_KEY, _SHORT_BASE, _SEM_HTTP,
)
from database import db_get_link, db_set_link, _db
from globals import _get_final, _set_final
from logger import log_nrm
from pipeline.classificacao import _ENCURTADORES, _classificar_cached, _eh_magalu_url
from pipeline.normalizacao import desencurtar
from plataformas.cleaners import _limpar_params_magalu
from utils.hashes import _gerar_code_magalu
from utils.urls import _netloc, _sanitizar_url
import time


def _afiliar_url_magalu(url: str) -> str:
    p = urlparse(url)
    params = {k: v[0] for k, v in parse_qs(p.query, keep_blank_values=True).items()}
    params = _limpar_params_magalu(params)
    params.update({
        "partner_id":        _MGL_PARTNER,
        "promoter_id":       _MGL_PROMOTER,
        "utm_source":        "divulgador",
        "utm_medium":        "magalu",
        "utm_campaign":      _MGL_PROMOTER,
        "pid":               _MGL_PID,
        "c":                 _MGL_PROMOTER,
        "af_force_deeplink": "true",
    })
    return urlunparse(p._replace(query=urlencode(params), fragment=""))


async def _encurtador_proprio_magalu(url_afiliada: str) -> Optional[str]:
    if _MGL_PARTNER not in url_afiliada:
        log_nrm.warning("  ⚠️ Encurtador próprio: partner_id ausente"); return None
    if _MGL_PROMOTER not in url_afiliada:
        log_nrm.warning("  ⚠️ Encurtador próprio: promoter_id ausente"); return None
    if "magalu" not in url_afiliada and "magazineluiza" not in url_afiliada:
        log_nrm.warning("  ⚠️ Encurtador próprio: não é URL Magalu"); return None
    code  = _gerar_code_magalu(url_afiliada)
    short = f"{_SHORT_BASE}/{code}-magalu"
    try:
        with _db() as db:
            db.execute(
                "INSERT OR IGNORE INTO short_links(code,url,ts) VALUES(?,?,?)",
                (code, url_afiliada, time.time()))
        log_nrm.info(f"  ✅ Encurtador próprio: {short}")
        return short
    except Exception as e:
        log_nrm.error(f"  ❌ Encurtador próprio: {e}"); return None


async def _cuttly(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    if not _CUTTLY_KEY: return None
    try:
        async with sessao.get(
            "https://cutt.ly/api/api.php",
            params={"key": _CUTTLY_KEY, "short": url},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            data   = await r.json()
            status = data.get("url", {}).get("status")
            if status in (1, 2, 3, 4, 5, 6, 7):
                return data["url"].get("shortLink")
    except Exception as e:
        log_nrm.warning(f"⚠️ Cuttly: {e}")
    return None


async def _cuttly_background(url: str, msg_id: int):
    from globals import _get_session
    from pipeline.publicacao import editar
    try:
        sessao = await _get_session()
        short  = await _cuttly(url, sessao)
        if short: await editar(msg_id, short)
    except Exception as e:
        log_nrm.warning(f"⚠️ cuttly_background: {e}")


async def _afiliar_magalu(url: str, sessao: aiohttp.ClientSession,
                           msg_id: int = 0) -> Optional[str]:
    url = _sanitizar_url(url)
    log_nrm.debug(f"▶ MGL entrada: {url[:80]}")
    cached = _get_final(url)
    if cached: return cached
    cached = db_get_link(url)
    if cached: return cached
    nl = _netloc(url)
    if nl == "cutt.ly" or nl == "maga.lu" or nl in _ENCURTADORES:
        try:
            async with _SEM_HTTP:
                url_exp = await desencurtar(url, sessao)
            if not _eh_magalu_url(url_exp):
                log_nrm.warning(
                    f"  MGL pós-expand não é Magalu: {_netloc(url_exp)} — descarta")
                return None
            url = url_exp
        except Exception as e:
            log_nrm.error(f"  ❌ MGL desencurtar: {e}"); return None
    cl = _classificar_cached(url)
    if cl.plat != "magalu" or cl.tipo == "invalido":
        log_nrm.warning(f"  MGL descartado: plat={cl.plat} tipo={cl.tipo}")
        return None
    afiliado = _afiliar_url_magalu(url)
    if not afiliado or ("magalu" not in afiliado and "magazineluiza" not in afiliado):
        log_nrm.warning("  ⚠️ MGL afiliação inválida"); return None
    short = await _encurtador_proprio_magalu(afiliado)
    if short:
        _set_final(url, short); db_set_link(url, short, "magalu")
        log_nrm.info(f"  ✅ MGL próprio: {short}"); return short
    short = await _cuttly(afiliado, sessao)
    if short:
        _set_final(url, short); db_set_link(url, short, "magalu")
        log_nrm.info(f"  ✅ MGL Cuttly: {short}"); return short
    log_nrm.warning("  ⚠️ MGL encurtadores falharam → longo afiliado")
    _set_final(url, afiliado); db_set_link(url, afiliado, "magalu")
    if msg_id:
        asyncio.create_task(_cuttly_background(afiliado, msg_id))
    return afiliado


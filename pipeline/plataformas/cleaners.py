"""Limpeza de parâmetros de URL por plataforma."""
from __future__ import annotations
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

_AMZ_MANTER = frozenset({"keywords", "node", "k", "i", "rh", "n", "field-keywords"})

def _limpar_params_amazon(p) -> dict:
    return {k: v[0] for k, v in parse_qs(p.query).items()
            if k.lower() in _AMZ_MANTER and len(v[0]) < 60}

def _limpar_params_shopee(url: str) -> str:
    try:
        p = urlparse(url)
        params = {k: v[0] for k, v in parse_qs(p.query).items()
                  if k in {"shopid", "itemid", "smtt"}}
        return urlunparse(p._replace(
            query=urlencode(params) if params else "", fragment=""))
    except Exception:
        return url

def _limpar_params_magalu(params: dict) -> dict:
    remover = {
        "partnerid", "promoterid", "afforcedeeplink", "deeplinkvalue",
        "partner_id", "promoter_id", "utm_source", "utm_medium",
        "utm_campaign", "pid", "c", "af_force_deeplink",
        "deep_link_value", "isretargeting",
    }
    return {k: v for k, v in params.items() if k not in remover}
  

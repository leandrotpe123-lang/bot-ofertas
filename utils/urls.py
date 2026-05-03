"""Utilitários de URL: normalização, cache key, sanitização."""
from __future__ import annotations
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


def _cache_key(url: str) -> str:
    """Chave canônica de URL — remove UTMs e parâmetros de rastreamento."""
    try:
        p = urlparse(url.strip())
        host = (p.hostname or "").lower().strip(".")
        params = parse_qs(p.query)
        remover = {
            "ascsubtag", "smid", "utm_source", "utm_medium", "utm_campaign",
            "utm_term", "utm_content", "aff_id", "affiliate_id",
            "fbclid", "gclid", "camp", "creative", "linkcode", "linkid",
        }
        params_limpos = {k: v for k, v in params.items() if k.lower() not in remover}
        pares = [(k, val) for k, vals in params_limpos.items() for val in vals]
        query = urlencode(sorted(pares))
        return urlunparse((p.scheme.lower(), host, p.path.rstrip("/"), "", query, ""))
    except Exception:
        return url.strip().lower()


def _sanitizar_url(url: str) -> str:
    """Remove caracteres de lixo no final da URL."""
    return url.strip().rstrip('.,;)>!?\n\r ')


def _netloc(url: str) -> str:
    """Extrai o netloc limpo (sem www.) de uma URL."""
    try:
        p = urlparse(url)
        nl = (p.hostname or "").lower()
        if nl.startswith("www."):
            nl = nl[4:]
        return nl.strip(".")
    except Exception:
        return ""
      

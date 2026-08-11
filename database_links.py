"""Camada 7 — Banco / Persistência de URLs.

Responsabilidade ÚNICA: as duas tabelas de URL — `links_cache` (a
afiliação já resolvida de uma URL original) e `short_links` (o
encurtador próprio).

NÃO conhece post, oferta nem cupom. Não importa os outros módulos de
domínio, nem é importado por eles.

Usa _db de database_conexao — contrato interno da camada; ver o
cabeçalho daquele módulo.

Extraído de database sem qualquer alteração de comportamento.
"""
from __future__ import annotations
import time
from typing import Optional

from logger import log_db

# Contrato INTERNO da camada — ver cabeçalho de database_conexao.
from database_conexao import _db

# ── links_cache ───────────────────────────────────────────────────
def db_get_link(url: str) -> Optional[tuple[str, str]]:
    try:
        from utils.urls import _cache_key
        url = _cache_key(url)
        with _db() as db:
            row = db.execute(
                "SELECT url_conv, url_canon FROM links_cache WHERE url_orig=?",
                (url,)
            ).fetchone()
            if row:
                db.execute(
                    "UPDATE links_cache SET ts=? WHERE url_orig=?",
                    (time.time(), url))
                publicada = row[0]
                canonica  = row[1] or row[0]   
                return (publicada, canonica)
    except Exception as e:
        log_db.error(f"❌ db_get_link: {e}")
    return None

def db_set_link(url_orig: str, url_conv: str, url_canon: str, plat: str):
    try:
        from utils.urls import _cache_key
        url_orig = _cache_key(url_orig)
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO links_cache(url_orig,url_conv,url_canon,plat,ts)"
                " VALUES(?,?,?,?,?)",
                (url_orig, url_conv, url_canon, plat, time.time()))
    except Exception as e:
        log_db.error(f"❌ db_set_link: {e}")

# ── short_links ───────────────────────────────────────────────────
def db_get_short(code: str) -> Optional[str]:
    try:
        with _db() as db:
            row = db.execute(
                "SELECT url FROM short_links WHERE code=?", (code,)
            ).fetchone()
        return row[0] if row else None
    except Exception as e:
        log_db.error(f"❌ db_get_short: {e}")
        return None

def db_set_short(code: str, url: str):
    try:
        with _db() as db:
            db.execute(
                "INSERT OR IGNORE INTO short_links(code,url,ts) VALUES(?,?,?)",
                (code, url, time.time()))
    except Exception as e:
        log_db.error(f"❌ db_set_short: {e}")


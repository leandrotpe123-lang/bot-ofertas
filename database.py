"""Camada 7 — Banco de dados SQLite. Toda persistência passa por aqui."""
from __future__ import annotations
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from threading import Lock
from typing import Optional

from config import _DB_PATH, TTL_DEDUPE, TTL_LINK_INATIVO, TTL_SCHEDULER
from logger import log_db

_db_conn: Optional[sqlite3.Connection] = None
_db_lock  = Lock()

def _init_db():
    global _db_conn
    _db_conn = sqlite3.connect(
        _DB_PATH, check_same_thread=False, timeout=10, isolation_level=None)
    for p in [
        "PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL",
        "PRAGMA cache_size=-16000", "PRAGMA temp_store=MEMORY",
        "PRAGMA busy_timeout=10000",
    ]:
        _db_conn.execute(p)
    _db_conn.executescript("""
        CREATE TABLE IF NOT EXISTS links_cache(
            url_orig TEXT PRIMARY KEY, url_conv TEXT NOT NULL,
            plat TEXT NOT NULL, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dedupe_temp(
            fp TEXT PRIMARY KEY, plat TEXT NOT NULL, cupons TEXT,
            alma TEXT, camp TEXT, asin TEXT, id_prod TEXT, benef TEXT,
            ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS saturacao(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plat TEXT NOT NULL, sku TEXT, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS short_links(
            code TEXT PRIMARY KEY, url TEXT NOT NULL, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS oferta_estado(
            identity TEXT PRIMARY KEY, msg_id_dest INTEGER NOT NULL,
            score INTEGER NOT NULL DEFAULT 0, texto TEXT NOT NULL DEFAULT '',
            plat TEXT NOT NULL DEFAULT '', lider TEXT DEFAULT '',
            janela_fim REAL DEFAULT 0, edit_count INTEGER DEFAULT 0,
            shadow_reply_id INTEGER DEFAULT 0, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS shadow_reply(
            identity TEXT PRIMARY KEY, msg_id INTEGER NOT NULL,
            enviado INTEGER DEFAULT 0, ts REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_lc_plat     ON links_cache(plat);
        CREATE INDEX IF NOT EXISTS idx_lc_ts       ON links_cache(ts);
        CREATE INDEX IF NOT EXISTS idx_dt_plat     ON dedupe_temp(plat,ts);
        CREATE INDEX IF NOT EXISTS idx_dt_asin     ON dedupe_temp(asin);
        CREATE INDEX IF NOT EXISTS idx_dt_id       ON dedupe_temp(id_prod);
        CREATE INDEX IF NOT EXISTS idx_sat         ON saturacao(plat,ts);
        CREATE INDEX IF NOT EXISTS idx_sl_code     ON short_links(code);
        CREATE INDEX IF NOT EXISTS idx_oe_identity ON oferta_estado(identity);
        CREATE INDEX IF NOT EXISTS idx_oe_plat     ON oferta_estado(plat);
    """)
    for tabela, col, tipo in [
        ("dedupe_temp",   "benef",            "TEXT"),
        ("dedupe_temp",   "asin",             "TEXT"),
        ("dedupe_temp",   "id_prod",          "TEXT"),
        ("oferta_estado", "lider",            "TEXT"),
        ("oferta_estado", "janela_fim",       "REAL"),
        ("oferta_estado", "edit_count",       "INTEGER"),
        ("oferta_estado", "shadow_reply_id",  "INTEGER"),
    ]:
        try:
            _db_conn.execute(f"ALTER TABLE {tabela} ADD COLUMN {col} {tipo}")
        except sqlite3.OperationalError:
            pass
    log_db.info(f"🗄 DB ON | {_DB_PATH}")

@contextmanager
def _db():
    with _db_lock:
        try:
            yield _db_conn
        except sqlite3.Error as e:
            log_db.error(f"❌ DB: {e}"); raise

# ── links_cache ───────────────────────────────────────────────────
def db_get_link(url: str) -> Optional[str]:
    try:
        from utils.urls import _cache_key
        url = _cache_key(url)
        with _db() as db:
            row = db.execute(
                "SELECT url_conv FROM links_cache WHERE url_orig=?", (url,)
            ).fetchone()
            if row:
                db.execute(
                    "UPDATE links_cache SET ts=? WHERE url_orig=?",
                    (time.time(), url))
                return row[0]
    except Exception as e:
        log_db.error(f"❌ db_get_link: {e}")
    return None

def db_set_link(url_orig: str, url_conv: str, plat: str):
    try:
        from utils.urls import _cache_key
        url_orig = _cache_key(url_orig)
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO links_cache(url_orig,url_conv,plat,ts)"
                " VALUES(?,?,?,?)",
                (url_orig, url_conv, plat, time.time()))
    except Exception as e:
        log_db.error(f"❌ db_set_link: {e}")

# ── dedupe_temp ───────────────────────────────────────────────────
def db_get_dedupe(fp: str) -> Optional[dict]:
    try:
        limite = time.time() - TTL_DEDUPE
        with _db() as db:
            row = db.execute(
                "SELECT plat,cupons,alma,camp,asin,id_prod,benef,ts"
                " FROM dedupe_temp WHERE fp=? AND ts>=?",
                (fp, limite)).fetchone()
        if row:
            return {
                "plat": row[0], "cupons": json.loads(row[1] or "[]"),
                "alma": row[2], "camp": row[3],
                "asin": row[4] or "", "id_prod": row[5] or "",
                "benef": json.loads(row[6] or "[]"), "ts": row[7],
            }
    except Exception as e:
        log_db.error(f"❌ db_get_dedupe: {e}")
    return None

def db_get_ts_por_produto(plat: str, id_prod: str) -> Optional[float]:
    """
    Timestamp da passagem mais recente de uma oferta-produto, lido pela
    coluna canônica id_prod (índice idx_dt_id). Aplica o MESMO corte de
    TTL_DEDUPE que db_get_dedupe — entradas além de 24h não existem para
    efeito de ciclo de vida (oferta volta a NEW). Usado pelo C3 de
    produto, que precisa apenas do ts. Retorna None se não houver
    registro vivo. Não substitui db_get_dedupe nem toca o caminho do fp.
    """
    if not id_prod:
        return None
    try:
        limite = time.time() - TTL_DEDUPE
        with _db() as db:
            row = db.execute(
                "SELECT ts FROM dedupe_temp"
                " WHERE plat=? AND id_prod=? AND ts>=?"
                " ORDER BY ts DESC LIMIT 1",
                (plat, id_prod, limite)).fetchone()
        return row[0] if row else None
    except Exception as e:
        log_db.error(f"❌ db_get_ts_por_produto: {e}")
        return None

def db_set_dedupe(fp: str, plat: str, cupons: list, alma: str,
                  camp: str, asin: str = "", id_prod: str = "",
                  benef: list = None):
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO dedupe_temp"
                "(fp,plat,cupons,alma,camp,asin,id_prod,benef,ts)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (fp, plat, json.dumps(cupons or []), alma or "",
                 camp or "geral", asin or "", id_prod or "",
                 json.dumps(benef or []), time.time()))
    except Exception as e:
        log_db.error(f"❌ db_set_dedupe: {e}")

def db_buscar_janela_rapida(plat: str, janela: float = 900) -> list:
    try:
        limite = time.time() - janela
        with _db() as db:
            rows = db.execute(
                "SELECT fp,cupons,alma,asin,id_prod,benef,ts"
                " FROM dedupe_temp WHERE plat=? AND ts>=? ORDER BY ts DESC",
                (plat, limite)).fetchall()
        return [
            {"fp": r[0], "cupons": json.loads(r[1] or "[]"),
             "alma": r[2] or "", "asin": r[3] or "",
             "id_prod": r[4] or "", "benef": json.loads(r[5] or "[]"),
             "ts": r[6]}
            for r in rows
        ]
    except Exception as e:
        log_db.error(f"❌ db_janela: {e}")
        return []

# ── saturacao ─────────────────────────────────────────────────────
def db_registrar_sat(plat: str, sku: str = ""):
    try:
        with _db() as db:
            db.execute(
                "INSERT INTO saturacao(plat,sku,ts) VALUES(?,?,?)",
                (plat, sku or "", time.time()))
    except Exception as e:
        log_db.error(f"❌ db_sat: {e}")

def db_count_sat(plat: str, janela: float = 1800) -> int:
    try:
        limite = time.time() - janela
        with _db() as db:
            row = db.execute(
                "SELECT COUNT(*) FROM saturacao WHERE plat=? AND ts>=?",
                (plat, limite)).fetchone()
        return row[0] if row else 0
    except Exception as e:
        log_db.error(f"❌ db_count_sat: {e}")
        return 0

# ── oferta_estado ─────────────────────────────────────────────────
def db_get_estado(identity: str) -> Optional[dict]:
    try:
        with _db() as db:
            row = db.execute(
                "SELECT msg_id_dest,score,texto,plat,lider,janela_fim,"
                "edit_count,shadow_reply_id,ts"
                " FROM oferta_estado WHERE identity=?",
                (identity,)).fetchone()
        if row:
            return {
                "msg_id_dest": row[0], "score": row[1], "texto": row[2],
                "plat": row[3], "lider": row[4] or "",
                "janela_fim": row[5] or 0.0, "edit_count": row[6] or 0,
                "shadow_reply_id": row[7] or 0, "ts": row[8],
            }
    except Exception as e:
        log_db.error(f"❌ db_get_estado: {e}")
    return None

def db_set_estado(identity: str, msg_id_dest: int, score: int,
                  texto: str, plat: str, lider: str = "",
                  janela_fim: float = 0.0, edit_count: int = 0,
                  shadow_reply_id: int = 0):
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO oferta_estado"
                "(identity,msg_id_dest,score,texto,plat,lider,"
                "janela_fim,edit_count,shadow_reply_id,ts)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (identity, msg_id_dest, score, texto, plat, lider,
                 janela_fim, edit_count, shadow_reply_id, time.time()))
    except Exception as e:
        log_db.error(f"❌ db_set_estado: {e}")

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

# ── limpeza ───────────────────────────────────────────────────────
def db_limpar():
    from globals import _raw_cache, _final_cache
    try:
        agora = time.time()
        with _db() as db:
            db.execute("DELETE FROM dedupe_temp  WHERE ts<?", (agora - TTL_DEDUPE,))
            db.execute("DELETE FROM saturacao    WHERE ts<?", (agora - TTL_DEDUPE,))
            db.execute("DELETE FROM links_cache  WHERE ts<?", (agora - TTL_LINK_INATIVO,))
            db.execute("DELETE FROM oferta_estado WHERE ts<?", (agora - 30 * 86400,))
            db.execute("DELETE FROM shadow_reply  WHERE ts<?", (agora - 30 * 86400,))
        if len(_raw_cache) > 3000:
            for k in list(_raw_cache.keys())[:1000]:
                del _raw_cache[k]
        if len(_final_cache) > 3000:
            for k in list(_final_cache.keys())[:1000]:
                del _final_cache[k]
        log_db.debug("🗑 Limpeza temp OK")
    except Exception as e:
        log_db.error(f"❌ db_limpar: {e}")

def _db_count_links() -> int:
    try:
        with _db() as db:
            row = db.execute("SELECT COUNT(*) FROM links_cache").fetchone()
            return row[0] if row else 0
    except Exception:
        return 0
         

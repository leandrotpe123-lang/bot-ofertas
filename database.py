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
            url_canon TEXT, plat TEXT NOT NULL, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dedupe_temp(
            fp TEXT PRIMARY KEY, plat TEXT NOT NULL, cupons TEXT,
            alma TEXT, camp TEXT, id_prod TEXT, benef TEXT,
            cupom_id TEXT, ts REAL NOT NULL);
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
        CREATE TABLE IF NOT EXISTS post_estado(
            msg_id_dest INTEGER PRIMARY KEY,
            score INTEGER NOT NULL DEFAULT 0, texto TEXT NOT NULL DEFAULT '',
            plat TEXT NOT NULL DEFAULT '', lider TEXT DEFAULT '',
            janela_fim REAL DEFAULT 0, edit_count INTEGER DEFAULT 0,
            shadow_reply_id INTEGER DEFAULT 0, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS oferta_index(
            identity TEXT PRIMARY KEY, msg_id_dest INTEGER NOT NULL,
            ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS shadow_reply(
            identity TEXT PRIMARY KEY, msg_id INTEGER NOT NULL,
            enviado INTEGER DEFAULT 0, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS cupom_idx(
            plat TEXT NOT NULL, codigo TEXT NOT NULL,
            identity TEXT NOT NULL, ts REAL NOT NULL,
            PRIMARY KEY(plat, codigo));
        CREATE INDEX IF NOT EXISTS idx_lc_plat     ON links_cache(plat);
        CREATE INDEX IF NOT EXISTS idx_lc_ts       ON links_cache(ts);
        CREATE INDEX IF NOT EXISTS idx_dt_plat     ON dedupe_temp(plat,ts);
        CREATE INDEX IF NOT EXISTS idx_dt_id       ON dedupe_temp(id_prod);
        CREATE INDEX IF NOT EXISTS idx_dt_cupom    ON dedupe_temp(cupom_id);
        CREATE INDEX IF NOT EXISTS idx_sat         ON saturacao(plat,ts);
        CREATE INDEX IF NOT EXISTS idx_sl_code     ON short_links(code);
        CREATE INDEX IF NOT EXISTS idx_oe_identity ON oferta_estado(identity);
        CREATE INDEX IF NOT EXISTS idx_oe_plat     ON oferta_estado(plat);
        CREATE INDEX IF NOT EXISTS idx_oi_dest     ON oferta_index(msg_id_dest);
        CREATE INDEX IF NOT EXISTS idx_ci_lookup   ON cupom_idx(plat,codigo,ts);
    """)
    for tabela, col, tipo in [
        ("dedupe_temp",   "benef",            "TEXT"),
        ("dedupe_temp",   "id_prod",          "TEXT"),
        ("dedupe_temp",   "cupom_id",          "TEXT"),
        ("oferta_estado", "lider",            "TEXT"),
        ("oferta_estado", "janela_fim",       "REAL"),
        ("oferta_estado", "edit_count",       "INTEGER"),
        ("oferta_estado", "shadow_reply_id",  "INTEGER"),
        ("links_cache",   "url_canon",        "TEXT"),
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

def db_cupom_idx_buscar(plat: str, codigos: list, janela_s: float) -> Optional[str]:
    """Retorna a identity de um post de cupom se QUALQUER código da lista
    já estiver indexado dentro da janela (plat + código). Casamento por
    código compartilhado — o link nunca entra. Sem código → None."""
    cods = [c.upper() for c in (codigos or []) if c]
    if not cods:
        return None
    limite = time.time() - janela_s
    ph = ",".join("?" * len(cods))
    try:
        with _db() as cx:
            r = cx.execute(
                f"SELECT identity FROM cupom_idx"
                f" WHERE plat=? AND codigo IN ({ph}) AND ts>=?"
                f" ORDER BY ts DESC LIMIT 1",
                (plat, *cods, limite)).fetchone()
            return r[0] if r else None
    except Exception as e:
        log_db.error(f"❌ db_cupom_idx_buscar: {e}")
        return None


def db_cupom_idx_registrar(plat: str, codigos: list, identity: str) -> int:
    """Registra TODOS os códigos do post sob a MESMA identity
    (plat + código → identity), com ts atual (janela deslizante).
    Preserva todos os códigos — não só um. Retorna QUANTOS códigos
    eram NOVOS (não estavam no índice) — usado pela exceção de
    reativação 'passa se vier com mais códigos'."""
    cods = [c.upper() for c in (codigos or []) if c]
    if not cods or not identity:
        return 0
    agora = time.time()
    try:
        with _db() as cx:
            ph = ",".join("?" * len(cods))
            ja = {r[0] for r in cx.execute(
                f"SELECT codigo FROM cupom_idx"
                f" WHERE plat=? AND codigo IN ({ph})",
                (plat, *cods)).fetchall()}
            cx.executemany(
                "INSERT INTO cupom_idx(plat,codigo,identity,ts)"
                " VALUES(?,?,?,?)"
                " ON CONFLICT(plat,codigo) DO UPDATE SET"
                " identity=excluded.identity, ts=excluded.ts",
                [(plat, c, identity, agora) for c in cods])
            return sum(1 for c in cods if c not in ja)
    except Exception as e:
        log_db.error(f"❌ db_cupom_idx_registrar: {e}")
        return 0


def db_set_dedupe(fp: str, plat: str, cupons: list, alma: str,
                  camp: str, id_prod: str = "",
                  benef: list = None, cupom_id: str = ""):
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO dedupe_temp"
                "(fp,plat,cupons,alma,camp,id_prod,benef,cupom_id,ts)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (fp, plat, json.dumps(cupons or []), alma or "",
                 camp or "geral", id_prod or "",
                 json.dumps(benef or []), cupom_id or "", time.time()))
    except Exception as e:
        log_db.error(f"❌ db_set_dedupe: {e}")
        
def db_buscar_janela_rapida(plat: str, janela: float = 900) -> list:
    try:
        limite = time.time() - janela
        with _db() as db:
            rows = db.execute(
                "SELECT fp,cupons,alma,id_prod,benef,ts"
                " FROM dedupe_temp WHERE plat=? AND ts>=? ORDER BY ts DESC",
                (plat, limite)).fetchall()
        return [
            {"fp": r[0], "cupons": json.loads(r[1] or "[]"),
             "alma": r[2] or "",
             "id_prod": r[3] or "", "benef": json.loads(r[4] or "[]"),
             "ts": r[5]}
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

# ── post_estado / oferta_index (modelo container/oferta) ──────────
def db_get_post(msg_id_dest: int) -> Optional[dict]:
    """Estado real do post. MESMA forma de dict que db_get_estado
    (sem 'identity'), para decidir(...) consumir sem mudança."""
    try:
        with _db() as db:
            row = db.execute(
                "SELECT msg_id_dest,score,texto,plat,lider,janela_fim,"
                "edit_count,shadow_reply_id,ts"
                " FROM post_estado WHERE msg_id_dest=?",
                (msg_id_dest,)).fetchone()
        if row:
            return {
                "msg_id_dest": row[0], "score": row[1], "texto": row[2],
                "plat": row[3], "lider": row[4] or "",
                "janela_fim": row[5] or 0.0, "edit_count": row[6] or 0,
                "shadow_reply_id": row[7] or 0, "ts": row[8],
            }
    except Exception as e:
        log_db.error(f"❌ db_get_post: {e}")
    return None


def db_overlap_posts(ofertas: list[str]) -> list[tuple[int, int]]:
    """Posts que compartilham >=1 oferta com a lista dada, como
    (msg_id_dest, n_sobreposicao), ordenados por sobreposicao desc.
    Base do D1 (maior sobreposicao). A janela é avaliada por
    decidir(...) no chamador, não aqui. Lista vazia → []."""
    if not ofertas:
        return []
    try:
        marcadores = ",".join("?" * len(ofertas))
        with _db() as db:
            rows = db.execute(
                f"SELECT msg_id_dest, COUNT(*) AS n FROM oferta_index"
                f" WHERE identity IN ({marcadores})"
                f" GROUP BY msg_id_dest ORDER BY n DESC",
                tuple(ofertas)).fetchall()
        return [(r[0], r[1]) for r in rows]
    except Exception as e:
        log_db.error(f"❌ db_overlap_posts: {e}")
        return []


def db_ofertas_de_post(msg_id_dest: int) -> list[str]:
    """Todas as ofertas que compõem um post (caminho inverso, via
    idx_oi_dest). Usado para calcular ofertas novas na evolução e
    re-apontar na substituição."""
    try:
        with _db() as db:
            rows = db.execute(
                "SELECT identity FROM oferta_index WHERE msg_id_dest=?",
                (msg_id_dest,)).fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        log_db.error(f"❌ db_ofertas_de_post: {e}")
        return []


def db_registrar_post(msg_id_dest: int, ofertas: list[str], score: int,
                      texto: str, plat: str, lider: str = "",
                      janela_fim: float = 0.0, edit_count: int = 0,
                      shadow_reply_id: int = 0):
    """Upsert do estado do post + mapeamento de cada oferta→post.
    Serve para publicação nova E evolução (idempotente)."""
    try:
        agora = time.time()
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO post_estado"
                "(msg_id_dest,score,texto,plat,lider,"
                "janela_fim,edit_count,shadow_reply_id,ts)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (msg_id_dest, score, texto, plat, lider,
                 janela_fim, edit_count, shadow_reply_id, agora))
            for oferta in ofertas:
                db.execute(
                    "INSERT OR REPLACE INTO oferta_index"
                    "(identity,msg_id_dest,ts) VALUES(?,?,?)",
                    (oferta, msg_id_dest, agora))
    except Exception as e:
        log_db.error(f"❌ db_registrar_post: {e}")


def db_remover_post(msg_id_dest: int):
    """Apaga o post e TODAS as suas ofertas do índice. Uso restrito ao
    fallback de SUBSTITUIÇÃO (msg antiga apagada no Telegram, nova criada),
    chamado no id ANTIGO antes de db_registrar_post(novo, ...). As ofertas
    levadas à nova msg são reapontadas pelo próprio INSERT OR REPLACE do
    db_registrar_post; esta função existe para (a) remover o post_estado
    órfão e (b) impedir que ofertas do post antigo NÃO levadas à nova msg
    fiquem apontando para uma mensagem já apagada. NÃO usar no caminho de
    EDIÇÃO (msg_id preservado) — ali apagaria o post vivo."""
    try:
        with _db() as db:
            db.execute("DELETE FROM post_estado WHERE msg_id_dest=?",
                       (msg_id_dest,))
            db.execute("DELETE FROM oferta_index WHERE msg_id_dest=?",
                       (msg_id_dest,))
    except Exception as e:
        log_db.error(f"❌ db_remover_post: {e}")

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
            db.execute("DELETE FROM post_estado  WHERE ts<?", (agora - 30 * 86400,))
            db.execute("DELETE FROM oferta_index WHERE ts<?", (agora - 30 * 86400,))
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
         

"""Camada 7 — Banco / Conexão, mutex e schema.

Responsabilidade ÚNICA: a conexão SQLite do processo, o mutex que a
serializa, os PRAGMAs, o schema e o contexto de acesso `_db()`.

NÃO conhece regra de nenhum domínio: não sabe o que é um cupom, um
post ou um link. Fica ABAIXO dos módulos de domínio; não importa
nenhum deles.

══════════════════════════════════════════════════════════════════
CONTRATO INTERNO DA CAMADA — LEIA ANTES DE ALTERAR
══════════════════════════════════════════════════════════════════
_db tem prefixo `_`, mas NÃO é privado deste arquivo: é o ponto de
acesso usado por TODOS os módulos de domínio (database_links,
database_posts, database_cupons, database_manutencao) e também por
main.py, via a fachada `database`. O underscore foi PRESERVADO na
extração para não renomear nada fora de escopo — dívida registrada.

_db_conn e _db_lock são de fato internos: nenhum módulo os importa,
e importá-los seria BUG. `from ... import _db_conn` liga por VALOR e
capturaria o None anterior a _init_db(), nunca a conexão real.
Os domínios devem sempre usar `_db()`, que lê _db_conn na chamada.

_init_db mora aqui por restrição da linguagem, não por escolha: ele
declara `global _db_conn`, e esse global é o DESTE módulo. Movê-lo
para outro arquivo faria _db() enxergar None para sempre. Não separe.

Extraído de database sem qualquer alteração de comportamento.
"""
from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from threading import Lock
from typing import Optional

from config import _DB_PATH
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
        CREATE TABLE IF NOT EXISTS short_links(
            code TEXT PRIMARY KEY, url TEXT NOT NULL, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS post_estado(
            msg_id_dest INTEGER PRIMARY KEY,
            score INTEGER NOT NULL DEFAULT 0, texto TEXT NOT NULL DEFAULT '',
            plat TEXT NOT NULL DEFAULT '', lider TEXT DEFAULT '',
            janela_fim REAL DEFAULT 0, edit_count INTEGER DEFAULT 0,
            ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS oferta_index(
            identity TEXT PRIMARY KEY, msg_id_dest INTEGER NOT NULL,
            ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS cupom_idx(
            plat TEXT NOT NULL, codigo TEXT NOT NULL,
            identity TEXT NOT NULL, ts REAL NOT NULL,
            PRIMARY KEY(plat, codigo));
        CREATE TABLE IF NOT EXISTS origem_post(
            chat TEXT NOT NULL, msg_id INTEGER NOT NULL,
            dest INTEGER NOT NULL, ts REAL NOT NULL,
            PRIMARY KEY(chat, msg_id));
        CREATE INDEX IF NOT EXISTS idx_lc_plat     ON links_cache(plat);
        CREATE INDEX IF NOT EXISTS idx_lc_ts       ON links_cache(ts);
        CREATE INDEX IF NOT EXISTS idx_sl_code     ON short_links(code);
        CREATE INDEX IF NOT EXISTS idx_oi_dest     ON oferta_index(msg_id_dest);
        CREATE INDEX IF NOT EXISTS idx_ci_lookup   ON cupom_idx(plat,codigo,ts);
    """)
    for tabela, col, tipo in [
        ("links_cache",   "url_canon",        "TEXT"),
        ("post_estado",   "midia_chat",       "TEXT"),
        ("post_estado",   "score_versao",     "INTEGER"),
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
          

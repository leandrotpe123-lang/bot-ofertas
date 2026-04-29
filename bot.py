# ╔══════════════════════════════════════════════════════════════════╗
# ║  FOGUETÃO v76.0 — Arquitetura Limpa (6 camadas)                 ║
# ║  1.Ingestão → 2.Classificação → 3.Normalização →                ║
# ║  4.Deduplicação → 5.Montagem → 6.Envio                          ║
# ╚══════════════════════════════════════════════════════════════════╝
from __future__ import annotations

import asyncio, concurrent.futures, hashlib, heapq, io, json, logging
import os, random, re, sqlite3, time, unicodedata

from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from functools import lru_cache
from threading import Lock
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

import aiohttp
from aiohttp import web
from bs4 import BeautifulSoup
from telethon import TelegramClient, events
from telethon.errors import (
    AuthKeyUnregisteredError,
    FloodWaitError,
    MessageNotModifiedError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage

try:
    from PIL import Image
    _PIL_OK = True
except ImportError:
    _PIL_OK = False

# ═══════════════════════════════════════════════════════════════════
# LOGS
# ═══════════════════════════════════════════════════════════════════
def _mk_log(nome: str, cor: str) -> logging.Logger:
    lg = logging.getLogger(nome)
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            f'\033[{cor}m[%(name)-10s]\033[0m %(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'))
        lg.addHandler(h); lg.setLevel(logging.DEBUG)
    return lg

log_ing  = _mk_log('INGESTAO',  '1;37')
log_cls  = _mk_log('CLASSIF',   '1;36')
log_nrm  = _mk_log('NORMAL',    '1;33')
log_ded  = _mk_log('DEDUP',     '1;35')
log_fmt  = _mk_log('FORMAT',    '1;34')
log_out  = _mk_log('ENVIO',     '1;32')
log_db   = _mk_log('DB',        '1;38;5;208')
log_sys  = _mk_log('SISTEMA',   '1;37')
log_hc   = _mk_log('HEALTH',    '1;38;5;118')

# ═══════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════════
API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION", "")
GRUPOS_ORIGEM  = ["promotom", "fumotom", "botofera", "fadadoscupons", "SamuelF3lipePromo", "paraseubaby", "fadapromos"]
GRUPO_DESTINO  = "@ofertap"
_AMZ_TAG       = os.environ.get("AMAZON_TAG",         "leo21073-20")
_SHP_APP_ID    = os.environ.get("SHOPEE_APP_ID",      "18348480261")
_SHP_SECRET    = os.environ.get("SHOPEE_SECRET",      "")
_MGL_PARTNER   = os.environ.get("MAGALU_PARTNER_ID",  "3440")
_MGL_PROMOTER  = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
_MGL_PID       = os.environ.get("MAGALU_PID",         "magazinevoce")
_MGL_SLUG      = os.environ.get("MAGALU_SLUG",        "magazineleo12")
_CUTTLY_KEY    = os.environ.get("CUTTLY_API_KEY",     "")
_IMG_AMZ       = "cupom-amazon.jpg"
_IMG_SHP       = "shopee_promo.jpg"
_IMG_MGL       = "magalu_promo.jpg"
_SEM_ENVIO     = asyncio.Semaphore(3)
_SEM_HTTP      = asyncio.Semaphore(20)
_EXECUTOR      = concurrent.futures.ThreadPoolExecutor(max_workers=4)
USER_AGENTS    = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

# ═══════════════════════════════════════════════════════════════════
# PERSISTÊNCIA JSON (mapa de mensagens)
# ═══════════════════════════════════════════════════════════════════
ARQUIVO_MAPEAMENTO = "map_mensagens.json"
_MAP_LOCK = Lock()

def _ler_json(path: str) -> dict:
    if not os.path.exists(path): return {}
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception as e: log_sys.error(f"❌ ler {path}: {e}"); return {}

def _gravar_json(path: str, data: dict, lock: Lock):
    with lock:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e: log_sys.error(f"❌ gravar {path}: {e}")

ler_mapa    = lambda: _ler_json(ARQUIVO_MAPEAMENTO)
salvar_mapa = lambda m: _gravar_json(ARQUIVO_MAPEAMENTO, m, _MAP_LOCK)

# ═══════════════════════════════════════════════════════════════════
# SQLITE
# ═══════════════════════════════════════════════════════════════════
_DB_PATH  = "foguetao.db"
_db_conn: Optional[sqlite3.Connection] = None
_db_lock  = Lock()
CACHE_TTL     = 86400
TTL_DEDUPE    = 86400
TTL_SCHEDULER = 30 * 86400
TTL_LINK_INATIVO = 7 * 86400

def _init_db():
    global _db_conn
    _db_conn = sqlite3.connect(
        _DB_PATH, check_same_thread=False,
        timeout=10, isolation_level=None
    )
    for p in [
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA cache_size=-16000",
        "PRAGMA temp_store=MEMORY",
        "PRAGMA busy_timeout=10000",
    ]:
        _db_conn.execute(p)

    _db_conn.executescript("""
        CREATE TABLE IF NOT EXISTS links_cache(
            url_orig TEXT PRIMARY KEY,
            url_conv TEXT NOT NULL,
            plat     TEXT NOT NULL,
            ts       REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dedupe_temp(
            fp      TEXT PRIMARY KEY,
            plat    TEXT NOT NULL,
            cupons  TEXT,
            alma    TEXT,
            camp    TEXT,
            asin    TEXT,
            id_prod TEXT,
            benef   TEXT,
            ts      REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS saturacao(
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            plat TEXT    NOT NULL,
            sku  TEXT,
            ts   REAL    NOT NULL
        );
        CREATE TABLE IF NOT EXISTS scheduler(
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            plat  TEXT    NOT NULL,
            hora  INTEGER NOT NULL,
            score REAL    DEFAULT 1.0,
            ts    REAL    NOT NULL
        );
        CREATE TABLE IF NOT EXISTS short_links(
            code TEXT PRIMARY KEY,
            url  TEXT NOT NULL,
            ts   REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS oferta_estado(
            identity    TEXT    PRIMARY KEY,
            msg_id_dest INTEGER NOT NULL,
            score       INTEGER NOT NULL DEFAULT 0,
            texto       TEXT    NOT NULL DEFAULT '',
            plat        TEXT    NOT NULL DEFAULT '',
            ts          REAL    NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_lc_plat    ON links_cache(plat);
        CREATE INDEX IF NOT EXISTS idx_lc_ts      ON links_cache(ts);
        CREATE INDEX IF NOT EXISTS idx_dt_plat    ON dedupe_temp(plat,ts);
        CREATE INDEX IF NOT EXISTS idx_dt_asin    ON dedupe_temp(asin);
        CREATE INDEX IF NOT EXISTS idx_dt_id      ON dedupe_temp(id_prod);
        CREATE INDEX IF NOT EXISTS idx_sat        ON saturacao(plat,ts);
        CREATE INDEX IF NOT EXISTS idx_sch        ON scheduler(plat,hora);
        CREATE INDEX IF NOT EXISTS idx_sl_code    ON short_links(code);
        CREATE INDEX IF NOT EXISTS idx_oe_identity ON oferta_estado(identity);
        CREATE INDEX IF NOT EXISTS idx_oe_plat    ON oferta_estado(plat);
    """)

    # Migração segura — adiciona colunas se não existirem
    for tabela, col, tipo in [
        ("dedupe_temp", "benef",   "TEXT"),
        ("dedupe_temp", "asin",    "TEXT"),
        ("dedupe_temp", "id_prod", "TEXT"),
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


# ── links_cache ──────────────────────────────────────────────────────────────

def db_get_link(url: str) -> Optional[str]:
    try:
        url = _cache_key(url)
        with _db() as db:
            row = db.execute(
                "SELECT url_conv FROM links_cache WHERE url_orig=?",
                (url,)
            ).fetchone()
            if row:
                db.execute(
                    "UPDATE links_cache SET ts=? WHERE url_orig=?",
                    (time.time(), url)
                )
                return row[0]
    except Exception as e:
        log_db.error(f"❌ db_get_link: {e}")
    return None

def db_set_link(url_orig: str, url_conv: str, plat: str):
    try:
        url_orig = _cache_key(url_orig)
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO links_cache"
                "(url_orig,url_conv,plat,ts) VALUES(?,?,?,?)",
                (url_orig, url_conv, plat, time.time())
            )
    except Exception as e:
        log_db.error(f"❌ db_set_link: {e}")


# ── dedupe_temp ──────────────────────────────────────────────────────────────

def db_get_dedupe(fp: str) -> Optional[dict]:
    try:
        limite = time.time() - TTL_DEDUPE
        with _db() as db:
            row = db.execute(
                "SELECT plat,cupons,alma,camp,asin,id_prod,benef,ts "
                "FROM dedupe_temp WHERE fp=? AND ts>=?",
                (fp, limite)
            ).fetchone()
        if row:
            return {
                "plat":    row[0],
                "cupons":  json.loads(row[1] or "[]"),
                "alma":    row[2],
                "camp":    row[3],
                "asin":    row[4] or "",
                "id_prod": row[5] or "",
                "benef":   json.loads(row[6] or "[]"),
                "ts":      row[7],
            }
    except Exception as e:
        log_db.error(f"❌ db_get_dedupe: {e}")
    return None

def db_set_dedupe(fp: str, plat: str, cupons: list, alma: str,
                  camp: str, asin: str = "", id_prod: str = "",
                  benef: list = None):
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO dedupe_temp"
                "(fp,plat,cupons,alma,camp,asin,id_prod,benef,ts)"
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    fp, plat,
                    json.dumps(cupons  or []),
                    alma   or "",
                    camp   or "geral",
                    asin   or "",
                    id_prod or "",
                    json.dumps(benef or []),
                    time.time(),
                )
            )
    except Exception as e:
        log_db.error(f"❌ db_set_dedupe: {e}")

def db_buscar_janela_rapida(plat: str, janela: float = 900) -> list:
    try:
        limite = time.time() - janela
        with _db() as db:
            rows = db.execute(
                "SELECT fp,cupons,alma,asin,id_prod,benef,ts "
                "FROM dedupe_temp WHERE plat=? AND ts>=? ORDER BY ts DESC",
                (plat, limite)
            ).fetchall()
        return [
            {
                "fp":      r[0],
                "cupons":  json.loads(r[1] or "[]"),
                "alma":    r[2] or "",
                "asin":    r[3] or "",
                "id_prod": r[4] or "",
                "benef":   json.loads(r[5] or "[]"),
                "ts":      r[6],
            }
            for r in rows
        ]
    except Exception as e:
        log_db.error(f"❌ db_janela: {e}")
        return []


# ── saturacao ────────────────────────────────────────────────────────────────

def db_registrar_sat(plat: str, sku: str = ""):
    try:
        with _db() as db:
            db.execute(
                "INSERT INTO saturacao(plat,sku,ts) VALUES(?,?,?)",
                (plat, sku or "", time.time())
            )
    except Exception as e:
        log_db.error(f"❌ db_sat: {e}")

def db_count_sat(plat: str, janela: float = 1800) -> int:
    try:
        limite = time.time() - janela
        with _db() as db:
            row = db.execute(
                "SELECT COUNT(*) FROM saturacao WHERE plat=? AND ts>=?",
                (plat, limite)
            ).fetchone()
        return row[0] if row else 0
    except Exception as e:
        log_db.error(f"❌ db_count_sat: {e}")
        return 0


# ── oferta_estado (sistema evolutivo) ────────────────────────────────────────

def db_get_estado(identity: str) -> Optional[dict]:
    try:
        with _db() as db:
            row = db.execute(
                "SELECT msg_id_dest,score,texto,plat,ts "
                "FROM oferta_estado WHERE identity=?",
                (identity,)
            ).fetchone()
        if row:
            return {
                "msg_id_dest": row[0],
                "score":       row[1],
                "texto":       row[2],
                "plat":        row[3],
                "ts":          row[4],
            }
    except Exception as e:
        log_db.error(f"❌ db_get_estado: {e}")
    return None

def db_set_estado(identity: str, msg_id_dest: int,
                  score: int, texto: str, plat: str):
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO oferta_estado"
                "(identity,msg_id_dest,score,texto,plat,ts)"
                "VALUES(?,?,?,?,?,?)",
                (identity, msg_id_dest, score, texto, plat, time.time())
            )
    except Exception as e:
        log_db.error(f"❌ db_set_estado: {e}")


# ── short_links (encurtador próprio Magalu) ──────────────────────────────────

def db_get_short(code: str) -> Optional[str]:
    try:
        with _db() as db:
            row = db.execute(
                "SELECT url FROM short_links WHERE code=?",
                (code,)
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
                (code, url, time.time())
            )
    except Exception as e:
        log_db.error(f"❌ db_set_short: {e}")


# ── limpeza geral ─────────────────────────────────────────────────────────────

def db_limpar():
    try:
        agora = time.time()
        with _db() as db:
            db.execute("DELETE FROM dedupe_temp  WHERE ts<?",
                       (agora - TTL_DEDUPE,))
            db.execute("DELETE FROM saturacao    WHERE ts<?",
                       (agora - TTL_DEDUPE,))
            db.execute("DELETE FROM scheduler    WHERE ts<?",
                       (agora - TTL_SCHEDULER,))
            db.execute("DELETE FROM links_cache  WHERE ts<?",
                       (agora - TTL_LINK_INATIVO,))
            db.execute("DELETE FROM oferta_estado WHERE ts<?",
                       (agora - 30 * 86400,))

        # Limita caches em memória
        global _raw_cache, _final_cache
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
            row = db.execute(
                "SELECT COUNT(*) FROM links_cache"
            ).fetchone()
            return row[0] if row else 0
    except Exception:
        return 0

# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 1 — INGESTÃO / EXTRAÇÃO
# Responsabilidade: receber mensagem crua, extrair texto, links e mídia.
# NÃO decide nada. NÃO altera dados.
# ═══════════════════════════════════════════════════════════════════════════════

_RE_URL = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c\u200d\u2060]+')

@dataclass
class MensagemBruta:
    msg_id: int
    chat:   str
    texto:  str
    links:  List[str]
    tem_midia: bool
    media_obj: object


def ingerir(event) -> MensagemBruta:
    """Extrai dados crus da mensagem. Zero lógica de negócio."""

    texto = event.message.text or getattr(event.message, "message", "") or ""

    links = [u.strip().rstrip('.,;)>]}!?') for u in _RE_URL.findall(texto)]

    tem_midia = (
        event.message.media is not None
        and not isinstance(event.message.media, MessageMediaWebPage)
    )

    try:
        chat_obj = getattr(event, "_chat", None)
        username = getattr(chat_obj, "username", None)
        chat = (username or str(event.chat_id)).lower()
    except Exception:
        chat = str(event.chat_id)

    log_ing.debug(
        f"📩 id={event.message.id} chat={chat} links={len(links)} midia={tem_midia}"
    )

    return MensagemBruta(
        msg_id=event.message.id,
        chat=chat,
        texto=texto,
        links=links,
        tem_midia=tem_midia,
        media_obj=event.message,
    )

# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 2 — CLASSIFICAÇÃO 
# ═══════════════════════════════════════════════════════════════════════════════

_MUNDIAIS=frozenset({
    "store.epicgames.com","epicgames.com","store.steampowered.com",
    "steampowered.com","gaming.amazon.com","twitch.tv","gog.com",
    "humblebundle.com","itch.io"
})
_BLOQUEADOS=frozenset({
    "mercadolivre.com.br","mercadopago.com.br","mercadolivre.com",
    "meli.com","ml.com.br","pelando.com.br","promobit.com.br",
    "cuponomia.com.br","zoom.com.br","buscape.com.br",
    "bondfaro.com.br","ofertasbrasil.com.br"
})
_AMZ_DOMINIOS=frozenset({
    "amazon.com.br","amazon.com","amzn.to","amzn.com","a.co",
    "amzlink.to","amzn.eu"
})
_SHP_DOMINIOS=frozenset({
    "shopee.com.br","s.shopee.com.br","shopee.com",
    "shope.ee","flapremios.com.br"
})
_MGL_DOMINIOS=frozenset({
    "magazineluiza.com.br","sacola.magazineluiza.com.br",
    "magazinevoce.com.br","maga.lu","divulgador.magalu.com"
})
_ENCURTADORES=frozenset({
    "bit.ly","cutt.ly","tinyurl.com","t.co","ow.ly","goo.gl",
    "rb.gy","is.gd","tiny.cc","buff.ly","short.io","bl.ink",
    "rebrand.ly","shorturl.at"
})
_PRESERVE=frozenset({"wa.me","api.whatsapp.com"})
_DELETAR=frozenset({"t.me","telegram.me","telegram.org","chat.whatsapp.com"})

_AMZ_PATHS_SEM_TAG=re.compile(
    r'^/(?:gaming(?:/|$)|claims(?:/|$)|gp/yourstore(?:/|$)|'
    r'gp/css(?:/|$)|gp/help(?:/|$)|gp/cart(?:/|$)|wishlist(?:/|$)|'
    r'hz/|ap/|gp/registry(?:/|$))',re.I
)

_P_SHP=[
    re.compile(r'/product/(\d+)/(\d+)'),
    re.compile(r'/item/(\d+)/(\d+)'),
    re.compile(r'/i\.(\d+)\.(\d+)')
]

_P_MGL=re.compile(r'/(?:[^/]+/)?p/([a-z0-9]{5,})(?:/|$|[?#])',re.I)

_P_AMZ_ASIN=[
    re.compile(r'/dp/([A-Z0-9]{10})',re.I),
    re.compile(r'/gp/product/([A-Z0-9]{10})',re.I),
    re.compile(r'[?&]asin=([A-Z0-9]{10})',re.I)
]

@dataclass
class LinkClassificado:
    url_original: str
    plat: Optional[str]
    tipo: Optional[str]
    sku: str
    id_global: str = ""

def _netloc(url:str)->str:
    try:
        p=urlparse(url)
        nl=(p.hostname or "").lower()
        if nl.startswith("www."): nl=nl[4:]
        return nl.strip(".")
    except Exception:
        return ""

def _extrair_asin(p)->str:
    text=p.path+"?"+p.query
    for pat in _P_AMZ_ASIN:
        m=pat.search(text)
        if m:return m.group(1).upper()
    return ""

def _extrair_sku_shopee(p)->str:
    text=p.path+"?"+p.query
    for pat in _P_SHP:
        m=pat.search(text)
        if m:return f"{m.group(1)}.{m.group(2)}"
    return ""

def _extrair_sku_magalu(p)->str:
    m=_P_MGL.search(p.path)
    return m.group(1) if m else ""

def classificar_url(url:str)->LinkClassificado:
    if not url or len(url)>4000 or "://" not in url:
        return LinkClassificado(url,None,"invalido","")

    p=urlparse(url)
    nl=_netloc(url)
    if not nl:
        return LinkClassificado(url,None,"invalido","")

    for d in _MUNDIAIS:
        if nl==d or nl.endswith("."+d):
            return LinkClassificado(url,"mundial","mundial","")

    for d in _BLOQUEADOS:
        if nl==d or nl.endswith("."+d):
            return LinkClassificado(url,None,"bloqueado","")

    for d in _DELETAR:
        if nl==d or nl.endswith("."+d):
            return LinkClassificado(url,None,"grupo_externo","")

    for d in _PRESERVE:
        if nl==d or nl.endswith("."+d):
            return LinkClassificado(url,"preservar","preservar","")

    for d in _MGL_DOMINIOS:
        if nl==d or nl.endswith("."+d):
            sku=_extrair_sku_magalu(p)
            if "sacola" in nl and not p.path.strip("/"):
                return LinkClassificado(url,"magalu","invalido",sku)
            tipo=("produto" if sku else
                  "lista" if "/l/" in p.path else
                  "selecao" if "/selecao/" in p.path else
                  "campanha")
            return LinkClassificado(
    url_original=url,
    plat="magalu",
    tipo=tipo,
    sku=sku,
    id_global=f"mgl:{sku}" if sku else ""
            )

    for d in _AMZ_DOMINIOS:
        if nl==d or nl.endswith("."+d):
            asin=_extrair_asin(p)
            if _AMZ_PATHS_SEM_TAG.match(p.path):
                return LinkClassificado(url,"amazon","claims","")
            tipo=("produto" if asin else
                  "busca" if re.search(r'/s[/?]|/deals|/b[/?]',p.path) else
                  "evento" if re.search(r'/events/|/stores/',p.path) else
                  "campanha")
            return LinkClassificado(
    url_original=url,
    plat="amazon",
    tipo=tipo,
    sku=asin,
    id_global=f"amz:{asin}" if asin else ""
            )

    for d in _SHP_DOMINIOS:
        if nl==d or nl.endswith("."+d):
            if nl=="flapremios.com.br":
                return LinkClassificado(url,"shopee","campanha","")
            sku=_extrair_sku_shopee(p)
            return LinkClassificado(
    url_original=url,
    plat="shopee",
    tipo="produto" if sku else "busca",
    sku=sku,
    id_global=f"shp:{sku}" if sku else ""
            )

    for d in _ENCURTADORES:
        if nl==d or nl.endswith("."+d):
            return LinkClassificado(url,"expandir","encurtado","")

    return LinkClassificado(url,None,"desconhecido","")

_cls_cache:OrderedDict[str,LinkClassificado]=OrderedDict()
_cls_lock=Lock()
_CACHE_LIMIT=5000

def _cache_key(url:str)->str:
    try:
        p=urlparse(url.strip())
        host=(p.hostname or "").lower().strip(".")
        return urlunparse((p.scheme.lower(),host,p.path.rstrip("/"),"",p.query,""))
    except Exception:
        return url.strip().lower()

def _classificar_cached(url:str)->LinkClassificado:
    key=_cache_key(url)
    with _cls_lock:
        lc=_cls_cache.get(key)
        if lc is not None:
            _cls_cache.move_to_end(key)
            return lc
    lc=classificar_url(url)
    with _cls_lock:
        _cls_cache[key]=lc
        _cls_cache.move_to_end(key)
        if len(_cls_cache)>_CACHE_LIMIT:
            _cls_cache.popitem(last=False)
    return lc

def classificar_links(links:List[str])->List[LinkClassificado]:
    vistos:Set[str]=set()
    result:List[LinkClassificado]=[]
    for u in links:
        key=_cache_key(u)
        if key in vistos:
            continue
        vistos.add(key)
        result.append(_classificar_cached(u))
    validos=[r for r in result if r.plat is not None]
    log_cls.debug(f"🔍 {len(validos)}/{len(links)} classificados ({len(links)-len(result)} duplicados)")
    return result

# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 3 — NORMALIZAÇÃO  (pipeline 12 passos)
# ═══════════════════════════════════════════════════════════════════════════════

# ── 3a. Filtro de texto ──────────────────────────────────────────────────────

_FILTRO_TEXTO = [
    "Monitor Samsung","Fonte Mancer","Placa de video","Monitor LG",
    "PC home Essential","Suporte articulado","VHAGAR","Superframe",
    "AM5","AM4","GTX","Placa de Vídeo","DDR5","DDR4","Dram","Monitor Safe",
    "Monitor Redragon","CL18","CL16","CL32","MT/s","MHz",
    "RX 580","Ryzen","Placa Mãe","Gabinete Gamer",
    "Water Cooler","Monitor Dell","Monitor Gamer","Air Cooler",
]
_RE_MERCADO_LIVRE = re.compile(r'\b(?:mercado\s*livre|mercadolivre|mercado\s*pago)\b', re.I)
_RE_MULTI_OFERTA  = re.compile(
    r'\b(?:ofertas?|promoções?)\s+(?:na\s+|no\s+|da\s+)?'
    r'(?:shopee|amazon|magalu|magazine\s*luiza)\b', re.I)
_RE_PRECO_LINHA   = re.compile(r'R\$\s?[\d.,]+')
_RE_URL_COUNT     = re.compile(r'https?://')

def _eh_multi_produto(texto: str) -> bool:
    if _RE_MULTI_OFERTA.search(texto): return True
    linhas_preco = sum(1 for l in texto.splitlines() if _RE_PRECO_LINHA.search(l))
    return linhas_preco >= 2 or len(_RE_URL_COUNT.findall(texto)) >= 3

def texto_bloqueado(texto: str) -> bool:
    if _RE_MERCADO_LIVRE.search(texto):
        log_cls.debug("🚫 Mercado Livre"); return True
    if _eh_multi_produto(texto): return False
    tl = texto.lower()
    for p in _FILTRO_TEXTO:
        if p.lower() in tl:
            log_cls.debug(f"🚫 Filtro: '{p}'"); return True
    return False

# ── 3b. Limpeza de ruído textual ─────────────────────────────────────────────

_RE_INVISIVEIS  = re.compile(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]')
_RE_GRUPO_EXT   = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)[^\s]*', re.I)
_RE_LIXO_STRUCT = re.compile(
    r'^\s*(?:-?\s*An[uú]ncio|Publicidade|:::+|---+|===+|'
    r'[-–—]\s*(?:ML|MG|AMZ)|(?:ML|MG|AMZ)\s*:)\s*$', re.I)
_RE_CTA = re.compile(
    r'^\s*(?:link\s+(?:do\s+)?produto|link\s+da\s+oferta|resgate\s+aqui|'
    r'clique\s+aqui|acesse\s+aqui|compre\s+aqui|grupo\s+vip|'
    r'entrar\s+no\s+grupo|acessar\s+grupo)\s*:?\s*$', re.I)
_RE_REDES = re.compile(
    r'^\s*(?:redes\s+\w+|'
    r'[-–]\s*grupo\s*(?:cupons?|promoções?|vip)?\s*:?\s*$|'
    r'[-–]\s*(?:chat|twitter|whatsapp|instagram|tiktok|youtube)\s*:?\s*$|'
    r'acesse\s+nossas\s+redes)', re.I)
_RE_ROTULO    = re.compile(r'^\s*[-–•]\s*\w[\w\s]{0,30}:\s*$')
_RE_EMOJI_CHK = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF\u2B50\u2B55]")

def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI_CHK.search(s))

def _eh_header_canal(linha: str) -> bool:
    l = linha.strip()
    if not l or _tem_emoji(l[0]): return False
    if re.match(r'^[A-ZÀ-Ú][\w\s]{2,30}\s*/\s*[\w\s]{2,30}', l): return True
    if re.match(r'^[A-ZÀÁÂÃÉÊÍÓÔÕÚ\s]{4,30}[\s🔥💥⚡🚀]+$', l, re.UNICODE): return True
    return False

def limpar_texto(texto: str) -> str:
    texto = _RE_INVISIVEIS.sub(" ", texto).replace("\r\n", "\n").replace("\r", "\n")
    linhas = texto.split("\n")
    saida: List[str] = []
    vazio = False; em_redes = False; primeira = True
    for linha in linhas:
        l = linha.strip()
        if not l:
            if not vazio: saida.append("")
            vazio = True; em_redes = False; continue
        vazio = False
        if primeira:
            primeira = False
            if _eh_header_canal(l): continue
        if _RE_REDES.match(l): em_redes = True; continue
        if em_redes:
            if _RE_ROTULO.match(l) or not l: continue
            if not re.match(r'https?://', l): em_redes = False
            else: continue
        if _RE_CTA.match(l) or _RE_LIXO_STRUCT.match(l): continue
        if _RE_GRUPO_EXT.search(l):
            l = _RE_GRUPO_EXT.sub("", l).strip()
            if not l: continue
        saida.append(l)
    return "\n".join(saida).strip()

# ── 3c. Extração de cupom / SKU ──────────────────────────────────────────────

_KW_CUPOM = re.compile(r'\b(?:cupom|cupon|c[oó]digo|coupon|resgate|cod)\b', re.I)
_KW_COD   = re.compile(r'\b([A-Z][A-Z0-9_-]{3,19})\b')
_FALSO_CUPOM = frozenset({
    "FRETE","GRÁTIS","GRATIS","AMAZON","SHOPEE","MAGALU","LINK","CLIQUE","ACESSE",
    "CONFIRA","HOJE","AGORA","PROMO","BLACK","SUPER","MEGA","ULTRA","VIP","NOVO",
    "NOVA","NUM","PRECO","PCT","PS5","PS4","XBOX","USB","ATX","RGB","LED","HD",
    "SSD","RAM","APP","BOT","API","URL","OK","BR","PIX","ASTRO","DIGITAL","SLIM",
    "GRAN","TURISMO","PACOTE","PLAYSTATION","NINTENDO","SONY","SAMSUNG","APPLE",
    "XIAOMI","PHILIPS","OSTER","MONDIAL","ARNO","BRAUN","LENOVO","LOGITECH",
    "NESTLÉ","NESTLE","ALPINO","PAMPERS","POSITIVO","INTELBRAS","LG","MALIBU",
    "OFF","VOLTA","ATIVO","VOLTOU","RENOVADO","NORMALIZOU",
})

def extrair_cupom(texto: str) -> str:
    for linha in texto.splitlines():
        if not _KW_CUPOM.search(linha): continue
        for m in _KW_COD.finditer(linha):
            c = m.group(1)
            if c not in _FALSO_CUPOM and len(c) >= 4:
                return c
    return ""

def _extrair_asin_texto(texto: str, mapa: dict) -> str:
    for u in list(mapa.values()) + [texto]:
        for pat in _P_AMZ_ASIN:
            m = pat.search(u)
            if m: return m.group(1).upper()
    return ""

def _extrair_id_magalu(texto: str, mapa: dict) -> str:
    for u in list(mapa.values()) + [texto]:
        m = _P_MGL.search(u)
        if m: return m.group(1)
    return ""

def tem_contexto(texto: str) -> bool:
    linhas = [l.strip() for l in texto.splitlines()
              if l.strip() and not re.match(r'https?://', l.strip())]
    if not linhas: return False
    total = " ".join(linhas)
    indicadores = [
        r'off', r'%', r'r\$', r'cupom', r'desconto', r'promoção', r'oferta',
        r'grátis', r'evento', r'live', r'relâmpago', r'flash', r'volta',
        r'normalizou', r'a\s+partir', r'ativo', r'disponivel', r'pix',
        r'voltando', r'reativado', r'jogos?\s+gr[aá]tis',
    ]
    for ind in indicadores:
        if re.search(ind, total, re.I): return True
    return len(total) > 20

# ── 3d. Sanitização ──────────────────────────────────────────────────────────

def _sanitizar_url(url: str) -> str:
    return url.strip().rstrip('.,;)>!?\n\r ')

# ── 3e. Caches raw + final + classificação ───────────────────────────────────

_raw_cache:   OrderedDict[str, str]              = OrderedDict()
_final_cache: OrderedDict[str, str]              = OrderedDict()
_cls_cache:   OrderedDict[str, LinkClassificado] = OrderedDict()
_cls_lock    = Lock()
_cache_lock  = Lock()
_CACHE_LIMIT = 5000

def _cache_key(url: str) -> str:
    try:
        p = urlparse(url.strip())
        host = (p.hostname or "").lower().strip(".")
        path = p.path.rstrip("/")
        params = parse_qs(p.query)
        remover_genericos = {
            "ascsubtag","smid","utm_source","utm_medium","utm_campaign",
            "utm_term","utm_content","aff_id","affiliate_id",
            "fbclid","gclid","camp","creative","linkcode","linkid",
        }
        params_limpos = {
            k: v for k, v in params.items()
            if k.lower() not in remover_genericos
        }
        pares = [(k, val) for k, vals in params_limpos.items() for val in vals]
        query = urlencode(sorted(pares))
        return urlunparse((p.scheme.lower(), host, path, "", query, ""))
    except Exception:
        return url.strip().lower()

def _set_raw(url: str, valor: str):
    key = _cache_key(url)
    with _cache_lock:
        _raw_cache[key] = valor
        _raw_cache.move_to_end(key)
        if len(_raw_cache) > _CACHE_LIMIT:
            _raw_cache.popitem(last=False)

def _set_final(url: str, valor: str):
    key = _cache_key(url)
    with _cache_lock:
        _final_cache[key] = valor
        _final_cache.move_to_end(key)
        if len(_final_cache) > _CACHE_LIMIT:
            _final_cache.popitem(last=False)

def _get_raw(url: str) -> Optional[str]:
    with _cache_lock:
        return _raw_cache.get(_cache_key(url))

def _get_final(url: str) -> Optional[str]:
    with _cache_lock:
        return _final_cache.get(_cache_key(url))

def _classificar_cached(url: str) -> LinkClassificado:
    key = _cache_key(url)
    with _cls_lock:
        lc = _cls_cache.get(key)
        if lc is not None:
            _cls_cache.move_to_end(key); return lc
    lc = classificar_url(url)
    with _cls_lock:
        _cls_cache[key] = lc
        _cls_cache.move_to_end(key)
        if len(_cls_cache) > _CACHE_LIMIT:
            _cls_cache.popitem(last=False)
    return lc

def _log_cache_stats():
    log_db.info(
        f"📦 Cache | raw={len(_raw_cache)} final={len(_final_cache)} "
        f"cls={len(_cls_cache)} db_links={_db_count_links()}"
    )

def _db_count_links() -> int:
    try:
        with _db() as db:
            row = db.execute("SELECT COUNT(*) FROM links_cache").fetchone()
            return row[0] if row else 0
    except Exception:
        return 0

# ── 3f. Desencurtador reforçado ──────────────────────────────────────────────

# cutt.ly SEMPRE via GET — nunca HEAD
_FORCA_GET = frozenset({
    "amzlink.to","amzn.to","a.co","amzn.com","bit.ly","cutt.ly",
    "tinyurl.com","rb.gy","is.gd","ow.ly","buff.ly","maga.lu",
})

# Domínios Magalu conhecidos — usados para validar após desencurtar
_MGL_DOMINIOS_SET = frozenset({
    "magazineluiza.com.br","sacola.magazineluiza.com.br",
    "magazinevoce.com.br","maga.lu","divulgador.magalu.com",
    "m.magazineluiza.com.br",
})

def _eh_magalu_url(url: str) -> bool:
    nl = _netloc(url)
    return any(nl == d or nl.endswith("." + d) for d in _MGL_DOMINIOS_SET)

async def desencurtar(url: str, sessao: aiohttp.ClientSession, depth: int = 0) -> str:
    """
    Desencurtador reforçado.
    cutt.ly: sempre GET com follow_redirects, extrai Location header se precisar.
    Magalu encurtado: segue até destrinchar o domínio final.
    Anti-loop: para se depth > 15 ou se voltou para cutt.ly após redirect.
    """
    if depth > 15: return url
    url = _sanitizar_url(url)
    if not url.startswith(("http://", "https://")): return url
    nl = _netloc(url)
    # Anti-loop: cutt.ly só é processado na primeira chamada
    if depth > 0 and nl == "cutt.ly": return url
    cached = _get_raw(url)
    if cached: return cached

    hdrs = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }

    try:
        # cutt.ly e _FORCA_GET: sempre GET direto
        usar_head = nl not in _FORCA_GET and not any(
            nl.endswith("." + d) for d in _FORCA_GET)

        if usar_head:
            try:
                async with sessao.head(url, headers=hdrs, allow_redirects=True,
                                       timeout=aiohttp.ClientTimeout(total=10),
                                       max_redirects=20) as r:
                    final = str(r.url)
                    if final != url:
                        _set_raw(url, final)
                        return await desencurtar(final, sessao, depth + 1)
            except Exception:
                pass

        # GET completo
        async with sessao.get(url, headers=hdrs, allow_redirects=True,
                              timeout=aiohttp.ClientTimeout(total=20),
                              max_redirects=20) as r:
            pos  = str(r.url)
            # Redirect resolvido pelo aiohttp
            if pos != url:
                _set_raw(url, pos)
                return await desencurtar(pos, sessao, depth + 1)

            html = await r.text(errors="ignore")
            if len(html) > 500_000:
                _set_raw(url, pos); return pos

            soup = BeautifulSoup(html, "html.parser")

            # meta refresh
            ref = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)", ref["content"], re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    if novo.startswith("http"):
                        return await desencurtar(novo, sessao, depth + 1)

            # JS redirect
            for pat in [
                r'window\.location(?:\.href)?\s*=\s*["\']([^"\']{15,})["\']',
                r'location\.replace\s*\(\s*["\']([^"\']{15,})["\']\s*\)',
                r'location\.href\s*=\s*["\']([^"\']{15,})["\']',
            ]:
                mj = re.search(pat, html)
                if mj and mj.group(1).startswith("http"):
                    return await desencurtar(mj.group(1), sessao, depth + 1)

            # og:url
            og = soup.find("meta", attrs={"property": "og:url"})
            if og and og.get("content","").startswith("http") and og["content"] != url:
                return await desencurtar(og["content"], sessao, depth + 1)

            # canonical
            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href","").startswith("http") and canon["href"] != url:
                return await desencurtar(canon["href"], sessao, depth + 1)

            _set_raw(url, pos); return pos

    except asyncio.TimeoutError:
        log_nrm.warning(f"⏱ Timeout desencurtar d={depth}: {url[:60]}"); return url
    except Exception as e:
        log_nrm.error(f"❌ desencurtar d={depth}: {e}"); return url

# ── 3g. Limpeza de parâmetros ────────────────────────────────────────────────

_AMZ_MANTER = frozenset({"keywords","node","k","i","rh","n","field-keywords"})

def _limpar_params_amazon(p) -> dict:
    return {k: v[0] for k, v in parse_qs(p.query).items()
            if k.lower() in _AMZ_MANTER and len(v[0]) < 60}

def _limpar_params_shopee(url: str) -> str:
    try:
        p = urlparse(url)
        params = {k: v[0] for k, v in parse_qs(p.query).items()
                  if k in {"shopid","itemid","smtt"}}
        return urlunparse(p._replace(
            query=urlencode(params) if params else "", fragment=""))
    except Exception:
        return url

def _limpar_params_magalu(params: dict) -> dict:
    remover = {
        "partnerid","promoterid","afforcedeeplink","deeplinkvalue",
        "partner_id","promoter_id","utm_source","utm_medium","utm_campaign",
        "pid","c","af_force_deeplink","deep_link_value","isretargeting",
    }
    return {k: v for k, v in params.items() if k not in remover}

# ── 3h. Motores de afiliação ─────────────────────────────────────────────────

def _limpar_url_amazon(url: str) -> Optional[str]:
    """Limpa e aplica tag. tag é EXCLUSIVO da Amazon."""
    try:
        p    = urlparse(url)
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
        return urlunparse(p._replace(
            scheme="https", netloc=p.netloc,
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
    if nl in {"amzn.to","a.co","amzn.com","amzlink.to"}:
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
        if _AMZ_PATHS_SEM_TAG.match(p.path):
            final = urlunparse(p._replace(query="", fragment=""))
        else:
            final = f"{url.split('?',1)[0]}?tag={_AMZ_TAG}"
    if not final or "amazon" not in _netloc(final):
        log_nrm.warning(f"  ⚠️ AMZ validação falhou: {final}"); return None
    _set_final(url, final)
    db_set_link(url, final, "amazon")
    log_nrm.info(f"  ✅ AMZ: {final[:70]}")
    return final

_SHP_REPASSE_DIRETO = frozenset({"flapremios.com.br"})

async def _expandir_shopee(url: str, sessao: aiohttp.ClientSession) -> str:
    """
    Expande QUALQUER link Shopee antes de chamar a API.
    s.shopee.com.br, shope.ee e encurtadores → GET completo.
    Resolve erro 11001 invalid origin url.
    """
    nl = _netloc(url)
    precisa_expandir = (
        nl in {"s.shopee.com.br", "shope.ee", "s.shopee.com"}
        or nl in _ENCURTADORES
        or nl in _FORCA_GET
    )
    if precisa_expandir:
        try:
            async with _SEM_HTTP:
                expandida = await desencurtar(url, sessao)
                log_nrm.debug(f"  SHP expandida: {expandida[:80]}")
                return expandida
        except Exception as e:
            log_nrm.warning(f"  ⚠️ SHP expandir falhou: {e}")
    return url

def _extrair_url_produto_shopee(url: str) -> Optional[str]:
    """Extrai URL canônica do produto para fallback de afiliação."""
    try:
        p = urlparse(url)
        for pat in _P_SHP:
            m = pat.search(p.path + "?" + p.query)
            if m:
                return f"https://shopee.com.br/product/{m.group(1)}/{m.group(2)}"
    except Exception:
        pass
    return None

async def _chamar_api_shopee(
    url_produto: str, sessao: aiohttp.ClientSession
) -> Optional[str]:
    """
    Chama API Shopee com retry inteligente (3 tentativas).
    Não desiste na primeira falha.
    """
    for tentativa in range(1, 4):
        try:
            ts      = str(int(time.time()))
            payload = json.dumps({
                "query": (
                    f'mutation {{ generateShortLink(input: '
                    f'{{ originUrl: "{url_produto}" }}) {{ shortLink }} }}'
                )
            }, separators=(",", ":"))
            sig = hashlib.sha256(
                f"{_SHP_APP_ID}{ts}{payload}{_SHP_SECRET}".encode()
            ).hexdigest()
            hdrs = {
                "Authorization": (
                    f"SHA256 Credential={_SHP_APP_ID},"
                    f"Timestamp={ts},Signature={sig}"
                ),
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
                    erros = res.get('errors') or res.get('error')
                    log_nrm.warning(
                        f"  ⚠️ SHP API t={tentativa}: {erros}"
                    )
        except Exception as e:
            log_nrm.warning(f"  ⚠️ SHP t={tentativa}: {e}")
        if tentativa < 3:
            await asyncio.sleep(tentativa * 1.5)
    return None

async def _afiliar_shopee(
    url: str, sessao: aiohttp.ClientSession
) -> Optional[str]:
    """
    Fluxo Shopee:
    1. Repasse direto (flapremios)
    2. Cache
    3. Expandir SEMPRE (resolve erro 11001)
    4. Limpeza leve
    5. API com retry 3x
    6. Fallback: URL canônica do produto
    7. Descarta se todas falharem (nunca envia sem afiliação)
    """
    url = _sanitizar_url(url)
    log_nrm.debug(f"▶ SHP: {url[:80]}")

    nl = _netloc(url)
    for d in _SHP_REPASSE_DIRETO:
        if nl == d or nl.endswith("." + d):
            log_nrm.info(f"  ↩️ SHP repasse direto: {url[:60]}")
            return url

    cached = _get_final(url)
    if cached: return cached
    cached = db_get_link(url)
    if cached: return cached

    # Passo 3: expandir SEMPRE antes da API
    url_expandida = await _expandir_shopee(url, sessao)

    # Passo 4: limpeza leve
    url_limpa = _limpar_params_shopee(url_expandida)

    # Passo 5: API com retry
    log_nrm.debug(f"  SHP → API: {url_limpa[:80]}")
    link = await _chamar_api_shopee(url_limpa, sessao)

    # Passo 6: fallback URL canônica
    if not link:
        url_prod = _extrair_url_produto_shopee(url_expandida)
        if url_prod and url_prod != url_limpa:
            log_nrm.info(f"  🔄 SHP fallback produto: {url_prod[:60]}")
            link = await _chamar_api_shopee(url_prod, sessao)

    # Passo 7: descarta — nunca envia sem afiliação
    if not link:
        log_nrm.warning(
            f"  ❌ SHP sem afiliação após 3 tentativas → descarta: {url[:60]}"
        )
        return None

    if "shopee" not in _netloc(link):
        log_nrm.warning(f"  ⚠️ SHP validação falhou: {link}")
        return None

    _set_final(url, link)
    db_set_link(url, link, "shopee")
    return link

# ═══════════════════════════════════════════════════════════════════════════════
# ENCURTADOR PRÓPRIO — exclusivo Magalu
# Formato: https://leoind.com.br/XXXXXXX-magalu
# REGRA: só encurta DEPOIS de aplicar afiliação — nunca antes
# ═══════════════════════════════════════════════════════════════════════════════

_SHORT_BASE = f"https://{os.environ.get('RAILWAY_PUBLIC_DOMAIN', 'leoind.com.br')}"

def _gerar_code_magalu(url_afiliada: str) -> str:
    """Código estável baseado na URL já afiliada."""
    return hashlib.sha256(url_afiliada.encode()).hexdigest()[:7]

async def _encurtador_proprio_magalu(url_afiliada: str) -> Optional[str]:
    """
    Encurtador próprio exclusivo para Magalu.
    Só aceita URL que já tenha os parâmetros de afiliado corretos.
    Salva no SQLite e retorna link curto.
    NUNCA é chamado antes de _afiliar_url_magalu().
    """
    # Validação: garante que é URL afiliada com IDs corretos
    if _MGL_PARTNER not in url_afiliada:
        log_nrm.warning("  ⚠️ Encurtador próprio: partner_id ausente — recusado")
        return None
    if _MGL_PROMOTER not in url_afiliada:
        log_nrm.warning("  ⚠️ Encurtador próprio: promoter_id ausente — recusado")
        return None
    if "magalu" not in url_afiliada and "magazineluiza" not in url_afiliada:
        log_nrm.warning("  ⚠️ Encurtador próprio: não é URL Magalu — recusado")
        return None

    code = _gerar_code_magalu(url_afiliada)
    short = f"{_SHORT_BASE}/{code}-magalu"

    try:
        with _db() as db:
            db.execute(
                "INSERT OR IGNORE INTO short_links(code, url, ts) VALUES(?,?,?)",
                (code, url_afiliada, time.time())
            )
        log_nrm.info(f"  ✅ Encurtador próprio: {short}")
        return short
    except Exception as e:
        log_nrm.error(f"  ❌ Encurtador próprio: {e}")
        return None

async def _resolver_short_magalu(code: str) -> Optional[str]:
    """Resolve code → URL afiliada original. Usado pelo servidor de redirect."""
    try:
        with _db() as db:
            row = db.execute(
                "SELECT url FROM short_links WHERE code=?", (code,)
            ).fetchone()
        return row[0] if row else None
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════════════════════════
# MAGALU — fluxo completo em ordem obrigatória
# ═══════════════════════════════════════════════════════════════════════════════

def _afiliar_url_magalu(url: str) -> str:
    """
    Troca parâmetros de afiliado do grupo pelos do promotor.
    NÃO altera path nem estrutura do link.
    Sem tag — tag é exclusivo da Amazon.
    """
    p = urlparse(url)
    params = {
        k: v[0]
        for k, v in parse_qs(p.query, keep_blank_values=True).items()
    }
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

async def _afiliar_magalu(
    url: str, sessao: aiohttp.ClientSession, msg_id: int = 0
) -> Optional[str]:
    """
    Fluxo Magalu em ordem obrigatória:
    1. Cache
    2. Desencurtar (cutt.ly / maga.lu / encurtadores)
    3. Validar que é Magalu após expansão
    4. Aplicar afiliação ← OBRIGATÓRIO antes de qualquer encurtamento
    5. Encurtador próprio (leoind.com.br/XXXX-magalu)
    6. Fallback: Cuttly
    7. Fallback final: longo afiliado + background
    """
    url = _sanitizar_url(url)
    log_nrm.debug(f"▶ MGL entrada: {url[:80]}")

    # 1. Cache
    cached = _get_final(url)
    if cached:
        log_nrm.debug(f"  MGL cache hit: {cached[:60]}")
        return cached
    cached = db_get_link(url)
    if cached:
        log_nrm.debug(f"  MGL db hit: {cached[:60]}")
        return cached

    nl = _netloc(url)

    # 2. Desencurtar se necessário
    if nl == "cutt.ly" or nl == "maga.lu" or nl in _ENCURTADORES:
        log_nrm.debug(f"  MGL desencurtando: {url[:60]}")
        try:
            async with _SEM_HTTP:
                url_exp = await desencurtar(url, sessao)
            log_nrm.debug(f"  MGL expandido: {url_exp[:80]}")
            # 3. Valida que é Magalu após expansão
            if not _eh_magalu_url(url_exp):
                log_nrm.warning(
                    f"  MGL pós-expand não é Magalu: {_netloc(url_exp)} — descarta"
                )
                return None
            url = url_exp
        except Exception as e:
            log_nrm.error(f"  ❌ MGL desencurtar: {e}")
            return None

    # 3. Valida plataforma
    cl = _classificar_cached(url)
    if cl.plat != "magalu" or cl.tipo == "invalido":
        log_nrm.warning(f"  MGL descartado: plat={cl.plat} tipo={cl.tipo}")
        return None

    # 4. Aplica afiliação — NUNCA encurta antes daqui
    afiliado = _afiliar_url_magalu(url)
    if not afiliado or (
        "magalu" not in afiliado and "magazineluiza" not in afiliado
    ):
        log_nrm.warning("  ⚠️ MGL afiliação inválida")
        return None
    log_nrm.debug(f"  MGL afiliado: {afiliado[:80]}")

    # 5. Encurtador próprio (leoind.com.br)
    short = await _encurtador_proprio_magalu(afiliado)
    if short:
        _set_final(url, short)
        db_set_link(url, short, "magalu")
        log_nrm.info(f"  ✅ MGL próprio: {short}")
        return short

    # 6. Fallback: Cuttly
    short = await _cuttly(afiliado, sessao)
    if short:
        _set_final(url, short)
        db_set_link(url, short, "magalu")
        log_nrm.info(f"  ✅ MGL Cuttly: {short}")
        return short

    # 7. Fallback final: longo afiliado + tenta encurtar depois
    log_nrm.warning("  ⚠️ MGL encurtadores falharam → longo afiliado")
    _set_final(url, afiliado)
    db_set_link(url, afiliado, "magalu")
    if msg_id:
        asyncio.create_task(_cuttly_background(afiliado, msg_id))
    return afiliado                    

# ── 3i. Pipeline principal ───────────────────────────────────────────────────

async def _normalizar_um(
    lc: LinkClassificado,
    sessao: aiohttp.ClientSession,
    msg_id: int = 0,
) -> Tuple[str, Optional[str], str]:
    plat = lc.plat
    if plat == "mundial":   return lc.url_original, lc.url_original, "mundial"
    if plat == "preservar": return lc.url_original, lc.url_original, "preservar"
    if plat is None or lc.tipo in ("invalido","bloqueado","grupo_externo","desconhecido"):
        return lc.url_original, None, plat or "none"
    if plat == "amazon" and lc.tipo == "claims":
        return lc.url_original, lc.url_original, "amazon"
    cached = _get_final(lc.url_original)
    if cached: return lc.url_original, cached, plat
    cached = db_get_link(lc.url_original)
    if cached: return lc.url_original, cached, plat
    url = lc.url_original
    if plat == "expandir":
        try: url = await desencurtar(url, sessao)
        except Exception: return lc.url_original, None, "none"
        lc   = _classificar_cached(url)
        plat = lc.plat
        if plat is None: return lc.url_original, None, "none"
        if plat == "mundial": return lc.url_original, url, "mundial"
        if plat == "amazon" and lc.tipo == "claims": return lc.url_original, url, "amazon"
        cached = _get_final(url)
        if cached: return lc.url_original, cached, plat
        cached = db_get_link(url)
        if cached: return lc.url_original, cached, plat
    if plat == "amazon":
        convertido = await _afiliar_amazon(url, sessao)
    elif plat == "shopee":
        convertido = await _afiliar_shopee(url, sessao)
    elif plat == "magalu":
        convertido = await _afiliar_magalu(url, sessao, msg_id)
    else:
        convertido = None
    return lc.url_original, convertido, plat

# ── 3j. Estado do evento ─────────────────────────────────────────────────────

from enum import Enum

class EstadoEvento(Enum):
    NEW       = "new"
    SEEN      = "seen"
    EXPIRED   = "expired"
    RESTOCKED = "restocked"

_RE_RESTOCK_C3 = re.compile(
    r'voltou|restock|reativado|dispon[ií]vel\s+novamente|'
    r'voltou\s+ao\s+estoque|de\s+volta|ativo\s+novamente|'
    r'normalizou|voltando|voltou\s+cupom|relançamento',
    re.I
)
_JANELA_C3: Dict[str, float] = {
    "shopee": 60.0, "amazon": 300.0,
    "magalu": 300.0, "default": 120.0,
}
_TTL_RESTOCK_C3: Dict[str, float] = {
    "shopee": 3600.0, "amazon": 7200.0,
    "magalu": 14400.0, "default": 3600.0,
}

def _fp_c3(id_global: str, plat: str) -> str:
    return hashlib.sha256(f"{plat}|{id_global}".encode()).hexdigest()[:32]

def detectar_estado_evento(texto: str, id_global: str, plat: str) -> EstadoEvento:
    eh_restock = bool(_RE_RESTOCK_C3.search(texto))
    entrada    = db_get_dedupe(_fp_c3(id_global, plat))
    if not entrada:
        return EstadoEvento.NEW
    ts_ant = entrada.get("ts", 0)
    delta  = time.time() - ts_ant
    janela = _JANELA_C3.get(plat, _JANELA_C3["default"])
    ttl    = _TTL_RESTOCK_C3.get(plat, _TTL_RESTOCK_C3["default"])
    if delta < janela:
        return EstadoEvento.SEEN
    if eh_restock:
        return EstadoEvento.RESTOCKED
    if delta > ttl:
        return EstadoEvento.EXPIRED
    return EstadoEvento.SEEN

# ── 3k. MensagemNormalizada ───────────────────────────────────────────────────

@dataclass
class MensagemNormalizada:
    msg_id:        int
    chat:          str
    texto_limpo:   str
    mapa:          Dict[str, str]
    preservar:     List[str]
    plat:          str
    cupom:         str
    sku:           str
    tem_midia:     bool
    media_obj:     object
    estado_evento: EstadoEvento = EstadoEvento.NEW
    ids_globais:   List[str]    = field(default_factory=list)

# ── 3l. normalizar() ─────────────────────────────────────────────────────────

async def normalizar(bruta: MensagemBruta) -> Optional[MensagemNormalizada]:
    if not bruta.texto.strip(): return None
    if texto_bloqueado(bruta.texto): return None
    texto_limpo = limpar_texto(bruta.texto)
    if not tem_contexto(texto_limpo): return None
    classificados = classificar_links(bruta.links)
    converter     = [lc for lc in classificados if lc.plat not in ("preservar", None)]
    preservar_lst = [lc.url_original for lc in classificados if lc.plat == "preservar"]
    if not converter and not preservar_lst:
        if "fadadoscupons" not in bruta.chat: return None
    conn = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False)
    async with aiohttp.ClientSession(
        connector=conn,
        timeout=aiohttp.ClientTimeout(total=40, connect=8),
        headers={"User-Agent": random.choice(USER_AGENTS)},
    ) as sessao:
        resultados = await asyncio.gather(
            *[_normalizar_um(lc, sessao, bruta.msg_id) for lc in converter[:50]],
            return_exceptions=True,
        )
    mapa:  Dict[str, str] = {}
    plats: List[str]      = []
    for res in resultados:
        if isinstance(res, Exception):
            log_nrm.error(f"❌ normalizar link: {res}"); continue
        orig, conv, plat = res
        if conv and plat not in ("none", None):
            mapa[orig] = conv
            if plat not in ("mundial", "preservar"):
                plats.append(plat)
    if converter and not mapa and not preservar_lst:
        log_nrm.warning(f"🚫 Zero links convertidos | @{bruta.chat}")
        return None
    plat_dom = max(set(plats), key=plats.count) if plats else "amazon"
    cupom    = extrair_cupom(texto_limpo)
    sku      = (
        next((f"{lc.plat[:3]}_{lc.sku}" for lc in classificados if lc.sku), "")
        or _extrair_asin_texto(texto_limpo, mapa)
        or _extrair_id_magalu(texto_limpo, mapa)
    )

# Coleta ids_globais
ids_globais: List[str] = []

for orig in mapa:
    lc = _classificar_cached(orig)
    if lc.sku and lc.sku not in ids_globais:
        ids_globais.append(lc.sku)

if sku and sku not in ids_globais:
    ids_globais.append(sku)

# Detecta estado do evento
estado = EstadoEvento.NEW

if ids_globais:
    estado = detectar_estado_evento(
        texto_limpo,
        ids_globais[0],
        plat_dom
    )

elif cupom:
    fp_cup = _fp_c3(f"cup_{cupom}", plat_dom)
    entrada = db_get_dedupe(fp_cup)

    if entrada:
        delta = time.time() - entrada.get("ts", 0)
        janela = _JANELA_C3.get(plat_dom, 120.0)
        estado = (
            EstadoEvento.SEEN
            if delta < janela
            else EstadoEvento.EXPIRED
        )

log_nrm.info(
    f"✅ {len(mapa)}/{len(converter)} | "
    f"plat={plat_dom} cupom='{cupom}' sku={sku} "
    f"estado={estado.value} ids={ids_globais}"
)

_log_cache_stats()

return MensagemNormalizada(
    msg_id=bruta.msg_id,
    chat=bruta.chat,
    texto_limpo=texto_limpo,
    mapa=mapa,
    preservar=preservar_lst,
    plat=plat_dom,
    cupom=cupom,
    sku=sku,
    tem_midia=bruta.tem_midia,
    media_obj=bruta.media_obj,
    estado_evento=estado,
    ids_globais=ids_globais,
    )            
            
# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 4 — DEDUPLICAÇÃO  (nível sênior)
# ═══════════════════════════════════════════════════════════════════════════════

_RUIDO_NORM = frozenset({
    "promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora",
    "imperdivel","imperdível","exclusivo","limitado","corra","ative","use",
    "saiu","vazou","resgate","acesse","confira","link","clique","app",
    "relampago","relâmpago","click","veja","novo","nova","valido","válido",
    "somente","apenas","ate","até","partir","ainda","volta","ativo",
    "disponivel","disponível","pix","parcelas","unidades","estoque",
    "shopee","amazon","magalu","magazineluiza","magazine",
})
_RE_EMJ_NORM = re.compile(r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF]+", re.UNICODE)
_SIM_FORTE   = 0.82   # era 0.88 — mais restritivo
_SIM_MEDIO   = 0.70   # era 0.75

def _fp(s: str) -> str: return hashlib.sha256(s.encode()).hexdigest()[:32]

def _rm_acentos(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", t)
                   if unicodedata.category(c) != "Mn")

def _alma(t: str) -> str:
    t = _rm_acentos(t.lower())
    t = re.sub(r'https?://\S+', ' ', t)
    t = _RE_EMJ_NORM.sub(' ', t)
    t = re.sub(r'(\d+\s?(gb|tb|mah|v|w|hz|fps))', r' ATTR_\1 ', t)
    t = re.sub(r'r\$\s*[\d.,]+', ' VALOR ', t)
    t = re.sub(r'\b\d+%', ' PCT ', t)
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return ' '.join(sorted(
        w for w in t.split()
        if w not in _RUIDO_NORM and (len(w) > 2 or "attr_" in w)
    ))

def _cupons(t: str) -> frozenset:
    return frozenset(re.findall(r'\b([A-Z0-9_-]{4,20})\b', t))

def _benef(t: str) -> frozenset:
    b = set()
    if re.search(r'frete\s+gr[aá]t', t, re.I): b.add("frete_gratis")
    for m in re.findall(r'(\d+)\s*%?\s*off', t, re.I): b.add(f"off_{m}")
    # Captura R$ X OFF em R$ Y
    for m in re.findall(r'r\$\s*([\d.,]+)\s*off', t, re.I):
        b.add(f"valor_off_{m.replace('.','').replace(',','')}")
    return frozenset(b)

def _sim(a: str, b: str) -> float:
    if not a or not b: return 0.0
    if min(len(a), len(b)) / max(len(a), len(b)) < 0.6: return 0.0
    return SequenceMatcher(None, a, b).ratio()

def _eh_reativacao(t: str) -> bool:
    return bool(re.search(
        r'voltou|reativado|dispon[ií]vel\s+novamente|estoque|voltando|normalizou',
        t, re.I))

def _normalizar_valor(t: str) -> str:
    """Extrai e normaliza valores monetários para comparação."""
    vals = re.findall(r'r\$\s*([\d.,]+)', t, re.I)
    return "|".join(sorted(v.replace('.','').replace(',','.') for v in vals))

# ── Score evolutivo ───────────────────────────────────────────────────────────

def calcular_score(norm) -> int:
    """
    Calcula score de riqueza da mensagem.
    Quanto maior, mais informação útil para o usuário.
    Usado para decidir se edita mensagem já enviada.
    """
    texto = norm.texto_limpo
    score = 0

    # Link de produto presente
    if norm.mapa:                                          score += 3
    # Preço explícito
    if re.search(r'r\$\s*[\d.,]+', texto, re.I):          score += 2
    # Cupom com código
    if norm.cupom:                                         score += 2
    # Desconto percentual
    if re.search(r'\d+\s*%\s*off', texto, re.I):          score += 2
    # Valor de desconto explícito
    if re.search(r'r\$\s*[\d.,]+\s*off', texto, re.I):    score += 2
    # Mínimo de compra
    if re.search(r'(acima|mínimo|min)\s+de\s+r\$', texto, re.I): score += 1
    # Frete grátis
    if re.search(r'frete\s+gr[aá]t', texto, re.I):        score += 1
    # Imagem disponível
    if norm.tem_midia:                                     score += 1
    # Tem SKU identificado
    if norm.sku:                                           score += 1

    return score

def identidade_canonica(norm) -> str:
    """
    Gera chave única e estável para a oferta.
    Mesma oferta = mesma chave, independente de variação de texto ou link.
    """
    # Prioridade: ID estruturado > cupom > texto normalizado
    if norm.ids_globais:
        return f"{norm.plat}|{norm.ids_globais[0]}"
    if norm.cupom:
        return f"{norm.plat}|cup|{norm.cupom}"
    # Fallback: hash do texto normalizado
    alma_v = _alma(norm.texto_limpo)
    return f"{norm.plat}|txt|{_fp4(alma_v)}"

async def deve_enviar_async(norm) -> bool:
    """Versão assíncrona — usa bloqueio atômico para concorrência real."""
    try:
        texto       = norm.texto_limpo
        plat        = norm.plat
        estado      = norm.estado_evento
        ids_globais = norm.ids_globais
        cupons      = _cupons_set(texto)
        alma_v      = _alma(texto)
        benef       = _benef_set(texto)
        valores     = _normalizar_valor(texto)
        janela      = _janela(plat)

        if estado == EstadoEvento.RESTOCKED:
            if ids_globais:
                fp_rst = _fp4(f"{plat}|{ids_globais[0]}|restock|{int(time.time()//60)}")
                ok = await _atomic_claim(fp_rst)
                if not ok:
                    log_ded.info(f"🔁 [BLOQUEADO_DUPLICADO] RESTOCK RACE {ids_globais[0]}")
                    return False
                db_set_dedupe(_fp4(f"{plat}|{ids_globais[0]}"), plat,
                              list(cupons), alma_v, "restock",
                              ids_globais[0], ids_globais[0], list(benef))
                log_ded.info(f"♻️ [PASSOU_RESTOCK] {ids_globais[0]} plat={plat}")
                return True
            return True

        if estado == EstadoEvento.SEEN:
            log_ded.info(f"🔁 [BLOQUEADO_JANELA] SEEN ids={ids_globais} plat={plat}")
            return False

        if estado == EstadoEvento.EXPIRED:
            if ids_globais:
                fp_base = _fp4(f"{plat}|{ids_globais[0]}")
                entrada = db_get_dedupe(fp_base)
                if entrada:
                    benef_ant = frozenset(entrada.get("benef", []))
                    if benef and benef != benef_ant:
                        fp_ben = _fp_benef(ids_globais[0], plat, benef)
                        ok = await _atomic_claim(fp_ben)
                        if not ok: return False
                        db_set_dedupe(fp_base, plat, list(cupons), alma_v, "benef",
                                      ids_globais[0], ids_globais[0], list(benef))
                        log_ded.info(f"✳️ [PASSOU_BENEFICIO_NOVO] {ids_globais[0]}")
                        return True
            return False

        for id_global in ids_globais:
            fp = _fp4(f"{plat}|{id_global}")
            ts_mem = await _atomic_check(fp)
            if ts_mem is not None:
                if (time.monotonic() - ts_mem) < janela:
                    log_ded.info(f"🔁 [BLOQUEADO_JANELA] MEM {id_global}")
                    return False
            ok = await _atomic_claim(fp)
            if not ok:
                log_ded.info(f"🔁 [BLOQUEADO_DUPLICADO] ATOMIC RACE {id_global}")
                return False
            entrada_db = db_get_dedupe(fp)
            if entrada_db:
                delta = time.time() - entrada_db.get("ts", 0)
                if delta < janela:
                    await _atomic_release(fp)
                    log_ded.info(f"🔁 [BLOQUEADO_JANELA] DB {id_global}")
                    return False
                benef_ant = frozenset(entrada_db.get("benef", []))
                if benef and benef != benef_ant:
                    fp_ben = _fp_benef(id_global, plat, benef)
                    ok_ben = await _atomic_claim(fp_ben)
                    if not ok_ben:
                        await _atomic_release(fp)
                        return False
                    db_set_dedupe(fp, plat, list(cupons), alma_v, "benef",
                                  id_global, id_global, list(benef))
                    log_ded.info(f"✳️ [PASSOU_BENEFICIO_NOVO] {id_global}")
                    return True
                await _atomic_release(fp)
                return False
            db_set_dedupe(fp, plat, list(cupons), alma_v, "id",
                          id_global, id_global, list(benef))
            log_ded.info(f"✅ [PASSOU_NEW] {id_global} plat={plat}")
            return True

        if norm.cupom:
            fp_cup = _fp4(f"{plat}|cup|{norm.cupom}")
            ts_mem = await _atomic_check(fp_cup)
            if ts_mem is not None and (time.monotonic() - ts_mem) < janela:
                log_ded.info(f"🔁 [BLOQUEADO_JANELA] CUP MEM {norm.cupom}")
                return False
            ok = await _atomic_claim(fp_cup)
            if not ok:
                log_ded.info(f"🔁 [BLOQUEADO_DUPLICADO] CUP RACE {norm.cupom}")
                return False
            entrada_db = db_get_dedupe(fp_cup)
            if entrada_db:
                delta = time.time() - entrada_db.get("ts", 0)
                if delta < janela:
                    await _atomic_release(fp_cup)
                    log_ded.info(f"🔁 [BLOQUEADO_JANELA] CUP DB {norm.cupom}")
                    return False
                vals_ant = _normalizar_valor(entrada_db.get("alma", ""))
                if valores and valores != vals_ant:
                    db_set_dedupe(fp_cup, plat, list(cupons), alma_v, "cup_val",
                                  "", "", list(benef))
                    log_ded.info(f"✳️ [PASSOU_BENEFICIO_NOVO] CUP {norm.cupom}")
                    return True
                await _atomic_release(fp_cup)
                return False
            db_set_dedupe(fp_cup, plat, list(cupons), alma_v, "cup",
                          "", "", list(benef))
            log_ded.info(f"✅ [PASSOU_NEW] CUP {norm.cupom} plat={plat}")
            return True

        fp_txt = _fp4(f"{plat}|{alma_v}|{'|'.join(sorted(benef))}|{valores}")
        ok = await _atomic_claim(fp_txt)
        if not ok:
            log_ded.info(f"🔁 [BLOQUEADO_DUPLICADO] TEXTO RACE plat={plat}")
            return False
        for e in db_buscar_janela_rapida(plat, janela=max(janela, 900)):
            alma_ant = e.get("alma", "")
            if not alma_ant: continue
            sim_v    = _sim(alma_v, alma_ant)
            if sim_v > _SIM_FORTE:
                await _atomic_release(fp_txt)
                log_ded.info(f"🔁 [BLOQUEADO_DUPLICADO] SIM {sim_v:.2f}")
                return False
        db_set_dedupe(fp_txt, plat, list(cupons), alma_v, "gen", "", "", list(benef))
        log_ded.info(f"✅ [PASSOU_NEW] TEXTO plat={plat}")
        return True

    except Exception as e:
        log_ded.error(f"❌ ERRO DEDUPE: {e}"); return True


def deve_enviar(norm) -> bool:
    """Wrapper síncrono — compatibilidade apenas."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            log_ded.warning("⚠️ deve_enviar() em loop — use deve_enviar_async()")
            return True
        return loop.run_until_complete(deve_enviar_async(norm))
    except Exception:
        return True

def deve_enviar(norm: MensagemNormalizada) -> bool:
    try:
        texto  = norm.texto_limpo
        plat   = norm.plat
        cupons = _cupons(texto)
        alma_v = _alma(texto)
        benef  = _benef(texto)
        asin   = _extrair_asin_texto(texto, norm.mapa)
        id_mgl = _extrair_id_magalu(texto, norm.mapa)
        valores = _normalizar_valor(texto)

        # ── Reativação ──────────────────────────────────────────────
        if _eh_reativacao(texto):
            fp_re = _fp(f"reativ_{plat}_{'|'.join(sorted(cupons))}_{asin}_{id_mgl}")
            if db_get_dedupe(fp_re):
                log_ded.info("🔁 [REATIVAÇÃO BLOQ]"); return False
            db_set_dedupe(fp_re, plat, list(cupons), alma_v, "reativ",
                          asin, id_mgl, list(benef))
            return True

        # ── Amazon: ASIN único ──────────────────────────────────────
        if asin:
            fp = f"amz_{asin}"
            entrada = db_get_dedupe(fp)
            if entrada:
                # Mesmo ASIN mas benefícios diferentes → deixa passar
                benef_ant = frozenset(entrada.get("benef", []))
                if benef and benef != benef_ant:
                    log_ded.info(f"  AMZ ASIN {asin} novo benefício → passa")
                    db_set_dedupe(fp, plat, list(cupons), alma_v, "amz",
                                  asin, "", list(benef))
                    return True
                log_ded.info(f"🔁 [AMZ-ASIN] {asin}"); return False
            db_set_dedupe(fp, plat, list(cupons), alma_v, "amz",
                          asin, "", list(benef))
            return True

        # ── Magalu: ID produto único ────────────────────────────────
        if id_mgl:
            fp = f"mgl_{id_mgl}"
            entrada = db_get_dedupe(fp)
            if entrada:
                benef_ant = frozenset(entrada.get("benef", []))
                if benef and benef != benef_ant:
                    log_ded.info("  MGL ID novo benefício → passa")
                    db_set_dedupe(fp, plat, list(cupons), alma_v, "mgl",
                                  "", id_mgl, list(benef))
                    return True
                log_ded.info("🔁 [MGL-ID]"); return False
            db_set_dedupe(fp, plat, list(cupons), alma_v, "mgl",
                          "", id_mgl, list(benef))
            return True

        # ── Cupom exato ─────────────────────────────────────────────
        fp_cup = None
        if cupons:
            fp_cup = _fp(f"{plat}|cup|{'|'.join(sorted(cupons))}")
            entrada = db_get_dedupe(fp_cup)
            if entrada:
                # Mesmo cupom mas valor diferente → passa
                vals_ant = _normalizar_valor(entrada.get("alma",""))
                if valores and valores != vals_ant:
                    log_ded.info(f"  Cupom {plat} novo valor → passa")
                else:
                    log_ded.info(f"🔁 [CUPOM BLOQ] {plat}"); return False

        # ── Benefício exato ─────────────────────────────────────────
        fp_ben = None
        if benef:
            fp_ben = _fp(f"{plat}|ben|{'|'.join(sorted(benef))}")
            if db_get_dedupe(fp_ben):
                log_ded.info("🔁 [BENEF BLOQ]"); return False

        # ── Similaridade na janela rápida ───────────────────────────
        for e in db_buscar_janela_rapida(plat, janela=900):  # 15 min
            alma_ant = e.get("alma","")
            if not alma_ant: continue
            sim_v = _sim(alma_v, alma_ant)
            cupons_ant = set(e.get("cupons",[]))
            # Bloqueia se: alta sim OU (cupom em comum + sim média)
            if sim_v > _SIM_FORTE:
                log_ded.info(f"🔁 [SIM {sim_v:.2f}] {plat}"); return False
            if cupons and cupons & cupons_ant and sim_v > _SIM_MEDIO:
                log_ded.info(f"🔁 [SIM+CUP {sim_v:.2f}] {plat}"); return False

        # ── Registrar e enviar ──────────────────────────────────────
        fp_final = _fp(
            f"{plat}|{alma_v}|{'|'.join(sorted(cupons))}"
            f"|{'|'.join(sorted(benef))}|{valores}"
        )
        db_set_dedupe(fp_final, plat, list(cupons), alma_v, "gen",
                      asin, id_mgl, list(benef))
        if fp_cup: db_set_dedupe(fp_cup, plat, list(cupons), alma_v, "cup",
                                  asin, id_mgl, list(benef))
        if fp_ben: db_set_dedupe(fp_ben, plat, list(cupons), alma_v, "ben",
                                  asin, id_mgl, list(benef))
        return True

    except Exception as e:
        log_ded.error(f"❌ ERRO DEDUPE: {e}"); return True

# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 5 — MONTAGEM
# ═══════════════════════════════════════════════════════════════════════════════

_EMJ: Dict[str, List[str]] = {
    "titulo_oferta": ["🔥"],
    "titulo_cupom":  ["🚨"],
    "titulo_evento": ["⚠️"],
    "desconto":      ["🎟"],   # % OFF, R$ X OFF — em linha de desconto/cupom
    "preco_produto": ["💵"],   # preço de produto (não cupom)
    "resgate":       ["⭐"],   # resgate aqui, acesse aqui, lista
    "carrinho":      ["🛒"],
    "frete":         ["🚚", "📦"],
    "multi_item":    ["🔹"],
    "link_prod":     ["✅"],   # link produto / link oferta
}
_EMJ_IDX: Dict[str, int] = {k: 0 for k in _EMJ}

def _prox_emoji(cat: str) -> str:
    lst = _EMJ[cat]; idx = _EMJ_IDX[cat]; e = lst[idx % len(lst)]
    _EMJ_IDX[cat] = (idx + 1) % len(lst); return e

_KW_PRECO      = re.compile(r'R\$\s?[\d.,]+', re.I)
_KW_DESCONTO   = re.compile(r'\b(?:\d+\s*%\s*off|r\$\s*[\d.,]+\s*off|off\s*r\$|limite\s*r\$)\b', re.I)
_KW_FRETE      = re.compile(r'\b(?:frete\s+gr[aá]t|entrega\s+gr[aá]t|sem\s+frete|frete\s+0)\b', re.I)
_KW_RESGATE    = re.compile(r'\b(?:resgate|acesse|ative|lista|use\s+o\s+cupom)\b', re.I)
_KW_CARRINHO   = re.compile(r'\b(?:carrinho|cart)\b', re.I)
_KW_LINK_PROD  = re.compile(r'\b(?:link\s+produto|link\s+oferta|link\s+lista)\b', re.I)

_RE_LIXO_PREF  = re.compile(r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:|[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?)\s*', re.I)
_RE_ANUNCIO    = re.compile(r'^\s*[-#]?\s*(?:an[uú]ncio|publicidade|patrocinado)\s*$', re.I)
_RE_URL_RENDER = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c]+')

def _contar_produtos(texto: str) -> int:
    return sum(1 for l in texto.splitlines() if _KW_PRECO.search(l))

def _eh_linha_cupom(linha: str) -> bool:
    """Linha que descreve um cupom/desconto — NÃO é preço de produto."""
    return bool(_KW_DESCONTO.search(linha) or _KW_CUPOM.search(linha))

def _emoji_linha(linha: str, eh_titulo: bool, is_multi: bool = False) -> Optional[str]:
    if _tem_emoji(linha): return None
    if eh_titulo:
        if _KW_EVENTO.search(linha): return _prox_emoji("titulo_evento")
        if _KW_CUPOM.search(linha):  return _prox_emoji("titulo_cupom")
        return _prox_emoji("titulo_oferta")
    if is_multi and _KW_PRECO.search(linha): return "🔹"
    if _KW_FRETE.search(linha):     return _prox_emoji("frete")
    if _KW_CARRINHO.search(linha):  return _prox_emoji("carrinho")
    if _KW_LINK_PROD.search(linha): return _prox_emoji("link_prod")
    if _KW_RESGATE.search(linha):   return _prox_emoji("resgate")
    # Desconto/cupom → 🎟  |  preço puro de produto → 💵
    if _KW_PRECO.search(linha):
        if _eh_linha_cupom(linha): return _prox_emoji("desconto")
        return _prox_emoji("preco_produto")
    if _eh_linha_cupom(linha): return _prox_emoji("desconto")
    return None

def _crases(linha: str, eh_titulo: bool = False) -> str:
    if "http" in linha or eh_titulo: return linha
    if "`" in linha: return linha
    if not _KW_CUPOM.search(linha): return linha
    def _sub(m: re.Match) -> str:
        c = m.group(0)
        if c in _FALSO_CUPOM or len(c) < 4: return c
        return f"`{c}`"
    return re.sub(r'\b([A-Z][A-Z0-9_-]{4,20})\b', _sub, linha)

def montar_texto(norm: MensagemNormalizada) -> str:
    mapa    = {**norm.mapa, **{u: u for u in norm.preservar}}
    is_multi = _contar_produtos(norm.texto_limpo) >= 2
    saida: List[str] = []; primeiro = True
    for linha in norm.texto_limpo.split("\n"):
        l = linha.strip()
        if not l: saida.append(""); continue
        if _RE_ANUNCIO.match(l): saida.append(l); continue
        l = _RE_LIXO_PREF.sub("", l).strip()
        if not l: continue
        urls_na_linha = _RE_URL_RENDER.findall(l)
        sem_urls      = _RE_URL_RENDER.sub("", l).strip()
        if urls_na_linha and not sem_urls:
            for u in urls_na_linha:
                uc = u.rstrip('.,;)>')
                if uc in mapa: saida.append(mapa[uc])
            continue
        l = _RE_URL_RENDER.sub(
            lambda m: mapa.get(m.group(0).rstrip('.,;)>'), ""), l).strip()
        if not l: continue
        eh_titulo = primeiro
        l = _crases(l, eh_titulo=eh_titulo)
        if not _tem_emoji(l):
            e = _emoji_linha(l, eh_titulo=eh_titulo, is_multi=is_multi)
            if e: l = f"{e} {l}"
        primeiro = False; saida.append(l)
    return "\n".join(saida).strip()

@dataclass
class MensagemNormalizada:
    msg_id:        int
    chat:          str
    texto_limpo:   str
    mapa:          Dict[str, str]
    preservar:     List[str]
    plat:          str
    cupom:         str
    sku:           str
    tem_midia:     bool
    media_obj:     object
    estado_evento: EstadoEvento   = EstadoEvento.NEW
    ids_globais:   List[str]      = field(default_factory=list)

async def montar(norm: MensagemNormalizada) -> MensagemMontada:
    texto  = montar_texto(norm)
    imagem = await _resolver_imagem(norm)
    return MensagemMontada(
        msg_id=norm.msg_id, chat=norm.chat, plat=norm.plat,
        sku=norm.sku, texto=texto, imagem=imagem,
        mapa=norm.mapa, msg_id_origem=norm.msg_id,
    )

async def buscar_imagem_produto(url: str) -> Optional[str]:
    """
    Busca og:image do produto via link afiliado.
    Prioridade: og:image → twitter:image → ld+json → maior img tag.
    Só para OFERTAS — nunca chamado para cupons.
    """
    if not url or not url.startswith("http"): return None
    hdrs = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept":     "text/html,*/*;q=0.9",
    }
    for t in range(1, 4):
        try:
            async with aiohttp.ClientSession(headers=hdrs) as s:
                async with s.get(
                    url, allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    ct = r.headers.get("content-type", "")
                    if "image" in ct:
                        return str(r.url)
                    html = await r.text(errors="ignore")
                    soup = BeautifulSoup(html, "html.parser")

                    # og:image (mais confiável)
                    for attr in [
                        {"property": "og:image"},
                        {"property": "og:image:secure_url"},
                        {"name":     "twitter:image"},
                    ]:
                        tag = soup.find("meta", attrs=attr)
                        if not tag: continue
                        img_url = tag.get("content", "")
                        if not img_url.startswith("http"): continue
                        # Remove parâmetros de resize que reduzem qualidade
                        img_url = re.sub(
                            r'[?&](?:width|height|w|h|size|resize|fit|'
                            r'quality|q|maxwidth|maxheight|format|auto|'
                            r'compress|crop|scale)=[^&]+',
                            '', img_url
                        ).rstrip('?&')
                        log_fmt.info(f"🖼 og:image: {img_url[:60]}")
                        return img_url

                    # ld+json
                    for scr in soup.find_all("script", type="application/ld+json"):
                        try:
                            data  = json.loads(scr.string or "")
                            items = data if isinstance(data, list) else [data]
                            for item in items:
                                img = item.get("image")
                                if isinstance(img, str) and img.startswith("http"):
                                    return img
                                if isinstance(img, list) and img:
                                    c = img[0]
                                    if isinstance(c, str): return c
                                    if isinstance(c, dict):
                                        u2 = c.get("url", "")
                                        if u2.startswith("http"): return u2
                        except Exception:
                            pass

                    # Maior img tag como último recurso
                    melhor = None; melhor_area = 0
                    for img_tag in soup.find_all("img", src=True):
                        src = img_tag.get("src", "")
                        if not src.startswith("http"): continue
                        if any(x in src.lower() for x in
                               ["icon","logo","avatar","badge","spinner"]):
                            continue
                        try:
                            w    = int(img_tag.get("width",  0))
                            h    = int(img_tag.get("height", 0))
                            area = w * h
                            if area > melhor_area:
                                melhor_area = area; melhor = src
                        except (ValueError, TypeError):
                            if any(x in src.lower() for x in
                                   ["product","produto","item","image",
                                    "foto","zoom","large","xl","hd","original"]):
                                if not melhor: melhor = src
                    if melhor:
                        log_fmt.info(f"🖼 img tag: {melhor[:60]}")
                        return melhor

        except asyncio.TimeoutError:
            log_fmt.warning(f"⏱ Timeout buscar_img t={t}")
        except Exception as e:
            log_fmt.warning(f"⚠️ buscar_img t={t}: {e}")
        if t < 3: await asyncio.sleep(1.0)

    log_fmt.warning(f"🖼 Nenhuma imagem encontrada: {url[:60]}")
    return None


async def preparar_imagem_tg(media_obj) -> Optional[object]:
    try:
        buf = io.BytesIO()
        res = await client.download_media(media_obj, file=buf)
        if res is None: return None
        buf.seek(0)
        if buf.getbuffer().nbytes < 500: return None
        buf.name = "imagem.jpg"; return buf
    except Exception as e:
        log_fmt.warning(f"⚠️ download_media: {e}"); return None


async def preparar_imagem_url(url: str) -> Optional[object]:
    try:
        async with aiohttp.ClientSession(
            headers={"User-Agent": random.choice(USER_AGENTS)}
        ) as s:
            async with s.get(
                url, timeout=aiohttp.ClientTimeout(total=20),
                allow_redirects=True
            ) as r:
                if r.status == 200:
                    data = await r.read()
                    if len(data) < 1000: return None
                    buf = io.BytesIO(data)
                    buf.name = "produto.jpg"
                    return buf
    except Exception as e:
        log_fmt.warning(f"⚠️ preparar_img_url: {e}")
    return None

async def _resolver_imagem(norm: MensagemNormalizada) -> object:
    """
    Sistema evolutivo de imagem.
    Não usa fallback de imagem padrão — o sistema de score/edição
    trata isso automaticamente quando chegar versão melhor.

    OFERTA com mídia original → usa mídia
    OFERTA com link → busca og:image
    CUPOM com mídia original → usa mídia
    CUPOM sem mídia → None (sem fallback — score evolutivo resolve)
    """
    eh_cupom = bool(norm.cupom or _KW_CUPOM.search(norm.texto_limpo))

    # Mídia original sempre primeira opção para qualquer tipo
    if norm.tem_midia:
        img = await preparar_imagem_tg(norm.media_obj)
        if img:
            log_fmt.debug("🖼 Mídia original")
            return img

    # Cupom sem mídia → None, score evolutivo vai editar quando chegar melhor
    if eh_cupom:
        log_fmt.debug("🖼 Cupom sem mídia → aguarda versão evolutiva")
        return None

    # Oferta com link → busca og:image
    if norm.mapa:
        for link in norm.mapa.values():
            if not link.startswith("http"): continue
            img_url = await buscar_imagem_produto(link)
            if img_url:
                img = await preparar_imagem_url(img_url)
                if img:
                    log_fmt.info(f"🖼 og:image: {img_url[:60]}")
                    return img

    return None
    
# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 6 — ENVIO
# ═══════════════════════════════════════════════════════════════════════════════

_SEM_ENVIO     = asyncio.Semaphore(3)
_IDS_PROC:     set          = set()
_IDS_LOCK:     asyncio.Lock = None  # type: ignore
_RATE_LOCK:    asyncio.Lock = None  # type: ignore
_ULTIMO_ENV_TS = 0.0

# Rate limit removido — sem espera entre envios
async def _rate_limit():
    pass  # removido por solicitação

async def _marcar(msg_id: int):
    async with _IDS_LOCK:
        _IDS_PROC.add(msg_id)
        if len(_IDS_PROC) > 5000:
            for _ in range(len(_IDS_PROC) - 4000):
                _IDS_PROC.pop()

async def _foi_processado(msg_id: int) -> bool:
    async with _IDS_LOCK: return msg_id in _IDS_PROC

async def _enviar_msg(texto: str, img) -> object:
    if img:
        if len(texto) <= 1024:
            try:
                return await client.send_file(
                    GRUPO_DESTINO, img,
                    caption=texto, parse_mode="md",
                    force_document=False
                )
            except Exception as e:
                log_out.warning(f"⚠️ send_file+caption: {e}")
                try:
                    await client.send_file(
                        GRUPO_DESTINO, img, force_document=False)
                    return await client.send_message(
                        GRUPO_DESTINO, texto,
                        parse_mode="md", link_preview=True)
                except Exception as e2:
                    log_out.warning(f"⚠️ send_file sem caption: {e2}")
        else:
            try:
                await client.send_file(
                    GRUPO_DESTINO, img, force_document=False)
                return await client.send_message(
                    GRUPO_DESTINO, texto,
                    parse_mode="md", link_preview=False)
            except Exception as e:
                log_out.warning(f"⚠️ send_file longo: {e}")
    return await client.send_message(
        GRUPO_DESTINO, texto,
        parse_mode="md", link_preview=True)

async def enviar(montada: MensagemMontada, norm=None) -> bool:
    """
    Envio com sistema evolutivo de score.

    Fluxo:
    1. Calcula identidade canônica e score
    2. Verifica estado existente no banco
    3. Se não existe → envia normalmente
    4. Se existe e score maior → edita mensagem anterior
    5. Se existe e score menor/igual → ignora (não duplica)
    """
    async with _SEM_ENVIO:
        loop = asyncio.get_event_loop()

        # Calcula identidade e score se norm disponível
        identity = None
        score    = 0
        if norm is not None:
            identity = identidade_canonica(norm)
            score    = calcular_score(norm)

            # Verifica estado evolutivo
            estado = db_get_estado(identity)
            if estado:
                if score > estado["score"]:
                    # Versão melhor chegou → edita mensagem existente
                    log_out.info(
                        f"✳️ [EVOLUI] {identity} "
                        f"score {estado['score']}→{score}"
                    )
                    ok = await editar_por_id(
                        estado["msg_id_dest"], montada.texto,
                        montada.imagem
                    )
                    if ok:
                        db_set_estado(
                            identity, estado["msg_id_dest"],
                            score, montada.texto, montada.plat
                        )
                    return ok
                else:
                    # Versão igual ou pior → ignora sem duplicar
                    log_out.info(
                        f"🔁 [SCORE IGUAL/MENOR] {identity} "
                        f"score atual={score} salvo={estado['score']}"
                    )
                    return True

        # Não existe estado → envia normalmente
        img  = montada.imagem
        sent = None
        for t in range(1, 4):
            try:
                sent = await _enviar_msg(montada.texto, img)
                break
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log_out.error(f"❌ envio t={t}: {e}")
                if t == 1: img = None
                elif t < 3: await asyncio.sleep(2 ** t)

        if not sent:
            log_out.error(f"❌ Envio falhou | @{montada.chat}")
            return False

        # Salva mapa origem→destino
        mp = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        mp[str(montada.msg_id)] = sent.id
        try:
            await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
        except Exception as e:
            log_sys.error(f"❌ salvar_mapa: {e}")

        await _marcar(montada.msg_id)
        db_registrar_sat(montada.plat, montada.sku)
        try:
            await _burst_add()
        except Exception:
            pass

        # Salva estado evolutivo
        if identity is not None:
            db_set_estado(identity, sent.id, score, montada.texto, montada.plat)

        # Agenda encurtamento Magalu em background se necessário
        if montada.plat == "magalu" and montada.mapa:
            for orig, conv in montada.mapa.items():
                if "partner_id" in conv and "leoind.com.br" not in conv:
                    try:
                        asyncio.create_task(
                            _cuttly_background(conv, montada.msg_id))
                    except Exception:
                        pass

        log_out.info(
            f"🚀 [OK] @{montada.chat}→{GRUPO_DESTINO} | "
            f"{montada.msg_id}→{sent.id} | "
            f"{montada.plat.upper()} score={score} sku={montada.sku}"
        )
        return True


async def editar(msg_id_origem: int, texto_novo: str) -> bool:
    """Edita mensagem por ID de origem (usado pelo pipeline de edição)."""
    loop = asyncio.get_event_loop()
    mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
    id_d = mp.get(str(msg_id_origem))
    if not id_d: return False
    return await editar_por_id(id_d, texto_novo)


async def editar_por_id(
    msg_id_dest: int,
    texto_novo: str,
    imagem_nova=None
) -> bool:
    """
    Edita mensagem no grupo destino por ID de destino.
    Suporta edição de texto e imagem.
    """
    async with _SEM_ENVIO:
        for t in range(1, 4):
            try:
                if imagem_nova:
                    # Edita com nova imagem
                    try:
                        await client.edit_message(
                            GRUPO_DESTINO, msg_id_dest,
                            texto_novo, parse_mode="md",
                            file=imagem_nova
                        )
                    except Exception:
                        # Fallback: edita só texto
                        await client.edit_message(
                            GRUPO_DESTINO, msg_id_dest,
                            texto_novo, parse_mode="md"
                        )
                else:
                    await client.edit_message(
                        GRUPO_DESTINO, msg_id_dest,
                        texto_novo, parse_mode="md"
                    )
                log_out.info(f"✏️ Editado | dest_id={msg_id_dest}")
                return True
            except MessageNotModifiedError:
                return True
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log_out.error(f"❌ edit t={t}: {e}")
                if t < 3: await asyncio.sleep(2 ** t)
    return False

# ═══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR + FILA
# ═══════════════════════════════════════════════════════════════════════════════

_WORKERS_MAX = 4
_FILA_MAX    = 200
_COALESCE_MS = 800

_buf:      list         = []
_buf_lck:  asyncio.Lock = None  # type: ignore
_buf_evt:  asyncio.Event = None  # type: ignore
_w_ativos: int          = 0
_w_lck:    asyncio.Lock = None  # type: ignore
_coal:     dict         = {}

def _init_globals():
    """Cria objetos asyncio no event loop correto. DEVE ser a primeira linha de _run()."""
    global _buf_lck, _buf_evt, _w_lck, _buf, _coal, _w_ativos
    global _RATE_LOCK, _IDS_LOCK, _burst_lock
    _buf = []; _coal = {}; _w_ativos = 0
    _buf_lck   = asyncio.Lock(); _buf_evt = asyncio.Event(); _w_lck = asyncio.Lock()
    _RATE_LOCK = asyncio.Lock(); _IDS_LOCK = asyncio.Lock(); _burst_lock = asyncio.Lock()
    log_sys.debug("🔧 _init_globals OK")

def _prio(texto: str) -> int:
    tl = texto.lower()
    if "amazon" in tl: return 1
    if "shopee" in tl: return 2
    if "magalu" in tl: return 3
    return 9

def _fp_r(texto: str) -> str:
    return hashlib.sha256(re.sub(r'\s+','',texto.lower())[:80].encode()).hexdigest()[:12]

async def _enfileirar(event, is_edit: bool):
    texto = event.message.text or ""
    if not texto.strip(): return
    fp = _fp_r(texto); agora = time.monotonic()
    async with _buf_lck:
        if not is_edit and agora - _coal.get(fp, 0.0) < _COALESCE_MS / 1000: return
        _coal[fp] = agora
        if len(_buf) >= _FILA_MAX:
            log_sys.warning(f"⚠️ Fila cheia | id={event.message.id}"); return
        heapq.heappush(_buf, (0 if is_edit else _prio(texto), agora, event, is_edit))
    _buf_evt.set()

async def _worker_loop():
    global _w_ativos
    while True:
        await _buf_evt.wait()
        while True:
            item = None
            async with _buf_lck:
                if _buf: item = heapq.heappop(_buf)
                else: _buf_evt.clear(); break
            if item is None: break
            prio, ts, event, is_edit = item
            async with _w_lck:
                if _w_ativos >= _WORKERS_MAX:
                    async with _buf_lck: heapq.heappush(_buf, item); _buf_evt.set()
                    await asyncio.sleep(0.2); break
                _w_ativos += 1
            try:
                if time.monotonic() - ts > 60:
                    log_sys.warning(f"⏱ Expirado | id={event.message.id}"); continue
                await _pipeline(event, is_edit)
            except Exception as e: log_sys.error(f"❌ Worker: {e}", exc_info=True)
            finally:
                async with _w_lck: _w_ativos -= 1

# ── Anti-saturação ────────────────────────────────────────────────────────────

_SAT_MAX_PLAT  = 10
_SAT_BURST_LIM = 6
_SAT_BURST_JAN = 60
_burst: List[float] = []
_burst_lock: asyncio.Lock = None  # type: ignore

# _KW_EVENTO definido aqui — antes de delay_saturacao que o usa
_KW_EVENTO = re.compile(
    r'\b(?:quiz|roleta|miss[aã]o|arena|girar|gire|roda|jogar|jogue|desafio)\b',
    re.I
)

async def _burst_add():
    async with _burst_lock:
        agora = time.monotonic(); _burst.append(agora)
        while _burst and agora - _burst[0] > _SAT_BURST_JAN: _burst.pop(0)

async def _burst_count() -> int:
    async with _burst_lock:
        agora = time.monotonic()
        return sum(1 for t in _burst if agora - t <= _SAT_BURST_JAN)

async def delay_saturacao(plat: str, texto: str) -> float:
    if _KW_EVENTO.search(texto): return 0.0
    delay = 0.0
    if db_count_sat(plat) >= _SAT_MAX_PLAT: delay += 6.0
    if await _burst_count() >= _SAT_BURST_LIM: delay += 4.0
    return delay

async def _pipeline(event, is_edit: bool = False):
    """Orquestra as 6 camadas em sequência."""
    msg_id = event.message.id

    # Anti-loop
    if not is_edit:
        if await _foi_processado(msg_id): return
    else:
        loop = asyncio.get_event_loop()
        mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        if str(msg_id) not in mp: return

    # ── Camada 1: Ingestão ──────────────────────────────────────────
    try:
        bruta = ingerir(event)
    except Exception as e:
        log_sys.error(f"❌ ingestao: {e}"); return

    log_sys.info(
        f"{'✏️' if is_edit else '📩'} @{bruta.chat} | "
        f"id={msg_id} | q={len(_buf)} w={_w_ativos}"
    )

    # ── Camadas 2+3: Classificação + Normalização ───────────────────
    try:
        norm = await normalizar(bruta)
    except Exception as e:
        log_sys.error(f"❌ normalizar: {e}"); return
    if norm is None: return

    # ── Camada 4: Deduplicação + Score evolutivo ────────────────────
    if not is_edit:
        try:
            if not await deve_enviar_async(norm): return
        except Exception as e:
            log_sys.error(f"❌ deve_enviar: {e}"); return

        try:
            delay = await delay_saturacao(norm.plat, norm.texto_limpo)
            if delay > 0: await asyncio.sleep(delay)
        except Exception as e:
            log_sys.error(f"❌ saturacao: {e}")

    # ── Camada 5: Montagem ──────────────────────────────────────────
    try:
        montada = await montar(norm)
    except Exception as e:
        log_sys.error(f"❌ montar: {e}"); return

    # ── Camada 6: Envio ─────────────────────────────────────────────
    if is_edit:
        # Edição: verifica se imagem também mudou
        if norm.tem_midia:
            img_nova = await preparar_imagem_tg(norm.media_obj)
            if img_nova:
                loop = asyncio.get_event_loop()
                mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
                id_d = mp.get(str(msg_id))
                if id_d:
                    await editar_por_id(id_d, montada.texto, img_nova)
                    return
        await editar(msg_id, montada.texto)
    else:
        await enviar(montada, norm=norm)

async def processar(event, is_edit: bool = False):
    await _enfileirar(event, is_edit)

async def _iniciar_orchestrator():
    log_sys.info(f"🎛 Orchestrator | workers={_WORKERS_MAX} fila={_FILA_MAX} coalesce={_COALESCE_MS}ms")
    asyncio.create_task(_worker_loop())


# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════════

async def _health_check():
    while True:
        await asyncio.sleep(300)
        try:
            db_limpar()
            try:
                with _db() as db:
                    n_links = db.execute("SELECT COUNT(*) FROM links_cache").fetchone()[0]
                    n_dedup = db.execute("SELECT COUNT(*) FROM dedupe_temp").fetchone()[0]
                    n_sat   = db.execute("SELECT COUNT(*) FROM saturacao").fetchone()[0]
            except Exception: n_links = n_dedup = n_sat = "?"
            log_hc.info(f"💚 links={n_links}(perm) | dedupe={n_dedup} | sat={n_sat} | "
                        f"anti-loop={len(_IDS_PROC)} | fila={len(_buf)} w={_w_ativos} | "
                        f"PIL={'OK' if _PIL_OK else 'OFF'}")
        except Exception as e: log_hc.error(f"❌ Health: {e}", exc_info=True)

# ═══════════════════════════════════════════════════════════════════════════════
# INICIALIZAÇÃO
# ═══════════════════════════════════════════════════════════════════════════════

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

async def _run():
    _init_globals()
    _init_db()
    _KW_EVENTO = set()

    log_sys.info("🔌 Conectando...")
    await client.connect()

    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida")
        return False

    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 {GRUPOS_ORIGEM} → {GRUPO_DESTINO}")
    log_sys.info(f"🟠 Amazon: {_AMZ_TAG} | 🟣 Shopee: {_SHP_APP_ID} | 🔵 Magalu: {_MGL_PROMOTER}/{_MGL_SLUG}")
    log_sys.info(f"🖼 Pillow: {'OK' if _PIL_OK else 'OFF'}")
    log_sys.info("🚀 FOGUETÃO v76.0 — ONLINE")

    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def on_new(event):
        try: await processar(event, is_edit=False)
        except Exception as e: log_sys.error(f"❌ on_new: {e}", exc_info=True)

    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def on_edit(event):
        try: await processar(event, is_edit=True)
        except Exception as e: log_sys.error(f"❌ on_edit: {e}", exc_info=True)

    asyncio.create_task(_health_check())
    asyncio.create_task(_iniciar_orchestrator())
    asyncio.create_task(_iniciar_servidor_web())

    await client.run_until_disconnected()
    return True

# ═══════════════════════════════════════════════════════════════════════════════
# SERVIDOR DE REDIRECT — encurtador próprio Magalu
# Roda no mesmo processo do bot. Porta 8080.
# ═══════════════════════════════════════════════════════════════════════════════

async def _handle_redirect(request: web.Request) -> web.Response:
    code = request.match_info.get("code", "").replace("-magalu", "")
    if not code:
        return web.Response(status=404, text="Not found")
    url_destino = await _resolver_short_magalu(code)
    if url_destino:
        raise web.HTTPFound(location=url_destino)
    return web.Response(status=404, text="Link não encontrado")

async def _handle_health(request: web.Request) -> web.Response:
    return web.Response(text="OK")

async def _iniciar_servidor_web():
    app = web.Application()
    app.router.add_get("/", _handle_health)
    app.router.add_get("/health", _handle_health)
    app.router.add_get("/{code}", _handle_redirect)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log_sys.info(
        f"🌐 Servidor redirect ativo | porta={port} | base={_SHORT_BASE}"
    )

async def main():
    while True:
        try: await _run()
        except (AuthKeyUnregisteredError, SessionPasswordNeededError) as e:
            log_sys.error(f"❌ Auth fatal: {e}"); break
        except Exception as e:
            log_sys.error(f"💥 Caiu: {e} — restart 15s", exc_info=True)
            try: await client.disconnect()
            except Exception: pass
            await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())

"""Estados globais, locks, caches in-memory, HTTP session singleton."""
from __future__ import annotations
import asyncio
import random
from collections import OrderedDict
from threading import Lock
from typing import Dict, Optional

import aiohttp

import config
from logger import log_db

# ── Caches in-memory ─────────────────────────────────────────────
_raw_cache:   OrderedDict[str, str] = OrderedDict()
_final_cache: OrderedDict[str, str] = OrderedDict()
_cls_cache:   OrderedDict          = OrderedDict()   # str → LinkClassificado
_cls_lock    = Lock()
_cache_lock  = Lock()
_CACHE_LIMIT = 5000

# ── Persistência JSON ─────────────────────────────────────────────
_MAP_LOCK = Lock()

# ── Locks async (inicializados em _init_globals) ──────────────────
_buf_lck:        Optional[asyncio.Lock]  = None
_buf_evt:        Optional[asyncio.Event] = None
_w_lck:          Optional[asyncio.Lock]  = None
_IDS_LOCK:       Optional[asyncio.Lock]  = None
_BURST_LOCK:     Optional[asyncio.Lock]  = None
_atomic_lck_obj: Optional[asyncio.Lock]  = None
_pending_lock:   Optional[asyncio.Lock]  = None

# ── Estado do orchestrator ────────────────────────────────────────
_buf:      list       = []
_coal:     dict       = {}
_w_ativos: int        = 0
_IDS_PROC: set        = set()
_burst:    list       = []
_atomic_mem: Dict[str, float] = {}

# ── HTTP Session singleton ────────────────────────────────────────
_http_session: Optional[aiohttp.ClientSession] = None

async def _get_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        conn = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False)
        _http_session = aiohttp.ClientSession(
            connector=conn,
            timeout=aiohttp.ClientTimeout(total=40, connect=8),
            headers={"User-Agent": random.choice(config.USER_AGENTS)},
        )
    return _http_session

# ── Inicialização de todos os globals async ───────────────────────
def init_globals() -> None:          # ← mudei o nome (sem underline)
    """Inicializa todos os objetos globais. Deve ser chamado no startup."""
    global _buf_lck, _buf_evt, _w_lck, _buf, _coal, _w_ativos
    global _IDS_LOCK, _BURST_LOCK, _atomic_lck_obj, _atomic_mem
    global _IDS_PROC, _burst, _pending_lock

    _buf.clear()
    _coal.clear()
    _burst.clear()
    _atomic_mem.clear()
    _IDS_PROC.clear()
    _w_ativos = 0

    _buf_lck        = asyncio.Lock()
    _buf_evt        = asyncio.Event()
    _w_lck          = asyncio.Lock()
    _IDS_LOCK       = asyncio.Lock()
    _BURST_LOCK     = asyncio.Lock()
    _atomic_lck_obj = asyncio.Lock()
    _pending_lock   = asyncio.Lock()

    # Semáforos no config
    import config as _cfg
    _cfg._SEM_ENVIO = asyncio.Semaphore(3)
    _cfg._SEM_HTTP  = asyncio.Semaphore(20)

    log_db.info("✅ Globals inicializados com sucesso")

# ── Helpers de cache ─────────────────────────────────────────────
def _set_raw(url: str, valor: str):
    from utils.urls import _cache_key
    key = _cache_key(url)
    with _cache_lock:
        _raw_cache[key] = valor
        _raw_cache.move_to_end(key)
        if len(_raw_cache) > _CACHE_LIMIT:
            _raw_cache.popitem(last=False)

def _set_final(url: str, valor: str):
    from utils.urls import _cache_key
    key = _cache_key(url)
    with _cache_lock:
        _final_cache[key] = valor
        _final_cache.move_to_end(key)
        if len(_final_cache) > _CACHE_LIMIT:
            _final_cache.popitem(last=False)

def _get_raw(url: str) -> Optional[str]:
    from utils.urls import _cache_key
    with _cache_lock:
        return _raw_cache.get(_cache_key(url))

def _get_final(url: str) -> Optional[str]:
    from utils.urls import _cache_key
    with _cache_lock:
        return _final_cache.get(_cache_key(url))

def _log_cache_stats():
    from database import _db_count_links
    log_db.debug(
        f"📦 Cache | raw={len(_raw_cache)} final={len(_final_cache)} "
        f"cls={len(_cls_cache)} db_links={_db_count_links()}"
)
      

"""Estados globais, locks, caches in-memory, HTTP session singleton.

═══════════════════════════════════════════════════════════════════
v80.4 — Auditoria sênior aplicada
═══════════════════════════════════════════════════════════════════
Cirurgias incluídas:
  • Cirurgia 11 (Bug #24)  — _EXCLUSAO_POOLS_LOCK inicializado aqui
  • Cirurgia 20 (Bug #34)  — _session_lock protege _get_session
═══════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import asyncio
import random
from collections import OrderedDict
from threading import Lock
from typing import Dict, Optional

import aiohttp

import config
from plataformas.contrato import Afiliacao
from logger import log_db

# ── Caches in-memory ─────────────────────────────────────────────
_raw_cache:   OrderedDict[str, str] = OrderedDict()
_final_cache: OrderedDict[str, Afiliacao] = OrderedDict()
_cache_lock  = Lock()
_CACHE_LIMIT = 5000

# ── Locks async (inicializados em _init_globals) ──────────────────
_IDS_LOCK:       Optional[asyncio.Lock]  = None
_atomic_lck_obj: Optional[asyncio.Lock]  = None

# CIRURGIA 11 (Bug #24): mutex global dos pools de lock em pipeline/exclusao.py.
# Antes era inicializado lazy DENTRO do getter do lock, com race
# possível (2 tasks ambas viam None e ambas criavam Lock —
# último wins, primeiro fica órfão).
_EXCLUSAO_POOLS_LOCK: Optional[asyncio.Lock] = None

# CIRURGIA 20 (Bug #34): lock pra proteger lazy init de _http_session
_session_lock: Optional[asyncio.Lock] = None

# ── Estado do orchestrator ────────────────────────────────────────
# _buf é o conjunto de tasks ADMITIDAS e ainda não concluídas. É a
# estrutura do teto de admissão (_FILA_MAX) e a referência forte que
# impede o coletor de descartar uma task em execução. len(_buf) é lido
# por telemetria; a mutação é exclusiva de pipeline.orchestrator_fila.
_buf:      set        = set()
_coal:     dict       = {}
_w_ativos: int        = 0
_IDS_PROC: set        = set()
_atomic_mem: Dict[str, float] = {}

# ── HTTP Session singleton ────────────────────────────────────────
_http_session: Optional[aiohttp.ClientSession] = None


async def _get_session() -> aiohttp.ClientSession:
    """
    Retorna a session singleton, criando-a sob lock se não existir.

    CIRURGIA 20 (Bug #34): antes era lazy init sem lock — duas tasks
    podiam ver `_http_session is None` simultaneamente e criar duas
    sessions, segunda sobrescreve primeira (memory leak da órfã).
    """
    global _http_session, _session_lock
    if _session_lock is None:
        _session_lock = asyncio.Lock()
    async with _session_lock:
        if _http_session is None or _http_session.closed:
            conn = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False)
            _http_session = aiohttp.ClientSession(
                connector=conn,
                timeout=aiohttp.ClientTimeout(total=40, connect=8),
                headers={"User-Agent": random.choice(config.USER_AGENTS)},
            )
        return _http_session


# ── Inicialização de todos os globals async ───────────────────────
def _init_globals():
    """
    Inicializa locks/eventos asyncio + zera caches em memória.

    REGRAS:
      - Containers mutáveis (_buf, _IDS_PROC, _atomic_mem,
        _coal): usa .clear() pra MANTER o mesmo objeto (evita bug
        de outros módulos que importaram com `from globals import _buf`
        ficarem apontando pra objeto antigo).

      - Locks asyncio (que começam None): tem que reassignar com
        asyncio.Lock(). Por isso TODOS os consumidores DEVEM usar
        `import globals as g` + `g._IDS_LOCK` (acessam dinamicamente).

      - _w_ativos (int): mantém reassign — int é imutável.
    """
    global _w_ativos
    global _IDS_LOCK, _atomic_lck_obj
    global _EXCLUSAO_POOLS_LOCK, _session_lock
    import config as _cfg

    # Containers mutáveis: clear() pra manter mesmo objeto
    _buf.clear()
    _coal.clear()
    _IDS_PROC.clear()
    _atomic_mem.clear()

    # Contador int: reassign
    _w_ativos = 0

    # Locks asyncio
    _IDS_LOCK           = asyncio.Lock()
    _atomic_lck_obj     = asyncio.Lock()
    _EXCLUSAO_POOLS_LOCK = asyncio.Lock()  
    _session_lock       = asyncio.Lock() 

    # Semáforos do config (só podem ser criados dentro do loop async)
    _cfg._SEM_ENVIO = asyncio.Semaphore(3)
    _cfg._SEM_HTTP  = asyncio.Semaphore(20)

    log_db.debug("🔧 _init_globals OK")


# ── Helpers de cache ─────────────────────────────────────────────
def _set_raw(url: str, valor: str):
    from utils.urls import _cache_key
    key = _cache_key(url)
    with _cache_lock:
        _raw_cache[key] = valor
        _raw_cache.move_to_end(key)
        if len(_raw_cache) > _CACHE_LIMIT:
            _raw_cache.popitem(last=False)


def _set_final(url: str, valor: Afiliacao):
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


def _get_final(url: str) -> Optional[Afiliacao]:
    from utils.urls import _cache_key
    with _cache_lock:
        return _final_cache.get(_cache_key(url))

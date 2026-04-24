# ╔══════════════════════════════════════════════════════════════════╗
# ║  FOGUETÃO v76.0 — Arquitetura Limpa (6 camadas)                 ║
# ║  1.Ingestão → 2.Classificação → 3.Normalização →                ║
# ║  4.Deduplicação → 5.Montagem → 6.Envio                          ║
# ╚══════════════════════════════════════════════════════════════════╝
from __future__ import annotations
import asyncio, concurrent.futures, hashlib, heapq, io, json, logging
import os, random, re, sqlite3, time, unicodedata
from contextlib import contextmanager
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from threading import Lock
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse
import aiohttp
from bs4 import BeautifulSoup
from telethon import TelegramClient, events
from telethon.errors import (AuthKeyUnregisteredError, FloodWaitError,
    MessageNotModifiedError, SessionPasswordNeededError)
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
CACHE_TTL = 86400
TTL_DEDUPE    = 86400
TTL_SCHEDULER = 30 * 86400

def _init_db():
    global _db_conn
    _db_conn = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=10, isolation_level=None)
    for p in ["PRAGMA journal_mode=WAL","PRAGMA synchronous=NORMAL",
              "PRAGMA cache_size=-16000","PRAGMA temp_store=MEMORY"]:
        _db_conn.execute(p)
    _db_conn.executescript("""
        CREATE TABLE IF NOT EXISTS links_cache(
            url_orig TEXT PRIMARY KEY, url_conv TEXT NOT NULL,
            plat TEXT NOT NULL, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dedupe_temp(
            fp TEXT PRIMARY KEY, plat TEXT NOT NULL,
            cupons TEXT, alma TEXT, camp TEXT,
            asin TEXT, id_prod TEXT, benef TEXT, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS saturacao(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plat TEXT NOT NULL, sku TEXT, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS scheduler(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plat TEXT NOT NULL, hora INTEGER NOT NULL,
            score REAL DEFAULT 1.0, ts REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_lc_plat ON links_cache(plat);
        CREATE INDEX IF NOT EXISTS idx_dt_plat ON dedupe_temp(plat,ts);
        CREATE INDEX IF NOT EXISTS idx_dt_asin ON dedupe_temp(asin);
        CREATE INDEX IF NOT EXISTS idx_dt_id   ON dedupe_temp(id_prod);
        CREATE INDEX IF NOT EXISTS idx_sat     ON saturacao(plat,ts);
        CREATE INDEX IF NOT EXISTS idx_sch     ON scheduler(plat,hora);
    """)
    for tabela, col, tipo in [
        ("dedupe_temp","benef","TEXT"),
        ("dedupe_temp","asin","TEXT"),
        ("dedupe_temp","id_prod","TEXT"),
    ]:
        try: _db_conn.execute(f"ALTER TABLE {tabela} ADD COLUMN {col} {tipo}")
        except sqlite3.OperationalError: pass
    log_db.info(f"🗄 DB ON | {_DB_PATH}")

@contextmanager
def _db():
    with _db_lock:
        try: yield _db_conn
        except sqlite3.Error as e: log_db.error(f"❌ DB: {e}"); raise

def db_get_link(url: str) -> Optional[str]:
    try:
        with _db() as db:
            row = db.execute(
                "SELECT url_conv FROM links_cache WHERE url_orig=?",
                (url,)
            ).fetchone()
        return row[0] if row else None
    except Exception as e:
        log_db.error(f"❌ db_get_link: {e}")
        return None


def db_set_link(url_orig: str, url_conv: str, plat: str):
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO links_cache(url_orig,url_conv,plat,ts)VALUES(?,?,?,?)",
                (url_orig, url_conv, plat, time.time())
            )
    except Exception as e:
        log_db.error(f"❌ db_set_link: {e}")


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
                "plat": row[0],
                "cupons": json.loads(row[1] or "[]"),
                "alma": row[2],
                "camp": row[3],
                "asin": row[4] or "",
                "id_prod": row[5] or "",
                "benef": json.loads(row[6] or "[]"),
                "ts": row[7],
            }
    except Exception as e:
        log_db.error(f"❌ db_get_dedupe: {e}")

    return None


def db_set_dedupe(fp: str, plat: str, cupons: list, alma: str,
                  camp: str, asin: str = "", id_prod: str = "", benef: list = None):
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO dedupe_temp "
                "(fp,plat,cupons,alma,camp,asin,id_prod,benef,ts)VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    fp,
                    plat,
                    json.dumps(cupons or []),
                    alma or "",
                    camp or "geral",
                    asin or "",
                    id_prod or "",
                    json.dumps(benef or []),
                    time.time(),
                ),
            )
    except Exception as e:
        log_db.error(f"❌ db_set_dedupe: {e}")


def db_buscar_janela_rapida(plat: str, janela: float = 600) -> list:
    try:
        limite = time.time() - janela
        with _db() as db:
            rows = db.execute(
                "SELECT fp,cupons,alma,asin,id_prod,benef,ts "
                "FROM dedupe_temp WHERE plat=? AND ts>=? ORDER BY ts DESC",
                (plat, limite),
            ).fetchall()

        return [
            {
                "fp": r[0],
                "cupons": json.loads(r[1] or "[]"),
                "alma": r[2] or "",
                "asin": r[3] or "",
                "id_prod": r[4] or "",
                "benef": json.loads(r[5] or "[]"),
                "ts": r[6],
            }
            for r in rows
        ]

    except Exception as e:
        log_db.error(f"❌ db_janela: {e}")
        return []


def db_registrar_sat(plat: str, sku: str = ""):
    try:
        with _db() as db:
            db.execute(
                "INSERT INTO saturacao(plat,sku,ts)VALUES(?,?,?)",
                (plat, sku or "", time.time()),
            )
    except Exception as e:
        log_db.error(f"❌ db_sat: {e}")


def db_count_sat(plat: str, janela: float = 1800) -> int:
    try:
        limite = time.time() - janela
        with _db() as db:
            row = db.execute(
                "SELECT COUNT(*) FROM saturacao WHERE plat=? AND ts>=?",
                (plat, limite),
            ).fetchone()

        return row[0] if row else 0

    except Exception as e:
        log_db.error(f"❌ db_count_sat: {e}")
        return 0


def db_limpar():
    """Apaga apenas dados temporários. links_cache NUNCA é apagado."""
    try:
        agora = time.time()
        with _db() as db:
            db.execute("DELETE FROM dedupe_temp WHERE ts<?", (agora - TTL_DEDUPE,))
            db.execute("DELETE FROM saturacao WHERE ts<?", (agora - TTL_DEDUPE,))
            db.execute("DELETE FROM scheduler WHERE ts<?", (agora - TTL_SCHEDULER,))

        log_db.debug("🗑 Limpeza temp OK")

    except Exception as e:
        log_db.error(f"❌ db_limpar: {e}")

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
# Responsabilidade: identificar plataforma e tipo de cada URL.
# Retorna enums limpos — não converte, não filtra texto.
# ═══════════════════════════════════════════════════════════════════════════════

_AMZ_DOMINIOS = frozenset({"amazon.com.br","amazon.com","amzn.to","amzn.com","a.co","amzlink.to","amzn.eu"})
_SHP_DOMINIOS = frozenset({"shopee.com.br","s.shopee.com.br","shopee.com","shope.ee","flapremios.com.br"})
_MGL_DOMINIOS = frozenset({"magazineluiza.com.br","sacola.magazineluiza.com.br","magazinevoce.com.br","maga.lu"})
_ENCURTADORES = frozenset({"bit.ly","cutt.ly","tinyurl.com","t.co","ow.ly","goo.gl","rb.gy","is.gd","tiny.cc","buff.ly","short.io","bl.ink","rebrand.ly","shorturl.at"})
_PRESERVE     = frozenset({"wa.me","api.whatsapp.com"})
_DELETAR      = frozenset({"t.me","telegram.me","telegram.org","chat.whatsapp.com"})
_BLOQUEADOS   = frozenset({"mercadolivre.com.br","mercadopago.com.br","mercadolivre.com","meli.com","ml.com.br"})

_P_SHP = [re.compile(r'/product/(\d+)/(\d+)'), re.compile(r'/i\.(\d+)\.(\d+)')]
_P_MGL = re.compile(r'/(?:[^/]+/)?p/([a-z0-9]{6,})/?', re.I)
_P_AMZ_ASIN = [
    re.compile(r'/dp/([A-Z0-9]{10})', re.I),
    re.compile(r'/gp/product/([A-Z0-9]{10})', re.I),
    re.compile(r'[?&]asin=([A-Z0-9]{10})', re.I),
]

@dataclass
class LinkClassificado:
    url_original: str
    plat: str        # amazon | shopee | magalu | expandir | preservar | None
    tipo: str        # produto | busca | evento | campanha | encurtado | invalido
    sku:  str        # ASIN, shopee ID, magalu slug, etc.


def _netloc_parse(p) -> str:
    try:
        return p.netloc.lower().replace("www.", "")
    except Exception:
        return ""


def _extrair_asin(p) -> str:
    path = p.path + "?" + p.query
    for pat in _P_AMZ_ASIN:
        m = pat.search(path)
        if m:
            return m.group(1).upper()
    return ""


def _extrair_sku_shopee(p) -> str:
    path = p.path + "?" + p.query
    for pat in _P_SHP:
        m = pat.search(path)
        if m:
            return f"{m.group(1)}.{m.group(2)}"
    return ""


def _extrair_sku_magalu(p) -> str:
    m = _P_MGL.search(p.path)
    return m.group(1) if m else ""


def classificar_url(url: str) -> LinkClassificado:
    try:
        p = urlparse(url)
    except Exception:
        return LinkClassificado(url, None, "invalido", "")

    nl = _netloc_parse(p)
    if not nl:
        return LinkClassificado(url, None, "invalido", "")

    for d in _BLOQUEADOS:
        if nl == d or nl.endswith("." + d):
            log_cls.debug(f"🚫 Bloqueado: {nl}")
            return LinkClassificado(url, None, "bloqueado", "")

    for d in _DELETAR:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, None, "grupo_externo", "")

    for d in _PRESERVE:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, "preservar", "preservar", "")

    for d in _MGL_DOMINIOS:
        if nl == d or nl.endswith("." + d):
            sku = _extrair_sku_magalu(p)
            if "sacola" in nl and not p.path.strip("/"):
                return LinkClassificado(url, "magalu", "invalido", sku)
            tipo = (
                "produto" if sku
                else "lista" if "/l/" in p.path
                else "selecao" if "/selecao/" in p.path
                else "campanha"
            )
            return LinkClassificado(url, "magalu", tipo, sku)

    for d in _AMZ_DOMINIOS:
        if nl == d or nl.endswith("." + d):
            asin = _extrair_asin(p)
            tipo = (
                "produto" if asin
                else "busca" if re.search(r'/s[/?]|/deals|/b[/?]', p.path)
                else "evento" if re.search(r'/events/|/stores/', p.path)
                else "campanha"
            )
            return LinkClassificado(url, "amazon", tipo, asin)

    for d in _SHP_DOMINIOS:
        if nl == d or nl.endswith("." + d):
            if nl == "flapremios.com.br":
                return LinkClassificado(url, "shopee", "campanha", "")
            sku = _extrair_sku_shopee(p)
            return LinkClassificado(url, "shopee", "produto" if sku else "busca", sku)

    for d in _ENCURTADORES:
        if nl == d or nl.endswith("." + d):
            return LinkClassificado(url, "expandir", "encurtado", "")

    return LinkClassificado(url, None, "desconhecido", "")


def classificar_links(links: List[str]) -> List[LinkClassificado]:
    result = [classificar_url(u) for u in links]
    validos = [r for r in result if r.plat not in (None,)]
    log_cls.debug(f"🔍 {len(validos)}/{len(links)} classificados")
    return result

# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 3 — NORMALIZAÇÃO
# Cole no lugar do bloco CAMADA 3 atual em foguetao_v76.py (linhas 381–797)
#
# CORREÇÕES:
#   • _netloc() NÃO redefinida aqui — vem da camada 2 (resolve NameError)
#   • _limpar_url_amazon usa p.netloc (não força .com.br)
#   • _afiliar_amazon: tipo "claims" → retorna URL sem tag
#   • _afiliar_shopee: fallback → None (não envia sem afiliação)
#   • _afiliar_magalu: desencurta cutt.ly/*magalu* e divulgador.magalu.com
#                      retorna afiliado longo se Cuttly falhar (nunca None)
#                      agenda background para encurtar depois
#   • desencurtar: anti-loop cutt.ly, cache de URLs já desencurtadas,
#                  proteção de HTML > 500k
#   • _normalizar_um: trata "mundial" e "claims" passando URL intacta
#   • _extrair_asin_texto: loop unificado sem duplicidade
#   • extrair_cupom: exige KW na mesma linha do código
#   • normalizar(): limpa _cls_cache a cada ciclo
#                   mundiais incluídos no mapa como url→url
# ═══════════════════════════════════════════════════════════════════════════════

# ── 3a. Filtro de texto ──────────────────────────────────────────────────────

_FILTRO_TEXTO = [
    "Monitor Samsung","Fonte Mancer","Placa de video","Monitor LG",
    "PC home Essential","Suporte articulado","VHAGAR","Superframe",
    "AM5","AM4","GTX","DDR5","DDR4","Dram","Monitor Safe",
    "Monitor Redragon","CL18","CL16","CL32","MT/s","MHz",
    "RX 580","Ryzen","Placa Mãe","Gabinete Gamer",
    "Water Cooler","Air Cooler",
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
    """Extrai código de cupom. Exige KW explícita na mesma linha do código."""
    for linha in texto.splitlines():
        if not _KW_CUPOM.search(linha): continue
        for m in _KW_COD.finditer(linha):
            c = m.group(1)
            if c not in _FALSO_CUPOM and len(c) >= 4:
                return c
    return ""

def _extrair_asin_texto(texto: str, mapa: dict) -> str:
    """Loop unificado — sem duplicidade de regex."""
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

# ── 3d. Desencurtador ────────────────────────────────────────────────────────

_FORCA_GET = frozenset({
    "amzlink.to","amzn.to","a.co","amzn.com","bit.ly",
    "tinyurl.com","rb.gy","is.gd","cutt.ly","ow.ly","buff.ly",
})
# Cache in-process de URLs já desencurtadas — evita requests repetidos
_desc_cache: Dict[str, str] = {}

# Hint de Magalu em links encurtados
_RE_CUTT_MAGALU = re.compile(r'cutt\.ly/[^\s]*magalu', re.I)
_RE_DIVULGADOR  = re.compile(r'divulgador\.magalu\.com', re.I)

def _parece_magalu_encurtado(url: str) -> bool:
    return bool(_RE_CUTT_MAGALU.search(url) or _RE_DIVULGADOR.search(url))

async def desencurtar(url: str, sessao: aiohttp.ClientSession, depth: int = 0) -> str:
    """
    Segue redirects até 15 níveis.
    Anti-loop: cutt.ly não é re-expandido após redirect.
    Cache: URLs já resolvidas não fazem request de novo.
    HTML > 500k: ignora parse, retorna posição atual.
    Erro de rede: retorna URL original (não None — chamador decide).
    """
    if depth > 15:
        return url
    url = url.strip().rstrip('.,;)>')
    if not url.startswith(("http://", "https://")):
        return url

    # Anti-loop: se já é cutt.ly após redirect, para aqui
    nl = _netloc(url)
    if depth > 0 and nl == "cutt.ly":
        return url

    # Cache de desencurtamento
    if url in _desc_cache:
        return _desc_cache[url]

    hdrs = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,*/*;q=0.9",
        "Accept-Language": "pt-BR,pt;q=0.9",
    }
    try:
        usar_head = nl not in _FORCA_GET and not any(nl.endswith("." + d) for d in _FORCA_GET)
        if usar_head:
            try:
                async with sessao.head(url, headers=hdrs, allow_redirects=True,
                                       timeout=aiohttp.ClientTimeout(total=8),
                                       max_redirects=20) as r:
                    final = str(r.url)
                    if final != url:
                        _desc_cache[url] = final
                        return await desencurtar(final, sessao, depth + 1)
            except Exception:
                pass

        async with sessao.get(url, headers=hdrs, allow_redirects=True,
                              timeout=aiohttp.ClientTimeout(total=15),
                              max_redirects=20) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")

            if pos != url:
                _desc_cache[url] = pos
                return await desencurtar(pos, sessao, depth + 1)

            # Proteção HTML grande
            if len(html) > 500_000:
                _desc_cache[url] = pos
                return pos

            soup = BeautifulSoup(html, "html.parser")

            ref = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)", ref["content"], re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    if novo.startswith("http"):
                        return await desencurtar(novo, sessao, depth + 1)

            for pat in [
                r'window\.location(?:\.href)?\s*=\s*["\']([^"\']{15,})["\']',
                r'location\.replace\s*\(\s*["\']([^"\']{15,})["\']\s*\)',
            ]:
                mj = re.search(pat, html)
                if mj and mj.group(1).startswith("http"):
                    return await desencurtar(mj.group(1), sessao, depth + 1)

            og = soup.find("meta", attrs={"property": "og:url"})
            if og and og.get("content","").startswith("http") and og["content"] != url:
                return await desencurtar(og["content"], sessao, depth + 1)

            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href","").startswith("http") and canon["href"] != url:
                return await desencurtar(canon["href"], sessao, depth + 1)

            _desc_cache[url] = pos
            return pos

    except asyncio.TimeoutError:
        log_nrm.warning(f"⏱ Timeout desencurtar d={depth}: {url[:60]}")
        return url
    except Exception as e:
        log_nrm.error(f"❌ desencurtar d={depth}: {e}")
        return url

# ── 3e. Motores de afiliação ─────────────────────────────────────────────────

_AMZ_LIXO   = frozenset({
    "ascsubtag","btn_ref","ref_","ref","smid","sprefix","spla","dchild",
    "linkcode","linkid","camp","creative","pf_rd_p","pf_rd_r","pd_rd_wg",
    "pd_rd_w","content-id","pd_rd_r","pd_rd_i","ie","qid","_encoding",
    "dib","dib_tag","m","marketplaceid","ufe","th","psc","ingress",
    "visitid","ds","rnid","sr",
})
_AMZ_MANTER = frozenset({"keywords","node","k","i","rh","n","field-keywords"})

def _limpar_url_amazon(url: str) -> Optional[str]:
    """
    Limpa URL Amazon e adiciona tag.
    Usa p.netloc original — NÃO força .com.br (preserva .com, .eu).
    Paths sem comissão (prime, claims, gaming) → URL limpa SEM tag.
    """
    try:
        p    = urlparse(url)
        asin = _extrair_asin(url)

        # Sem comissão — devolve limpa sem tag
        if _AMZ_PATHS_SEM_TAG.match(p.path):
            return urlunparse(p._replace(query="", fragment=""))

        if asin:
            return urlunparse(p._replace(
                path=f"/dp/{asin}",
                query=f"tag={_AMZ_TAG}",
                fragment="",
            ))
        if "/promotion/" in p.path:
            return urlunparse(p._replace(
                query=f"tag={_AMZ_TAG}", fragment=""))

        params = {k: v[0] for k, v in parse_qs(p.query).items()
                  if k.lower() in _AMZ_MANTER and len(v[0]) < 60}
        params["tag"] = _AMZ_TAG
        return urlunparse(p._replace(
            scheme="https",
            netloc=p.netloc,        # ← preserva netloc original
            query=urlencode(params),
            fragment="",
        ))
    except Exception:
        return None

async def _afiliar_amazon(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    log_nrm.debug(f"▶ AMZ: {url[:80]}")
    cached = db_get_link(url)
    if cached: return cached

    # Claims/prime antes de qualquer processamento
    lc_pre = _classificar_cached(url)
    if lc_pre.tipo == "claims":
        log_nrm.debug(f"  AMZ claims/prime → sem tag: {url[:60]}")
        return url

    # Mundial (gaming.amazon.com) → passa intacto
    if lc_pre.plat == "mundial":
        return url

    try:
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)
    except Exception:
        return None

    # Reclassifica após expandir
    lc_exp = _classificar_cached(exp)
    if lc_exp.tipo == "claims" or lc_exp.plat == "mundial":
        return exp

    final = _limpar_url_amazon(exp)
    if not final:
        p = urlparse(exp)
        if _AMZ_PATHS_SEM_TAG.match(p.path):
            final = urlunparse(p._replace(query="", fragment=""))
        else:
            final = f"{exp.split('?',1)[0]}?tag={_AMZ_TAG}"

    db_set_link(url, final, "amazon")
    log_nrm.info(f"  ✅ AMZ: {final[:70]}")
    return final

_SHP_REPASSE_DIRETO = frozenset({"flapremios.com.br"})

async def _afiliar_shopee(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """
    Retry: 3 tentativas com backoff 1.5s, 3s, 4.5s.
    Fallback: None — não envia link sem afiliação.
    flapremios.com.br: repasse direto (já tem afiliação embutida).
    """
    log_nrm.debug(f"▶ SHP: {url[:80]}")
    nl = _netloc(url)
    for d in _SHP_REPASSE_DIRETO:
        if nl == d or nl.endswith("." + d):
            log_nrm.info(f"  ↩️ SHP repasse direto: {url[:60]}")
            return url

    cached = db_get_link(url)
    if cached: return cached

    for tentativa in range(1, 4):
        try:
            ts      = str(int(time.time()))
            payload = json.dumps({
                "query": (
                    f'mutation {{ generateShortLink(input: '
                    f'{{ originUrl: "{url}" }}) {{ shortLink }} }}'
                )
            }, separators=(",", ":"))
            sig  = hashlib.sha256(
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
                        db_set_link(url, link, "shopee")
                        log_nrm.info(f"  ✅ SHP t={tentativa}: {link}")
                        return link
                    log_nrm.warning(
                        f"  ⚠️ SHP API t={tentativa}: "
                        f"{res.get('errors') or res.get('error')}")
        except Exception as e:
            log_nrm.warning(f"  ⚠️ SHP t={tentativa}: {e}")
        if tentativa < 3:
            await asyncio.sleep(tentativa * 1.5)

    log_nrm.warning(f"  ❌ SHP falhou 3x → None: {url[:60]}")
    return None   # ← não envia link sem afiliação

_CUTTLY_LAST_429: float = 0.0
_CUTTLY_BACKOFF:  float = 65.0

def _afiliar_url_magalu(url: str) -> str:
    p      = urlparse(url)
    path   = re.sub(r'^(/magazine)[^/]+', rf'\1{_MGL_SLUG}', p.path)
    params = {k: v[0] for k, v in parse_qs(p.query, keep_blank_values=True).items()}
    for k in [
        "tag","partnerid","promoterid","afforcedeeplink","deeplinkvalue",
        "isretargeting","partner_id","promoter_id","utm_source","utm_medium",
        "utm_campaign","pid","c","af_force_deeplink","deep_link_value",
    ]:
        params.pop(k, None)
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
    base = urlunparse(p._replace(path=path, query="", fragment=""))
    params["deep_link_value"] = (
        f"{base}?utm_source=divulgador&utm_medium=magalu"
        f"&partner_id={_MGL_PARTNER}&promoter_id={_MGL_PROMOTER}"
    )
    return urlunparse(p._replace(path=path, query=urlencode(params), fragment=""))

async def _cuttly(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    global _CUTTLY_LAST_429
    api = f"https://cutt.ly/api/api.php?key={_CUTTLY_KEY}&short={quote(url, safe='')}"
    for tentativa in range(1, 4):
        espera = _CUTTLY_BACKOFF - (time.time() - _CUTTLY_LAST_429)
        if espera > 0:
            await asyncio.sleep(espera)
        try:
            async with _SEM_HTTP:
                async with sessao.get(api, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status == 429:
                        _CUTTLY_LAST_429 = time.time()
                        await asyncio.sleep(_CUTTLY_BACKOFF); continue
                    if r.status == 401:
                        log_nrm.error("  ❌ Cuttly 401 — chave inválida"); return None
                    if r.status != 200:
                        await asyncio.sleep(2 ** tentativa); continue
                    try:
                        data = await r.json(content_type=None)
                    except Exception:
                        await asyncio.sleep(2 ** tentativa); continue
                    status = data.get("url", {}).get("status")
                    if status in (7, 2):
                        short = data["url"].get("shortLink")
                        if short: return short
                    await asyncio.sleep(2 ** tentativa)
        except asyncio.TimeoutError:
            await asyncio.sleep(2 ** tentativa)
        except Exception as e:
            log_nrm.error(f"  ❌ Cuttly t={tentativa}: {e}")
            await asyncio.sleep(2 ** tentativa)
    return None

async def _cuttly_background(url_longo: str, msg_id_origem: int):
    """
    Tenta encurtar em background após envio.
    Quando consegue, edita a mensagem no destino.
    10 tentativas com intervalo de 45s — não trava o bot.
    """
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as sessao:
        for tent in range(10):
            await asyncio.sleep(45)
            try:
                short = await _cuttly(url_longo, sessao)
                if short:
                    loop    = asyncio.get_event_loop()
                    mapa    = await loop.run_in_executor(_EXECUTOR, ler_mapa)
                    id_dest = mapa.get(str(msg_id_origem))
                    if id_dest:
                        try:
                            msg_atual = await client.get_messages(GRUPO_DESTINO, ids=id_dest)
                            if msg_atual and msg_atual.text:
                                novo = msg_atual.text.replace(url_longo, short)
                                if novo != msg_atual.text:
                                    await client.edit_message(
                                        GRUPO_DESTINO, id_dest, novo, parse_mode="md")
                                    log_nrm.info(f"  ✅ MGL BG editado: {id_dest}")
                        except Exception as e:
                            log_nrm.warning(f"  ⚠️ MGL BG edit: {e}")
                    db_set_link(url_longo, short, "magalu")
                    return
            except Exception as e:
                log_nrm.warning(f"  ⚠️ MGL BG t={tent}: {e}")

async def _afiliar_magalu(url: str, sessao: aiohttp.ClientSession,
                           msg_id: int = 0) -> Optional[str]:
    """
    Motor Magalu.
    Fluxo: cache → desencurtar → validar → afiliar → encurtar → enviar sempre.
    Se Cuttly falhar → envia link longo afiliado + agenda background.
    Nunca retorna None se o link é Magalu válido.
    """
    log_nrm.debug(f"▶ MGL: {url[:80]}")
    cached = db_get_link(url)
    if cached: return cached

    nl = _netloc(url)

    # Desencurta se necessário
if _parece_magalu_encurtado(url) or "maga.lu" in nl or nl in _ENCURTADORES:
    try:
        async with _SEM_HTTP:
            url = await desencurtar(url, sessao)
    except Exception as e:
        log_nrm.error(f"  ❌ MGL desencurtar: {e}")
        return None

cl = _classificar_cached(url)
if cl.plat != "magalu" or cl.tipo == "invalido":
    log_nrm.debug(f"  MGL descartado: plat={cl.plat} tipo={cl.tipo}")
    return None

afiliado = _afiliar_url_magalu(url)

# --- AGORA AS LINHAS ABAIXO ESTÃO ALINHADAS COM O 'afiliado' ---

# Tenta encurtar (3 tentativas com retry interno em _cuttly)
short = await _cuttly(afiliado, sessao)
if short:
    db_set_link(url, short, "magalu")
    log_nrm.info(f"  ✅ MGL curto: {short}")
    return short

# Cuttly falhou → envia longo afiliado + tenta encurtar depois
log_nrm.warning("  ⚠️ Cuttly falhou → longo afiliado + background")
db_set_link(url, afiliado, "magalu")
if msg_id:
    asyncio.create_task(_cuttly_background(afiliado, msg_id))

return afiliado  # ← nunca None para Magalu válido

# ── 3f. Pipeline de normalização ─────────────────────────────────────────────

async def _normalizar_um(
    lc: LinkClassificado,
    sessao: aiohttp.ClientSession,
    msg_id: int = 0,
) -> Tuple[str, Optional[str], str]:
    """
    Retorna (url_original, url_convertida, plat).
    mundial  → url_convertida == url_original (sem conversão)
    claims   → url_convertida == url_original (sem tag)
    preservar→ url_convertida == url_original
    None     → url_convertida = None (descarta)
    """
    plat = lc.plat

    if plat == "mundial":
        return lc.url_original, lc.url_original, "mundial"
    if plat == "preservar":
        return lc.url_original, lc.url_original, "preservar"
    if plat is None or lc.tipo in ("invalido","bloqueado","grupo_externo","desconhecido"):
        return lc.url_original, None, plat or "none"
    if plat == "amazon" and lc.tipo == "claims":
        return lc.url_original, lc.url_original, "amazon"

    cached = db_get_link(lc.url_original)
    if cached: return lc.url_original, cached, plat

    url = lc.url_original

    # Encurtadores genéricos → desencurta e reclassifica
    if plat == "expandir":
        try:
            url = await desencurtar(url, sessao)
        except Exception:
            return lc.url_original, None, "none"
        lc   = _classificar_cached(url)
        plat = lc.plat
        if plat is None:
            return lc.url_original, None, "none"
        if plat == "mundial":
            return lc.url_original, url, "mundial"
        if plat == "amazon" and lc.tipo == "claims":
            return lc.url_original, url, "amazon"
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


@dataclass
class MensagemNormalizada:
    msg_id:      int
    chat:        str
    texto_limpo: str
    mapa:        Dict[str, str]
    preservar:   List[str]
    plat:        str
    cupom:       str
    sku:         str
    tem_midia:   bool
    media_obj:   object


async def normalizar(bruta: MensagemBruta) -> Optional[MensagemNormalizada]:
    """
    Orquestra camada 3.
    ClientSession criado uma vez aqui e passado para todas as funções internas.
    """
    if not bruta.texto.strip(): return None
    if texto_bloqueado(bruta.texto): return None

    texto_limpo = limpar_texto(bruta.texto)
    if not tem_contexto(texto_limpo): return None

    # Limpa cache de classificação do ciclo anterior
    _cls_cache.clear()
    _desc_cache.clear()

    classificados = classificar_links(bruta.links)
    converter     = [lc for lc in classificados
                     if lc.plat not in ("preservar", None)]
    preservar_lst = [lc.url_original for lc in classificados
                     if lc.plat == "preservar"]

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
            # Mundiais e preservar entram no mapa mas não contam como plataforma
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

    log_nrm.info(
        f"✅ {len(mapa)}/{len(converter)} | "
        f"plat={plat_dom} cupom='{cupom}' sku={sku}"
    )
    return MensagemNormalizada(
        msg_id=bruta.msg_id, chat=bruta.chat,
        texto_limpo=texto_limpo, mapa=mapa,
        preservar=preservar_lst, plat=plat_dom,
        cupom=cupom, sku=sku,
        tem_midia=bruta.tem_midia, media_obj=bruta.media_obj,
)

# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 4 — DEDUPLICAÇÃO  (DECISÃO PURA)
# Responsabilidade: True = envia | False = bloqueia.
# NÃO altera dados. NÃO faz parsing. NÃO chama rede.
# ═══════════════════════════════════════════════════════════════════════════════

_RUIDO_NORM = frozenset({"promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora","imperdivel","imperdível",
    "exclusivo","limitado","corra","ative","use","saiu","vazou","resgate","acesse","confira",
    "link","clique","app","relampago","relâmpago","click","veja","novo","nova","valido","válido",
    "somente","apenas","ate","até","partir","ainda","volta","ativo","disponivel","disponível",
    "pix","parcelas","unidades","estoque"})
_RE_EMJ_NORM = re.compile(r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF]+", re.UNICODE)
_SIM_FORTE   = 0.88
_SIM_MEDIO   = 0.75

def _fp(s: str) -> str: return hashlib.sha256(s.encode()).hexdigest()[:32]

def _rm_acentos(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", t) if unicodedata.category(c) != "Mn")

def _alma(t: str) -> str:
    t = _rm_acentos(t.lower())
    t = re.sub(r'https?://\S+', ' ', t)
    t = _RE_EMJ_NORM.sub(' ', t)
    t = re.sub(r'(\d+\s?(gb|tb|mah|v|w|hz|fps))', r' ATTR_\1 ', t)
    t = re.sub(r'r\$\s*[\d.,]+', ' VALOR ', t)
    t = re.sub(r'\b\d+%', ' PCT ', t)
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return ' '.join(sorted(w for w in t.split() if w not in _RUIDO_NORM and (len(w) > 2 or "attr_" in w)))

def _cupons(t: str) -> frozenset:
    return frozenset(re.findall(r'\b([A-Z0-9_-]{4,20})\b', t))

def _benef(t: str) -> frozenset:
    b = set()
    if re.search(r'frete\s+gr[aá]t', t, re.I): b.add("frete_gratis")
    for m in re.findall(r'(\d+)\s*%?\s*off', t, re.I): b.add(f"off_{m}")
    return frozenset(b)

def _sim(a: str, b: str) -> float:
    if not a or not b: return 0.0
    if min(len(a), len(b)) / max(len(a), len(b)) < 0.7: return 0.0
    return SequenceMatcher(None, a, b).ratio()

def _eh_reativacao(t: str) -> bool:
    return bool(re.search(r'voltou|reativado|dispon[ií]vel novamente|estoque', t, re.I))


def deve_enviar(norm: MensagemNormalizada) -> bool:
    """
    Decisão pura de deduplicação.
    Recebe MensagemNormalizada, retorna True/False.
    """
    try:
        texto = norm.texto_limpo
        plat  = norm.plat
        cupons = _cupons(texto)
        alma_v = _alma(texto)
        benef  = _benef(texto)
        asin   = _extrair_asin_texto(texto, norm.mapa)
        id_mgl = _extrair_id_magalu(texto, norm.mapa)

        # Reativação — bloqueia se já visto nessa janela
        if _eh_reativacao(texto):
            fp_re = _fp(f"reativ_{plat}_{'|'.join(sorted(cupons))}_{asin}_{id_mgl}")
            if db_get_dedupe(fp_re):
                log_ded.info("🔁 [REATIVAÇÃO BLOQ]"); return False
            db_set_dedupe(fp_re, plat, list(cupons), alma_v, "reativ", asin, id_mgl, list(benef))
            return True

        # Amazon — ASIN único
        if asin:
            fp = f"amz_{asin}"
            if db_get_dedupe(fp): log_ded.info(f"🔁 [AMZ-ASIN] {asin}"); return False
            db_set_dedupe(fp, plat, list(cupons), alma_v, "amz", asin, "", list(benef)); return True

        # Magalu — ID produto único
        if id_mgl:
            fp = f"mgl_{id_mgl}"
            if db_get_dedupe(fp): log_ded.info("🔁 [MGL-ID]"); return False
            db_set_dedupe(fp, plat, list(cupons), alma_v, "mgl", "", id_mgl, list(benef)); return True

        # Cupom
        fp_cup = None
        if cupons:
            fp_cup = _fp(f"{plat}|cup|{'|'.join(sorted(cupons))}|{asin or id_mgl}")
            if db_get_dedupe(fp_cup): log_ded.info(f"🔁 [CUPOM BLOQ] {plat}"); return False

        # Benefício
        fp_ben = None
        if benef:
            fp_ben = _fp(f"{plat}|ben|{'|'.join(sorted(benef))}|{asin or id_mgl}")
            if db_get_dedupe(fp_ben): log_ded.info("🔁 [BENEF BLOQ]"); return False

        # Similaridade na janela rápida
        for e in db_buscar_janela_rapida(plat):
            sim_v = _sim(alma_v, e.get("alma",""))
            if (cupons and cupons & set(e.get("cupons",[])) and sim_v > _SIM_MEDIO) or sim_v > _SIM_FORTE:
                log_ded.info(f"🔁 [SIM {sim_v:.2f}] {plat}"); return False

        # Registrar
        fp_final = _fp(f"{plat}|{alma_v}|{'|'.join(sorted(cupons))}|{'|'.join(sorted(benef))}")
        db_set_dedupe(fp_final, plat, list(cupons), alma_v, "gen", asin, id_mgl, list(benef))
        if fp_cup: db_set_dedupe(fp_cup, plat, list(cupons), alma_v, "cup", asin, id_mgl, list(benef))
        if fp_ben: db_set_dedupe(fp_ben, plat, list(cupons), alma_v, "ben", asin, id_mgl, list(benef))
        return True

    except Exception as e:
        log_ded.error(f"❌ ERRO DEDUPE: {e}"); return True


# ── Anti-saturação (parte da decisão) ───────────────────────────────────────

_SAT_MAX_PLAT  = 10
_SAT_BURST_LIM = 6
_SAT_BURST_JAN = 60
_burst: List[float] = []
_burst_lock: asyncio.Lock = None  # type: ignore
_KW_EVENTO = re.compile(r'\b(?:quiz|roleta|miss[aã]o|arena|girar|gire|roda|jogar|jogue|desafio)\b', re.I)

async def _burst_add():
    async with _burst_lock:
        agora = time.monotonic(); _burst.append(agora)
        while _burst and agora - _burst[0] > _SAT_BURST_JAN: _burst.pop(0)

async def _burst_count() -> int:
    async with _burst_lock:
        agora = time.monotonic(); return sum(1 for t in _burst if agora - t <= _SAT_BURST_JAN)

async def delay_saturacao(plat: str, texto: str) -> float:
    if _KW_EVENTO.search(texto): return 0.0
    delay = 0.0
    if db_count_sat(plat) >= _SAT_MAX_PLAT: delay += 6.0
    if await _burst_count() >= _SAT_BURST_LIM: delay += 4.0
    return delay


# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 5 — MONTAGEM (FORMATTER)
# Responsabilidade: construir mensagem final bonita + buscar imagem.
# NÃO decide envio. NÃO acessa Telegram. NÃO altera banco.
# ═══════════════════════════════════════════════════════════════════════════════

_EMJ: Dict[str, List[str]] = {
    "titulo_oferta":["🔥"],"titulo_cupom":["🚨"],"titulo_evento":["⚠️"],
    "preco":["💵"],"cupom_cod":["🎟"],"resgate":["✅"],
    "carrinho":["🛒"],"frete":["🚚","📦"],"multi_item":["🔹"],
}
_EMJ_IDX: Dict[str, int] = {k: 0 for k in _EMJ}

def _prox_emoji(cat: str) -> str:
    lst = _EMJ[cat]; idx = _EMJ_IDX[cat]; e = lst[idx % len(lst)]
    _EMJ_IDX[cat] = (idx + 1) % len(lst); return e

_KW_PRECO    = re.compile(r'R\$\s?[\d.,]+', re.I)
_KW_FRETE    = re.compile(r'\b(?:frete\s+gr[aá]t|entrega\s+gr[aá]t|sem\s+frete|frete\s+0)\b', re.I)
_KW_STATUS   = re.compile(r'\b(?:voltando|voltou|normalizou|renovado|ainda\s+ativo|de\s+volta|reativado)\b', re.I)
_KW_RESGATE  = re.compile(r'\b(?:resgate|clique|acesse|ative|use\s+o\s+cupom)\b', re.I)
_KW_CARRINHO = re.compile(r'\b(?:carrinho|cart)\b', re.I)

_RE_LIXO_PREF  = re.compile(r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:|[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?)\s*', re.I)
_RE_ANUNCIO    = re.compile(r'^\s*[-#]?\s*(?:an[uú]ncio|publicidade|patrocinado)\s*$', re.I)
_RE_URL_RENDER = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c]+')

def _contar_produtos(texto: str) -> int:
    return sum(1 for l in texto.splitlines() if _KW_PRECO.search(l))

def _emoji_linha(linha: str, eh_titulo: bool, is_multi: bool = False) -> Optional[str]:
    if _tem_emoji(linha): return None
    if eh_titulo:
        if _KW_EVENTO.search(linha): return _prox_emoji("titulo_evento")
        if _KW_CUPOM.search(linha): return _prox_emoji("titulo_cupom")
        return _prox_emoji("titulo_oferta")
    if is_multi and _KW_PRECO.search(linha): return "🔹"
    if _KW_FRETE.search(linha): return _prox_emoji("frete")
    if _KW_CUPOM.search(linha): return _prox_emoji("cupom_cod")
    if _KW_PRECO.search(linha): return _prox_emoji("preco")
    if _KW_RESGATE.search(linha): return _prox_emoji("resgate")
    if _KW_CARRINHO.search(linha): return _prox_emoji("carrinho")
    return None

def _crases(linha: str) -> str:
    if "http" in linha or "`" in linha: return linha
    if not (_KW_CUPOM.search(linha) or _KW_COD.search(linha)): return linha
    def _sub(m: re.Match) -> str:
        c = m.group(0); return c if (c in _FALSO_CUPOM or len(c) < 4) else f"`{c}`"
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
        l = _RE_URL_RENDER.sub(lambda m: mapa.get(m.group(0).rstrip('.,;)>'), ""), l).strip()
        if not l: continue
        l = _crases(l)
        if not _tem_emoji(l):
            e = _emoji_linha(l, eh_titulo=primeiro, is_multi=is_multi)
            if e: l = f"{e} {l}"
        primeiro = False; saida.append(l)
    return "\n".join(saida).strip()


async def buscar_imagem_produto(url: str) -> Optional[str]:
    if not url or not url.startswith("http"): return None
    hdrs = {"User-Agent": random.choice(USER_AGENTS), "Accept": "text/html,*/*;q=0.9"}
    for t in range(1, 4):
        try:
            async with aiohttp.ClientSession(headers=hdrs) as s:
                async with s.get(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    ct = r.headers.get("content-type","")
                    if "image" in ct: return str(r.url)
                    html = await r.text(errors="ignore"); soup = BeautifulSoup(html,"html.parser")
                    for attr in [{"property":"og:image"},{"property":"og:image:secure_url"},{"name":"twitter:image"}]:
                        tag = soup.find("meta", attrs=attr)
                        if not tag: continue
                        img_url = tag.get("content","")
                        if not img_url.startswith("http"): continue
                        img_url = re.sub(r'[?&](?:width|height|w|h|size|resize|fit|quality|q|maxwidth|maxheight|format|auto|compress|crop|scale)=[^&]+','',img_url).rstrip('?&')
                        return img_url
                    for scr in soup.find_all("script", type="application/ld+json"):
                        try:
                            data  = json.loads(scr.string or "")
                            items = data if isinstance(data, list) else [data]
                            for item in items:
                                img = item.get("image")
                                if isinstance(img, str) and img.startswith("http"): return img
                                if isinstance(img, list) and img:
                                    c = img[0]
                                    if isinstance(c, str): return c
                                    if isinstance(c, dict):
                                        u = c.get("url","")
                                        if u.startswith("http"): return u
                        except Exception: pass
                    melhor_src = None; melhor_area = 0
                    for img_tag in soup.find_all("img", src=True):
                        src = img_tag.get("src","")
                        if not src.startswith("http"): continue
                        if any(x in src.lower() for x in ["icon","logo","avatar","badge","spinner"]): continue
                        try:
                            w = int(img_tag.get("width",0)); h = int(img_tag.get("height",0)); area = w * h
                            if area > melhor_area: melhor_area = area; melhor_src = src
                        except (ValueError, TypeError):
                            if any(x in src.lower() for x in ["product","produto","item","image","foto","zoom","large","xl","hd","original"]):
                                if not melhor_src: melhor_src = src
                    if melhor_src: return melhor_src
        except asyncio.TimeoutError: log_fmt.warning(f"⏱ Timeout buscar_img t={t}")
        except Exception as e: log_fmt.warning(f"⚠️ buscar_img t={t}: {e}")
        if t < 3: await asyncio.sleep(1.0)
    return None


async def preparar_imagem_tg(media_obj) -> Optional[object]:
    try:
        buf = io.BytesIO()
        res = await client.download_media(media_obj, file=buf)
        if res is None: return None
        buf.seek(0)
        if buf.getbuffer().nbytes < 500: return None
        buf.name = "imagem.jpg"; return buf
    except Exception as e: log_fmt.warning(f"⚠️ download_media: {e}"); return None


async def preparar_imagem_url(url: str) -> Optional[object]:
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": random.choice(USER_AGENTS)}) as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=20), allow_redirects=True) as r:
                if r.status == 200:
                    data = await r.read()
                    if len(data) < 1000: return None
                    buf = io.BytesIO(data); buf.name = "produto.jpg"; return buf
    except Exception as e: log_fmt.warning(f"⚠️ preparar_img_url: {e}")
    return None


def _eh_cupom_exclusivo_amazon(norm: MensagemNormalizada) -> bool:
    if norm.plat != "amazon": return False
    if not _KW_CUPOM.search(norm.texto_limpo): return False
    tl = norm.texto_limpo.lower()
    return "shopee" not in tl and "magalu" not in tl and "magazine" not in tl


@dataclass
class MensagemMontada:
    msg_id:    int
    chat:      str
    plat:      str
    sku:       str
    texto:     str
    imagem:    object       # BytesIO | str (path) | None
    mapa:      Dict[str, str]
    msg_id_origem: int


async def montar(norm: MensagemNormalizada) -> MensagemMontada:
    texto = montar_texto(norm)
    img   = None

    if norm.tem_midia:
        img = await preparar_imagem_tg(norm.media_obj)

    if img is None:
        eh_cupom = bool(_KW_CUPOM.search(norm.texto_limpo))
        if norm.mapa and not eh_cupom:
            img_url = await buscar_imagem_produto(list(norm.mapa.values())[0])
            if img_url:
                img = await preparar_imagem_url(img_url)
        if img is None and eh_cupom:
            if norm.plat == "shopee" and os.path.exists(_IMG_SHP):
                img = _IMG_SHP
            elif _eh_cupom_exclusivo_amazon(norm) and os.path.exists(_IMG_AMZ):
                img = _IMG_AMZ
            elif norm.plat == "magalu" and os.path.exists(_IMG_MGL):
                img = _IMG_MGL

    return MensagemMontada(
        msg_id=norm.msg_id, chat=norm.chat, plat=norm.plat,
        sku=norm.sku, texto=texto, imagem=img,
        mapa=norm.mapa, msg_id_origem=norm.msg_id,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 6 — ENVIO (OUTPUT)
# Responsabilidade: enviar no Telegram. NÃO pensa em lógica nenhuma.
# ═══════════════════════════════════════════════════════════════════════════════

_RATE_LOCK:    asyncio.Lock = None  # type: ignore
_IDS_PROC:     set          = set()
_IDS_LOCK:     asyncio.Lock = None  # type: ignore
_ULTIMO_ENV_TS = 0.0

def _intervalo_atual() -> float:
    return 0.5 if 8 <= int(time.strftime("%H")) < 22 else 1.0

async def _rate_limit():
    global _ULTIMO_ENV_TS
    async with _RATE_LOCK:
        agora  = time.monotonic()
        espera = _intervalo_atual() - (agora - _ULTIMO_ENV_TS)
        if espera > 0: await asyncio.sleep(espera)
        _ULTIMO_ENV_TS = time.monotonic()

async def _marcar(msg_id: int):
    async with _IDS_LOCK:
        _IDS_PROC.add(msg_id)
        if len(_IDS_PROC) > 5000:
            for _ in range(len(_IDS_PROC) - 4000): _IDS_PROC.pop()

async def _foi_processado(msg_id: int) -> bool:
    async with _IDS_LOCK: return msg_id in _IDS_PROC


async def _enviar_msg(texto: str, img) -> object:
    if img:
        if len(texto) <= 1024:
            try:
                return await client.send_file(GRUPO_DESTINO, img,
                    caption=texto, parse_mode="md", force_document=False)
            except Exception as e:
                log_out.warning(f"⚠️ send_file+caption: {e}")
                try:
                    await client.send_file(GRUPO_DESTINO, img, force_document=False)
                    return await client.send_message(GRUPO_DESTINO, texto, parse_mode="md", link_preview=True)
                except Exception as e2: log_out.warning(f"⚠️ send_file sem caption: {e2}")
        else:
            try:
                await client.send_file(GRUPO_DESTINO, img, force_document=False)
                return await client.send_message(GRUPO_DESTINO, texto, parse_mode="md", link_preview=False)
            except Exception as e: log_out.warning(f"⚠️ send_file longo: {e}")
    return await client.send_message(GRUPO_DESTINO, texto, parse_mode="md", link_preview=True)


async def enviar(montada: MensagemMontada) -> bool:
    """Envia mensagem nova. Retorna True em sucesso."""
    await _rate_limit()
    async with _SEM_ENVIO:
        loop = asyncio.get_event_loop()
        sent = None
        img  = montada.imagem
        for t in range(1, 4):
            try: sent = await _enviar_msg(montada.texto, img); break
            except FloodWaitError as e: await asyncio.sleep(e.seconds)
            except Exception as e:
                log_out.error(f"❌ envio t={t}: {e}")
                if t == 1: img = None
                elif t < 3: await asyncio.sleep(2 ** t)
        if not sent: log_out.error(f"❌ Envio falhou | @{montada.chat}"); return False

        mp = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        mp[str(montada.msg_id)] = sent.id
        try: await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
        except Exception as e: log_sys.error(f"❌ salvar_mapa: {e}")

        await _marcar(montada.msg_id)
        db_registrar_sat(montada.plat, montada.sku)
        try: await _burst_add()
        except Exception: pass

        # Agendamento edição Magalu (link curto em background)
        if montada.plat == "magalu" and montada.mapa:
            for orig, conv in montada.mapa.items():
                if "partner_id" in conv and "cutt.ly" not in conv:
                    try: asyncio.create_task(_cuttly_background(conv, montada.msg_id))
                    except Exception: pass

        log_out.info(f"🚀 [OK] @{montada.chat}→{GRUPO_DESTINO} | {montada.msg_id}→{sent.id} | {montada.plat.upper()} sku={montada.sku}")
        return True


async def editar(msg_id_origem: int, texto_novo: str) -> bool:
    """Edita mensagem já enviada."""
    loop = asyncio.get_event_loop()
    mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
    id_d = mp.get(str(msg_id_origem))
    if not id_d: return False
    await _rate_limit()
    async with _SEM_ENVIO:
        for t in range(1, 4):
            try:
                await client.edit_message(GRUPO_DESTINO, id_d, texto_novo, parse_mode="md")
                log_out.info(f"✏️ Editado | dest_id={id_d}")
                return True
            except MessageNotModifiedError: return True
            except FloodWaitError as e: await asyncio.sleep(e.seconds)
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
    try: bruta = ingerir(event)
    except Exception as e: log_sys.error(f"❌ ingestao: {e}"); return

    log_sys.info(f"{'✏️' if is_edit else '📩'} @{bruta.chat} | id={msg_id} | q={len(_buf)} w={_w_ativos}")

    # ── Camadas 2+3: Classificação + Normalização ───────────────────
    try: norm = await normalizar(bruta)
    except Exception as e: log_sys.error(f"❌ normalizar: {e}"); return
    if norm is None: return

    # ── Camada 4: Deduplicação ──────────────────────────────────────
    if not is_edit:
        try:
            if not deve_enviar(norm): return
        except Exception as e: log_sys.error(f"❌ deve_enviar: {e}"); return

        try:
            delay = await delay_saturacao(norm.plat, norm.texto_limpo)
            if delay > 0: await asyncio.sleep(delay)
        except Exception as e: log_sys.error(f"❌ saturacao: {e}")

    # ── Camada 5: Montagem ──────────────────────────────────────────
    try: montada = await montar(norm)
    except Exception as e: log_sys.error(f"❌ montar: {e}"); return

    # ── Camada 6: Envio ─────────────────────────────────────────────
    if is_edit:
        await editar(msg_id, montada.texto)
    else:
        await enviar(montada)


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
    log_sys.info("🔌 Conectando...")
    await client.connect()
    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida"); return False
    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 {GRUPOS_ORIGEM} → {GRUPO_DESTINO}")
    log_sys.info(f"🟠 Amazon: {_AMZ_TAG} | 🟣 Shopee: {_SHP_APP_ID} | 🔵 Magalu: {_MGL_PROMOTER}/{_MGL_SLUG}")
    log_sys.info(f"🖼  Pillow: {'OK' if _PIL_OK else 'OFF'}")
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
    await _iniciar_orchestrator()
    await client.run_until_disconnected()
    return True

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

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║   FOGUETÃO v74.0 — ARQUITETURA SÊNIOR CONSOLIDADA                      ║
# ║   Pipeline único · Dedup semântica · Isolamento total por plataforma    ║
# ╚══════════════════════════════════════════════════════════════════════════╝
#
# DECISÃO CENTRAL: cupom + texto + contexto semântico
# NUNCA: link, emoji, preço, formatação, encurtador
#
# ORDEM DOS MÓDULOS
#  1  Logs
#  2  Configurações
#  3  Persistência JSON
#  4  Filtro de texto
#  5  Whitelist / classificação de domínio
#  6  Desencurtador
#  7  Motor Amazon (isolado)
#  8  Motor Shopee  (isolado)
#  9  Motor Magalu  (isolado)
# 10  Extração de links
# 11  Pipeline de conversão paralela
# 12  Limpeza de ruído textual
# 13  Emojis + radares semânticos
# 14  Renderizador
# 15  Deduplicação semântica (fingerprint DNA)
# 16  Buscador de imagem
# 17  Rate-limit interno
# 18  Anti-loop de edição
# 19  Envio (prioridade imagem)
# 20  Banco central SQLite
# 21  Parser ultra robusto
# 22  Scheduler inteligente
# 23  Anti-saturação algorítmica
# 24  Buffer Orchestrator + pipeline principal
# 25  Health check
# 26  Inicialização com auto-restart

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import heapq
import io
import json
import logging
import os
import random
import re
import sqlite3
import time
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass
from difflib import SequenceMatcher
from threading import Lock
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

import aiohttp
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


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 1 ▸ LOGS
# ══════════════════════════════════════════════════════════════════════════════

def _mk_log(nome: str, cor: str) -> logging.Logger:
    lg = logging.getLogger(nome)
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            f'\033[{cor}m[%(name)-10s]\033[0m %(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'))
        lg.addHandler(h)
        lg.setLevel(logging.DEBUG)
    return lg

log_amz  = _mk_log('AMAZON',   '1;33')
log_shp  = _mk_log('SHOPEE',   '1;38;5;208')
log_mgl  = _mk_log('MAGALU',   '1;34')
log_dedup= _mk_log('DEDUP',    '1;35')
log_img  = _mk_log('IMAGEM',   '1;36')
log_tg   = _mk_log('TELEGRAM', '1;32')
log_fil  = _mk_log('FILTRO',   '1;31')
log_lnk  = _mk_log('LINKS',    '1;38;5;51')
log_fmt  = _mk_log('FORMAT',   '1;33')
log_sys  = _mk_log('SISTEMA',  '1;37')
log_hc   = _mk_log('HEALTH',   '1;38;5;118')


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 2 ▸ CONFIGURAÇÕES
# ══════════════════════════════════════════════════════════════════════════════

API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION", "")

GRUPOS_ORIGEM  = ["promotom", "fumotom", "botofera", "fadadoscupons"]
GRUPO_DESTINO  = "@ofertap"

# Credenciais isoladas — NUNCA misturar entre plataformas
_AMZ_TAG      = os.environ.get("AMAZON_TAG",         "leo21073-20")
_SHP_APP_ID   = os.environ.get("SHOPEE_APP_ID",      "18348480261")
_SHP_SECRET   = os.environ.get("SHOPEE_SECRET",      "")
_MGL_PARTNER  = os.environ.get("MAGALU_PARTNER_ID",  "3440")
_MGL_PROMOTER = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
_MGL_PID      = os.environ.get("MAGALU_PID",         "magazinevoce")
_MGL_SLUG     = os.environ.get("MAGALU_SLUG",        "magazineleo12")
_CUTTLY_KEY   = os.environ.get("CUTTLY_API_KEY",     "")

_IMG_AMZ = "cupom-amazon.jpg"
_IMG_MGL = "magalu_promo.jpg"

ARQUIVO_CACHE      = "cache_dedup.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

_SEM_ENVIO     = asyncio.Semaphore(3)
_SEM_HTTP      = asyncio.Semaphore(20)
_EXECUTOR      = concurrent.futures.ThreadPoolExecutor(max_workers=4)
_RATE_LOCK     = asyncio.Lock()
_ULTIMO_ENV_TS = 0.0
_INTERVALO_MIN = 1.5

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

_ENCURTADORES = frozenset([
    "bit.ly", "cutt.ly", "tinyurl.com", "t.co", "ow.ly",
    "goo.gl", "rb.gy", "is.gd", "tiny.cc", "buff.ly",
    "short.io", "bl.ink", "rebrand.ly", "shorturl.at",
])

_PRESERVE = frozenset(["wa.me", "api.whatsapp.com"])
_DELETAR  = frozenset(["t.me", "telegram.me", "telegram.org", "chat.whatsapp.com"])


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 3 ▸ PERSISTÊNCIA JSON
# ══════════════════════════════════════════════════════════════════════════════

_MAP_LOCK   = Lock()
_CACHE_LOCK = Lock()

def _ler_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_sys.error(f"❌ Ler {path}: {e}")
        return {}

def _gravar_json(path: str, data: dict, lock: Lock):
    with lock:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log_sys.error(f"❌ Gravar {path}: {e}")

ler_mapa     = lambda: _ler_json(ARQUIVO_MAPEAMENTO)
salvar_mapa  = lambda m: _gravar_json(ARQUIVO_MAPEAMENTO, m, _MAP_LOCK)
ler_cache    = lambda: _ler_json(ARQUIVO_CACHE)
salvar_cache = lambda c: _gravar_json(ARQUIVO_CACHE, c, _CACHE_LOCK)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 4 ▸ FILTRO DE TEXTO
# Produto único na lista → bloqueia.
# Múltiplos produtos na mesma mensagem → passa sempre.
# ══════════════════════════════════════════════════════════════════════════════

_FILTRO_TEXTO = [
    "Monitor Samsung", "Fonte Mancer", "Placa de video", "Monitor LG",
    "PC home Essential", "Suporte articulado", "Gabinetes em oferta",
    "VHAGAR", "Superframe", "AM5", "AM4", "GTX", "DDR5", "DDR4",
    "Dram", "Monitor Safe", "Monitor Redragon", "CL18", "CL16", "CL32",
    "MT/s", "MHz", "RX 580", "Ryzen", "Placa Mãe", "Gabinete Gamer",
    "Water Cooler", "Air Cooler",
]

_RE_MULTI_OFERTA = re.compile(
    r'\b(?:ofertas?|promoções?)\s+(?:na\s+|no\s+|da\s+)?'
    r'(?:shopee|amazon|magalu|magazine\s*luiza)\b',
    re.I,
)

# Detecta múltiplos produtos: 3+ linhas com preço OU 3+ URLs na mesma msg
_RE_PRECO_LINHA = re.compile(r'R\$\s?[\d.,]+')
_RE_URL_COUNT   = re.compile(r'https?://')

def _eh_multi_produto(texto: str) -> bool:
    """Retorna True se a mensagem contém vários produtos."""
    if _RE_MULTI_OFERTA.search(texto):
        return True
    linhas_preco = sum(1 for l in texto.splitlines()
                       if _RE_PRECO_LINHA.search(l))
    urls         = len(_RE_URL_COUNT.findall(texto))
    return linhas_preco >= 2 or urls >= 3

def texto_bloqueado(texto: str) -> bool:
    """
    Multi-produto → nunca bloqueia.
    Produto único → bloqueia se tiver palavra da lista.
    """
    if _eh_multi_produto(texto):
        log_fil.debug("✅ Multi-produto — bypass filtro")
        return False
    tl = texto.lower()
    for p in _FILTRO_TEXTO:
        if p.lower() in tl:
            log_fil.debug(f"🚫 Filtro bloqueou: '{p}'")
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 5 ▸ WHITELIST + CLASSIFICAÇÃO DE DOMÍNIO
# ══════════════════════════════════════════════════════════════════════════════

def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""

def _eh_link_grupo_externo(url: str) -> bool:
    nl = _netloc(url)
    return any(nl == d or nl.endswith("." + d) for d in _DELETAR)

def classificar(url: str) -> Optional[str]:
    nl = _netloc(url)
    if not nl:
        return None
    if _eh_link_grupo_externo(url):
        return None
    for d in _PRESERVE:
        if nl == d or nl.endswith("." + d):
            return "preservar"
    for dom in ("magazineluiza.com.br", "sacola.magazineluiza.com.br",
                "magazinevoce.com.br", "maga.lu"):
        if nl == dom or nl.endswith("." + dom):
            return "magalu"
    for dom in ("amazon.com.br", "amzn.to", "amzn.com", "a.co"):
        if nl == dom or nl.endswith("." + dom):
            return "amazon"
    for dom in ("shopee.com.br", "s.shopee.com.br", "shopee.com", "shope.ee"):
        if nl == dom or nl.endswith("." + dom):
            return "shopee"
    for enc in _ENCURTADORES:
        if nl == enc or nl.endswith("." + enc):
            return "expandir"
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 6 ▸ DESENCURTADOR MÁXIMO
# Descasca até 15 camadas. Suporta amzlink.to, bit.ly, cutt.ly e qualquer outro.
# HEAD → GET → meta-refresh → JS location → canônico → força GET final.
# Shopee NUNCA chama esta função.
# ══════════════════════════════════════════════════════════════════════════════

_AMZLINK_DOMAINS = frozenset([
    "amzlink.to", "amzon.to", "amzn.to", "a.co", "amzn.com",
])

async def desencurtar(url: str, sessao: aiohttp.ClientSession,
                       depth: int = 0) -> str:
    if depth > 15:
        log_lnk.warning(f"⚠️ Profundidade máxima atingida: {url[:60]}")
        return url

    # Normaliza — remove espaços e caracteres inválidos
    url = url.strip().rstrip('.,;)>')

    hdrs = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    try:
        # ── Tentativa 1: HEAD rápido ──────────────────────────────────────
        try:
            async with sessao.head(
                url, headers=hdrs, allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=10),
                max_redirects=20,
            ) as r:
                final = str(r.url)
                if final != url:
                    log_lnk.debug(f"  HEAD d={depth} → {final[:70]}")
                    return await desencurtar(final, sessao, depth + 1)
                # HEAD retornou a mesma URL — pode ser que precise de GET
                # para amzlink.to e similares que bloqueiam HEAD
                nl = _netloc(url)
                if nl in _AMZLINK_DOMAINS or nl.endswith(".amzlink.to"):
                    raise Exception("amzlink — força GET")
                return final
        except Exception:
            pass  # cai no GET

        # ── Tentativa 2: GET completo ─────────────────────────────────────
        async with sessao.get(
            url, headers=hdrs, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=15),
            max_redirects=20,
        ) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")

            # Já chegou no destino pelo redirect HTTP
            if pos != url:
                log_lnk.debug(f"  GET-redirect d={depth} → {pos[:70]}")
                return await desencurtar(pos, sessao, depth + 1)

            soup = BeautifulSoup(html, "html.parser")

            # Meta refresh
            ref = soup.find("meta",
                            attrs={"http-equiv": re.compile("refresh", re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)",
                              ref["content"], re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    log_lnk.debug(f"  META d={depth} → {novo[:70]}")
                    return await desencurtar(novo, sessao, depth + 1)

            # JS location (amzlink.to usa isso)
            for pat in [
                r'window\.location(?:\.href)?\s*=\s*["\']([^"\']{10,})["\']',
                r'location\.replace\s*\(\s*["\']([^"\']{10,})["\']\s*\)',
                r'location\.assign\s*\(\s*["\']([^"\']{10,})["\']\s*\)',
                r'var\s+url\s*=\s*["\']([^"\']{10,})["\']',
                r'window\.open\s*\(\s*["\']([^"\']{10,})["\']',
            ]:
                mj = re.search(pat, html)
                if mj:
                    destino = mj.group(1)
                    if destino.startswith("http"):
                        log_lnk.debug(f"  JS d={depth} → {destino[:70]}")
                        return await desencurtar(destino, sessao, depth + 1)

            # Link canônico
            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href"):
                href = canon["href"]
                if href.startswith("http") and href != url:
                    log_lnk.debug(f"  CANON d={depth} → {href[:70]}")
                    return await desencurtar(href, sessao, depth + 1)

            # Último recurso: og:url
            og = soup.find("meta", attrs={"property": "og:url"})
            if og and og.get("content", "").startswith("http"):
                if og["content"] != url:
                    log_lnk.debug(f"  OG:URL d={depth} → {og['content'][:70]}")
                    return await desencurtar(og["content"], sessao, depth + 1)

            return pos

    except asyncio.TimeoutError:
        log_lnk.warning(f"⏱ Timeout d={depth}: {url[:70]}")
        return url
    except Exception as e:
        log_lnk.error(f"❌ Desencurtar d={depth}: {e} | {url[:60]}")
        return url


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 7 ▸ MOTOR AMAZON — ISOLADO
# ÚNICA função que usa _AMZ_TAG.
# Limpa a URL deixando só o necessário — nem curto demais, nem longo demais.
# Suporta amzlink.to via desencurtador de 15 camadas.
# ══════════════════════════════════════════════════════════════════════════════

_AMZ_LIXO = frozenset({
    "ascsubtag","btn_ref","ref_","ref","smid","sprefix","spla",
    "dchild","linkcode","linkid","camp","creative",
    "pf_rd_p","pf_rd_r","pd_rd_wg","pd_rd_w","content-id",
    "pd_rd_r","pd_rd_i","ie","qid","_encoding","dib","dib_tag",
    "m","marketplaceid","ufe","th","psc","ingress","visitid",
    "lp_context_asin","redirectasin","redirectmerchantid",
    "redirectasincustomeraction","ds","rnid","sr",
})
# Parâmetros FUNCIONAIS que devem ser mantidos em buscas/eventos
_AMZ_MANTER = frozenset({"tag","keywords","node","k","i","rh","n","field-keywords"})

# Domínios que precisam de desencurtamento
_AMZ_ENCURTADOS = frozenset({
    "amzlink.to","amzn.to","a.co","amzn.com",
})

def _limpar_url_amazon(url: str) -> str:
    """
    Reconstrói a URL Amazon por tipo.
    - Produto /dp/ASIN        → amazon.com.br/dp/ASIN?tag=
    - /gp/product/ASIN        → amazon.com.br/dp/ASIN?tag=
    - /promotion/psp/         → preserva path + tag
    - Busca /s? /b? /deals    → preserva path + params funcionais + tag
    - Genérico                → remove lixo + tag
    """
    p    = urlparse(url)
    path = p.path

    # Produto direto
    m = re.match(r'(/dp/[A-Z0-9]{10})', path)
    if m:
        return urlunparse(p._replace(
            scheme="https", netloc="www.amazon.com.br",
            path=m.group(1), query=f"tag={_AMZ_TAG}", fragment=""))

    m = re.match(r'/gp/product/([A-Z0-9]{10})', path)
    if m:
        return urlunparse(p._replace(
            scheme="https", netloc="www.amazon.com.br",
            path=f"/dp/{m.group(1)}", query=f"tag={_AMZ_TAG}", fragment=""))

    # Campanha
    if "/promotion/psp/" in path:
        return urlunparse(p._replace(
            query=f"tag={_AMZ_TAG}", fragment=""))

    # Busca/evento — mantém só params funcionais
    params = {}
    for k, v in parse_qs(p.query, keep_blank_values=False).items():
        kl = k.lower()
        if kl in _AMZ_MANTER:
            params[k] = v[0]
        elif kl not in _AMZ_LIXO and len(v[0]) < 60:
            params[k] = v[0]
    params["tag"] = _AMZ_TAG

    return urlunparse(p._replace(
        query=urlencode(params), fragment=""))


async def motor_amazon(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """Motor exclusivo Amazon. Debug isolado com log_amz."""
    nl = _netloc(url)
    log_amz.debug(f"▶ IN: {url[:80]}")

    # Sempre desencurta — garante URL final real antes de limpar
    async with _SEM_HTTP:
        exp = await desencurtar(url, sessao)

    log_amz.debug(f"  EXP: {exp[:80]}")

    if classificar(exp) != "amazon":
        log_amz.warning(f"  ⚠️ Não é Amazon após expansão: {exp[:70]}")
        return None

    final = _limpar_url_amazon(exp)
    log_amz.info(f"  ✅ OUT: {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ MOTOR SHOPEE — ISOLADO
# Chega pronta. NUNCA desencurta. API GraphQL oficial. Retry 3x.
# Retorna None em falha — pipeline descarta link sem comissão.
# ══════════════════════════════════════════════════════════════════════════════

async def motor_shopee(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    log_shp.debug(f"🔗 {url[:80]}")
    for t in range(1, 4):
        try:
            ts      = str(int(time.time()))
            payload = json.dumps(
                {"query": (
                    f'mutation {{ generateShortLink(input: '
                    f'{{ originUrl: "{url}" }}) {{ shortLink }} }}'
                )},
                separators=(",", ":"),
            )
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
                        log_shp.info(f"✅ {link}")
                        return link
                    raise ValueError("shortLink vazio")
        except Exception as e:
            log_shp.warning(f"⚠️ t={t}/3: {e}")
            await asyncio.sleep(2 ** t)

    log_shp.error("❌ Shopee API falhou 3x — link descartado")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 9 ▸ MOTOR MAGALU — ISOLADO
# Troca MAGALU_SLUG em qualquer formato:
#   /magazineXXX/produto/p/ID/   → /magazineleo12/produto/p/ID/
#   /magazineXXX/lojista/LOJA/   → /magazineleo12/lojista/LOJA/
#   /magazineXXX/                → /magazineleo12/
# Path NUNCA zerado. Apenas IDs substituídos.
# ══════════════════════════════════════════════════════════════════════════════

def _validar_magalu(url: str) -> bool:
    p    = urlparse(url)
    host = p.netloc.lower()
    path = p.path.rstrip("/")
    if "sacola" in host and path in ("", "/"):
        log_mgl.warning(f"⚠️ Sacola sem produto: {url[:60]}")
        return False
    if not path or path == "/" or len(path.split("/")) < 2:
        log_mgl.warning(f"⚠️ Homepage sem produto: {url[:60]}")
        return False
    return True


def _afiliar_magalu(url: str) -> str:
    """
    1. Substitui qualquer /magazineXXX/ pelo slug correto.
    2. Injeta IDs de afiliado. Preserva 100% do restante do path.
    """
    p    = urlparse(url)
    path = p.path
    host = p.netloc.lower()

    # Troca slug em magazinevoce.com.br e magazineluiza.com.br
    # Captura /magazineQUALQUERCOISA/ e substitui pelo slug configurado
    path = re.sub(
        r'^(/magazine)[^/]+(?=/|$)',
        rf'\1{_MGL_SLUG}',
        path
    )
    log_mgl.debug(f"  Path após slug: {path}")

    # Limpa params antigos de afiliado
    params = {k: v[0] for k, v in
              parse_qs(p.query, keep_blank_values=True).items()}
    for k in ["tag","partnerid","promoterid","afforcedeeplink",
               "deeplinkvalue","isretargeting","partner_id","promoter_id",
               "utm_source","utm_medium","utm_campaign","pid","c",
               "af_force_deeplink","deep_link_value"]:
        params.pop(k, None)

    # Injeta IDs corretos
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

    return urlunparse(p._replace(
        path=path, query=urlencode(params), fragment=""))


async def _cuttly(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    api = (f"https://cutt.ly/api/api.php"
           f"?key={_CUTTLY_KEY}&short={quote(url, safe='')}")
    for t in range(1, 4):
        try:
            async with _SEM_HTTP:
                async with sessao.get(
                    api, timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    if r.status != 200:
                        await asyncio.sleep(2 ** t)
                        continue
                    data   = await r.json(content_type=None)
                    status = data.get("url", {}).get("status")
                    if status in (7, 2):
                        short = data["url"].get("shortLink")
                        log_mgl.debug(f"  ✂️ Cuttly t={t}: {short}")
                        return short
                    log_mgl.warning(f"  ⚠️ Cuttly status={status} t={t}/3")
                    await asyncio.sleep(2 ** t)
        except Exception as e:
            log_mgl.error(f"  ❌ Cuttly t={t}: {e}")
            await asyncio.sleep(2 ** t)
    return None


async def motor_magalu(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """Motor exclusivo Magalu. Debug isolado com log_mgl."""
    log_mgl.debug(f"▶ IN: {url[:80]}")

    nl = _netloc(url)
    if "maga.lu" in nl or nl in _ENCURTADORES or classificar(url) == "expandir":
        async with _SEM_HTTP:
            url = await desencurtar(url, sessao)
        log_mgl.debug(f"  EXP: {url[:80]}")

    if classificar(url) != "magalu":
        log_mgl.warning(f"  ⚠️ Não é Magalu: {url[:70]}")
        return None

    if not _validar_magalu(url):
        return None

    afiliado = _afiliar_magalu(url)
    log_mgl.debug(f"  AFL: {afiliado[:80]}")

    final = await _cuttly(afiliado, sessao)
    if not final:
        log_mgl.error(f"  ❌ Cuttly falhou — descartado")
        return None

    log_mgl.info(f"  ✅ OUT: {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 10 ▸ EXTRAÇÃO DE LINKS
# ══════════════════════════════════════════════════════════════════════════════

_RE_URL = re.compile(
    r'https?://[^\s\)\]>,"\'<\u200b\u200c\u200d\u2060]+')

def extrair_links(texto: str) -> Tuple[List[str], List[str]]:
    brutos    = [u.strip().rstrip('.,;)>]}') for u in _RE_URL.findall(texto)]
    converter: List[str] = []
    preservar: List[str] = []
    vistos: set           = set()

    for url in brutos:
        if url in vistos:
            continue
        vistos.add(url)
        plat = classificar(url)
        if plat == "preservar":
            preservar.append(url)
        elif plat in ("amazon", "shopee", "magalu", "expandir"):
            converter.append(url)

    log_lnk.debug(f"🔗 {len(converter)} converter | {len(preservar)} preservar")
    return converter, preservar


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 11 ▸ PIPELINE DE CONVERSÃO — COM CACHE SQLite
# Antes de chamar a API, verifica o cache.
# Só chama API se o link não estiver no cache.
# ══════════════════════════════════════════════════════════════════════════════

async def _converter_um(url: str,
                         sessao: aiohttp.ClientSession
                         ) -> Tuple[Optional[str], Optional[str]]:
    plat = classificar(url)

    # Verifica cache SQLite antes de chamar qualquer API
    cached = db_get_link(url)
    if cached:
        log_lnk.debug(f"💾 Cache: [{plat}] {cached[:50]}")
        return cached, plat if plat in ("amazon","shopee","magalu") else "amazon"

    async def _rota_com_cache(u: str, motor, nome: str):
        r = await motor(u, sessao)
        if r:
            db_set_link(url, r, nome)
            return r, nome
        return None, None

    if plat == "amazon":
        return await _rota_com_cache(url, motor_amazon, "amazon")

    if plat == "shopee":
        return await _rota_com_cache(url, motor_shopee, "shopee")

    if plat == "magalu":
        return await _rota_com_cache(url, motor_magalu, "magalu")

    if plat == "expandir":
        try:
            async with _SEM_HTTP:
                exp = await desencurtar(url, sessao)
        except Exception:
            return None, None
        p2 = classificar(exp)
        if p2 in ("amazon", "shopee", "magalu"):
            return await _converter_um(exp, sessao)
        return None, None

    return None, None


async def converter_links(links: List[str]) -> Tuple[Dict[str, str], str]:
    if not links:
        return {}, "amazon"

    conn = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False)
    async with aiohttp.ClientSession(
        connector=conn,
        timeout=aiohttp.ClientTimeout(total=40, connect=8),
        headers={"User-Agent": random.choice(USER_AGENTS)},
    ) as sessao:
        resultados = await asyncio.gather(
            *[_converter_um(l, sessao) for l in links[:50]],
            return_exceptions=True,
        )

    mapa: Dict[str, str] = {}
    plats: List[str]      = []
    for i, res in enumerate(resultados):
        if isinstance(res, Exception):
            log_lnk.error(f"❌ [{i}]: {res}")
            continue
        novo, plat = res
        if novo and plat:
            mapa[links[i]] = novo
            plats.append(plat)

    plat_p = max(set(plats), key=plats.count) if plats else "amazon"
    log_lnk.info(f"✅ {len(mapa)}/{len(links)} | plat={plat_p}")
    return mapa, plat_p

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 12 ▸ LIMPEZA DE RUÍDO TEXTUAL
# Remove: CTAs de bots, links externos, lixo estrutural, blocos de redes sociais.
# ══════════════════════════════════════════════════════════════════════════════

_RE_INVISIVEIS = re.compile(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]')
_RE_GRUPO_EXT  = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)[^\s]*',
    re.I)

# Linhas de lixo estrutural
_RE_LIXO_STRUCT = re.compile(
    r'^\s*(?:'
    r'-?\s*An[uú]ncio|Publicidade|:::+|---+|===+'
    r'|[-–—]\s*(?:ML|MG|AMZ)|(?:ML|MG|AMZ)\s*:'
    r')\s*$',
    re.I)

# CTAs de outros bots
_RE_CTA = re.compile(
    r'^\s*(?:'
    r'link\s+(?:do\s+)?produto|link\s+da\s+oferta|'
    r'resgate\s+aqui|clique\s+aqui|acesse\s+aqui|'
    r'compre\s+aqui|grupo\s+vip|entrar\s+no\s+grupo|'
    r'acessar\s+grupo|saiba\s+mais\s*:|confira\s+no\s+app'
    r')\s*:?\s*$',
    re.I)

# Bloco de redes sociais — "Redes XXX", "-Grupo:", "-Chat:", "-Twitter:", etc.
_RE_REDES_BLOCO = re.compile(
    r'^\s*(?:'
    r'redes\s+\w+|'                          # "Redes Promotom"
    r'[-–]\s*grupo\s*(?:cupons?|promoções?|vip)?\s*:?\s*$|'
    r'[-–]\s*(?:chat|twitter|whatsapp|instagram|tiktok|youtube)\s*:?\s*$|'
    r'[-–]\s*link\s+(?:do\s+)?grupo\s*:?\s*$|'
    r'acesse\s+nossas\s+redes|'
    r'nossas\s+redes\s+sociais'
    r')',
    re.I)

# Linhas que são só rótulo vazio (ex: "-Grupo Promoções:" sem URL)
_RE_ROTULO_VAZIO = re.compile(
    r'^\s*[-–•]\s*\w[\w\s]{0,30}:\s*$'
)


def limpar_ruido_textual(texto: str) -> str:
    texto  = _RE_INVISIVEIS.sub(" ", texto)
    texto  = texto.replace("\r\n", "\n").replace("\r", "\n")
    linhas = texto.split("\n")

    saida: List[str] = []
    vazio = False
    _em_bloco_redes = False

    for linha in linhas:
        l = linha.strip()

        # Linha vazia — controla espaçamento
        if not l:
            if not vazio:
                saida.append("")
            vazio = True
            _em_bloco_redes = False
            continue
        vazio = False

        # Detecta início de bloco de redes sociais
        if _RE_REDES_BLOCO.match(l):
            _em_bloco_redes = True
            continue

        # Dentro do bloco de redes → descarta linhas de rótulo vazio
        if _em_bloco_redes:
            if _RE_ROTULO_VAZIO.match(l) or not l:
                continue
            # Se encontrou conteúdo real, sai do modo bloco
            if not re.match(r'https?://', l):
                _em_bloco_redes = False
            else:
                continue  # URL dentro do bloco de redes → descarta

        # CTAs de outros bots
        if _RE_CTA.match(l):
            continue

        # Lixo estrutural
        if _RE_LIXO_STRUCT.match(l):
            continue

        # Links de grupos externos
        if _RE_GRUPO_EXT.search(l):
            l = _RE_GRUPO_EXT.sub("", l).strip()
            if not l:
                continue

        saida.append(l)

    return "\n".join(saida).strip()


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 13 ▸ EMOJIS FIXOS + RADARES SEMÂNTICOS
# Rotação circular determinística.
# Multi-produto (≥2 itens com preço): usa 🔹 nos produtos, não emojis de dinheiro.
# Se a linha já tiver emoji → NÃO adiciona nenhum.
# ══════════════════════════════════════════════════════════════════════════════

_EMJ: Dict[str, List[str]] = {
    "titulo_oferta": ["🔥"],
    "titulo_cupom":  ["🚨", "🔔", "📢"],
    "titulo_evento": ["⚠️"],
    "preco":         ["💵", "💰", "🤑"],
    "cupom_cod":     ["🎟", "🏷"],
    "resgate":       ["✅", "🔗"],
    "carrinho":      ["🛒"],
    "frete":         ["🚚", "📦"],
    "multi_item":    ["🔹"],   # fixo para itens em lista multi-produto
}
_EMJ_IDX: Dict[str, int] = {k: 0 for k in _EMJ}

def _prox_emoji(cat: str) -> str:
    lista         = _EMJ[cat]
    idx           = _EMJ_IDX[cat]
    emoji         = lista[idx % len(lista)]
    _EMJ_IDX[cat] = (idx + 1) % len(lista)
    return emoji

_KW_CUPOM    = re.compile(
    r'\b(?:cupom|cupon|c[oó]digo|coupon|off|resgate|cod)\b', re.I)
_KW_PRECO    = re.compile(r'R\$\s?[\d.,]+', re.I)
_KW_FRETE    = re.compile(
    r'\b(?:frete\s+gr[aá]t|entrega\s+gr[aá]t|sem\s+frete|frete\s+0)\b', re.I)
_KW_EVENTO   = re.compile(
    r'\b(?:quiz|roleta|miss[aã]o|arena|girar|gire|roda|jogar|jogue|desafio)\b',
    re.I)
_KW_STATUS   = re.compile(
    r'\b(?:voltando|voltou|normalizou|renovado|estoque\s+renovado|regularizou)\b',
    re.I)
_KW_RESGATE  = re.compile(
    r'\b(?:resgate|clique|acesse|ative|use\s+o\s+cupom)\b', re.I)
_KW_CARRINHO = re.compile(r'\b(?:carrinho|cart)\b', re.I)
_KW_COD      = re.compile(r'\b([A-Z][A-Z0-9_-]{3,19})\b')

_RE_EMOJI_CHECK = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF"
    r"\U0001F900-\U0001F9FF\u2B50\u2B55\u231A\u231B"
    r"\U0001F100-\U0001F1FF]")

def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI_CHECK.search(s))

def _contar_produtos(texto: str) -> int:
    """Conta linhas com preço — indica quantidade de produtos."""
    return sum(1 for l in texto.splitlines() if _KW_PRECO.search(l))

def _emoji_de_linha(linha: str, eh_titulo: bool,
                    is_multi: bool = False) -> Optional[str]:
    """
    Retorna emoji para a linha.
    Se is_multi=True e a linha tem preço → usa 🔹 (item de lista).
    Se a linha já tem emoji → retorna None (não duplica).
    """
    # Já tem emoji → não adiciona
    if _tem_emoji(linha):
        return None

    if eh_titulo:
        if _KW_EVENTO.search(linha): return _prox_emoji("titulo_evento")
        if _KW_CUPOM.search(linha):  return _prox_emoji("titulo_cupom")
        return _prox_emoji("titulo_oferta")

    # Multi-produto: linhas com preço recebem 🔹
    if is_multi and _KW_PRECO.search(linha):
        return "🔹"

    if _KW_FRETE.search(linha):    return _prox_emoji("frete")
    if _KW_CUPOM.search(linha):    return _prox_emoji("cupom_cod")
    if _KW_PRECO.search(linha):    return _prox_emoji("preco")
    if _KW_RESGATE.search(linha):  return _prox_emoji("resgate")
    if _KW_CARRINHO.search(linha): return _prox_emoji("carrinho")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ RENDERIZADOR
# - Não duplica emojis existentes
# - Multi-produto usa 🔹 nos itens
# - Remove nome do canal/grupo da primeira linha
# - Preserva espaçamento original
# - Clique-e-copie nos cupons
# ══════════════════════════════════════════════════════════════════════════════

_RE_LIXO_PREF  = re.compile(
    r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:|[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?)\s*',
    re.I)
_RE_ANUNCIO    = re.compile(
    r'^\s*[-#]?\s*(?:an[uú]ncio|publicidade|patrocinado)\s*$', re.I)
_RE_URL_RENDER = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c]+')

# Remove "Nome do Canal 🔥" ou "Nome do Bot:" da primeira linha de texto
# Detecta padrões: "Redes PromoTom", "PromoTom Ofertas", "Canal XYZ"
_RE_NOME_CANAL = re.compile(
    r'^\s*(?:redes\s+)?\w[\w\s]{2,30}'
    r'(?:\s+(?:ofertas?|promos?|cupons?|indica|bot|canal|grupo))?\s*[:\-]?\s*$',
    re.I)

_FALSO_CUPOM = frozenset({
    "FRETE","GRÁTIS","GRATIS","AMAZON","SHOPEE","MAGALU","LINK",
    "CLIQUE","ACESSE","CONFIRA","HOJE","AGORA","PROMO","OFF",
    "BLACK","SUPER","MEGA","ULTRA","VIP","NOVO","NOVA","NUM","PRECO","PCT",
    "PS5","PS4","XBOX","USB","ATX","RGB","LED","HD","SSD","RAM",
    "APP","BOT","API","URL","HTTP","HTTPS",
})

def _crases(linha: str) -> str:
    """Coloca crases em códigos de cupom para clique-e-copie no Telegram."""
    if "http" in linha or "`" in linha:
        return linha
    if not (_KW_CUPOM.search(linha) or _KW_COD.search(linha)):
        return linha

    def _sub(m: re.Match) -> str:
        c = m.group(0)
        return c if (c in _FALSO_CUPOM or len(c) < 4) else f"`{c}`"

    return re.sub(r'\b([A-Z][A-Z0-9_-]{4,20})\b', _sub, linha)


def renderizar(texto: str, mapa_links: Dict[str, str],
               links_preservar: List[str], plat: str) -> str:
    mapa     = {**mapa_links, **{u: u for u in links_preservar}}
    is_multi = _contar_produtos(texto) >= 2

    linhas   = texto.split("\n")
    saida: List[str] = []
    primeiro = True
    idx_linha = 0

    for linha in linhas:
        l = linha.strip()
        idx_linha += 1

        if not l:
            saida.append("")
            continue

        # Remove linha de anúncio explícito (preserva como texto simples)
        if _RE_ANUNCIO.match(l):
            saida.append(l)
            continue

        l = _RE_LIXO_PREF.sub("", l).strip()
        if not l:
            continue

        # ── Substitui URLs ────────────────────────────────────────────────
        urls_na_linha = _RE_URL_RENDER.findall(l)
        sem_urls      = _RE_URL_RENDER.sub("", l).strip()

        if urls_na_linha and not sem_urls:
            # Linha só de link
            for u in urls_na_linha:
                uc = u.rstrip('.,;)>')
                convertido = mapa.get(uc)
                if convertido:
                    saida.append(convertido)
            continue

        l = _RE_URL_RENDER.sub(
            lambda m: mapa.get(m.group(0).rstrip('.,;)>'), ""),
            l).strip()
        if not l:
            continue

        # ── Clique-e-copie ────────────────────────────────────────────────
        l = _crases(l)

        # ── Emoji ─────────────────────────────────────────────────────────
        # Regra 1: se já tem emoji, não adiciona nenhum
        if not _tem_emoji(l):
            emoji = _emoji_de_linha(l, eh_titulo=primeiro, is_multi=is_multi)
            if emoji:
                l = f"{emoji} {l}"

        if primeiro:
            primeiro = False

        saida.append(l)

    return "\n".join(saida).strip()



# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 15 ▸ DEDUPLICAÇÃO SEMÂNTICA — DNA FINGERPRINT
#
# DECISÃO BASEADA APENAS EM: cupom + texto + contexto semântico
# NUNCA em: link, emoji, preço, formatação, encurtador
#
# CAMADAS (custo crescente):
#   F0 — DNA hash exato (cupom+texto)           → O(1)
#   F1 — Cupom idêntico + mesma plataforma      → O(n)
#   F2 — Alma textual muito similar             → O(n²) só como tiebreaker
#   F3 — Evento recorrente + cupom + 24h        → O(n)
#
# JANELA ANTI-SPAM: 10 min para mesmo grupo postar a mesma oferta
# ══════════════════════════════════════════════════════════════════════════════

_TTL_CACHE    = 120 * 60
_TTL_EVENTO   = 24  * 60 * 60
_JANELA_CURTA = 10  * 60
_SIM_TEXTO    = 0.88
_SIM_CAMPANHA = 0.80
_SIM_EVENTO   = 0.60

_RUIDO_NORM = frozenset({
    "promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora",
    "imperdivel","imperdível","exclusivo","limitado","corra","ative",
    "use","saiu","vazou","resgate","acesse","confira","link","clique",
    "app","relampago","relâmpago","click","veja","novo","nova",
})
_RE_EMJ_STRIP = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF]+",
    flags=re.UNICODE)
_FALSO_CUPOM_DEDUP = _FALSO_CUPOM  # reutiliza o mesmo conjunto

def _rm_ac(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", t)
                   if unicodedata.category(c) != "Mn")

def _extrair_todos_cupons(texto: str) -> frozenset:
    """Extrai TODOS os cupons. Base principal da decisão de dedup."""
    return frozenset(re.findall(r'\b([A-Z][A-Z0-9_-]{3,19})\b', texto)
                     ) - _FALSO_CUPOM_DEDUP

def _extrair_cupom(texto: str) -> str:
    cupons = _extrair_todos_cupons(texto)
    return next(iter(sorted(cupons)), "")

def _extrair_valor(texto: str) -> str:
    m = _KW_PRECO.search(texto)
    if not m:
        return ""
    return re.sub(r'[R$\s.,]', '', m.group(0))

def _extrair_sku(mapa: Dict[str, str]) -> str:
    if not mapa:
        return ""
    for url in mapa.values():
        m = re.search(r'/dp/([A-Z0-9]{10})', url)
        if m: return f"amz_{m.group(1)}"
        m = re.search(r'/i\.(\d+\.\d+)', url)
        if m: return f"shp_{m.group(1)}"
        m = re.search(r'/p/([a-z0-9]{6,})', url, re.I)
        if m: return f"mgl_{m.group(1)}"
    return hashlib.sha256(list(mapa.values())[0].encode()).hexdigest()[:16]

def _normalizar_alma(texto: str) -> str:
    """
    Extrai a essência semântica do texto.
    Remove: links, emojis, preços, números, pontuação, ruído.
    Resultado: palavras-chave ordenadas — base do tiebreaker F2.
    """
    t = _rm_ac(texto.lower())
    t = re.sub(r'https?://\S+', ' ', t)
    t = _RE_EMJ_STRIP.sub(' ', t)
    t = re.sub(r'r\$\s*[\d.,]+', ' PRECO ', t)
    t = re.sub(r'\b\d+\s*%', ' PCT ', t)
    t = re.sub(r'\b\d+\b', ' NUM ', t)
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return ' '.join(sorted(
        w for w in t.split() if w not in _RUIDO_NORM and len(w) > 1))

def _detectar_campanha(texto: str) -> str:
    tl     = texto.lower()
    tokens = []
    m = _KW_EVENTO.search(texto)
    if m: tokens.append(f"evt_{m.group(0).lower()}")
    if "amazon app" in tl: tokens.append("amazon_app")
    if "mastercard"  in tl: tokens.append("mastercard")
    if "prime"       in tl: tokens.append("prime")
    if "magalu app"  in tl: tokens.append("magalu_app")
    return "|".join(sorted(tokens)) if tokens else "geral"

def _gerar_fp(plat: str, cupons: frozenset, alma: str) -> str:
    """
    DNA fingerprint baseado em: plataforma + cupons + alma do texto.
    Imune a links, emojis, preço e formatação.
    """
    raw = f"{plat}|{'|'.join(sorted(cupons))}|{alma}"
    return hashlib.sha256(raw.encode()).hexdigest()

def _purgar(cache: dict, agora: float) -> dict:
    ttl = max(_TTL_CACHE, _TTL_EVENTO)
    return {k: v for k, v in cache.items() if agora - v.get("ts", 0) < ttl}

def _registrar_cache(plat: str, cupom: str, texto: str):
    cache    = ler_cache()
    agora    = time.time()
    cupons   = _extrair_todos_cupons(texto)
    alma     = _normalizar_alma(texto)
    campanha = _detectar_campanha(texto)
    fp       = _gerar_fp(plat, cupons, alma)
    cache[fp] = {
        "plat": plat, "cupom": cupom.upper(),
        "cupons": list(cupons), "camp": campanha,
        "alma": alma, "ts": agora,
    }
    salvar_cache(cache)

def deve_enviar(plat: str, cupom: str, texto: str) -> bool:
    """
    Decisão central baseada APENAS em cupom + texto + contexto.
    NUNCA usa link, emoji, preço ou formatação como critério.
    """
    # Mudança de status sempre passa (repost legítimo)
    if _KW_STATUS.search(texto):
        log_dedup.info("✅ Status change — passa direto")
        _registrar_cache(plat, cupom, texto)
        return True

    cache  = ler_cache()
    agora  = time.time()
    cache  = _purgar(cache, agora)

    cupons = _extrair_todos_cupons(texto)
    alma   = _normalizar_alma(texto)
    eh_evt = bool(_KW_EVENTO.search(texto))
    janela = _TTL_EVENTO if eh_evt else _JANELA_CURTA

    # F0: DNA fingerprint exato — O(1)
    fp = _gerar_fp(plat, cupons, alma)
    if fp in cache:
        log_dedup.info(f"🔁 [F0-DNA] | plat={plat} cupons={sorted(cupons)}")
        return False

    campanha = None
    for entrada in cache.values():
        if agora - entrada.get("ts", 0) >= janela: continue
        if entrada.get("plat") != plat:             continue

        cupons_c    = frozenset(entrada.get("cupons", []))
        cupom_igual = bool(cupons & cupons_c) if cupons else False

        # F1: cupom idêntico — bloqueia direto
        if cupom_igual:
            log_dedup.info(f"🔁 [F1-CUPOM] | match={cupons & cupons_c}")
            return False

        # F2: alma textual similar (tiebreaker — só sem cupom)
        alma_c = entrada.get("alma", "")
        sim    = SequenceMatcher(None, alma, alma_c).ratio() if alma_c else 0.0

        if sim >= _SIM_TEXTO:
            log_dedup.info(f"🔁 [F2-ALMA] | sim={sim:.2f}")
            return False

        if campanha is None:
            campanha = _detectar_campanha(texto)
        camp_c = entrada.get("camp", "")
        if campanha == camp_c and campanha != "geral" and sim >= _SIM_CAMPANHA:
            log_dedup.info(f"🔁 [F2-CAMP] | camp={campanha} sim={sim:.2f}")
            return False

        # F3: evento + cupom → 24h
        if eh_evt and cupom_igual and sim >= _SIM_EVENTO:
            log_dedup.info(f"🔁 [F3-EVT]")
            return False

    _registrar_cache(plat, cupom, texto)
    log_dedup.debug(f"✅ Nova | plat={plat} cupons={sorted(cupons)}")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 16 ▸ IMAGEM — BUSCA 4K + preparar_imagem
# Busca a maior imagem disponível (og:image, JSON-LD, img tag).
# preparar_imagem: baixa URL ou usa media do Telegram.
# ══════════════════════════════════════════════════════════════════════════════

_IMG_SHP = "shopee_promo.jpg"   # imagem fallback Shopee

async def buscar_imagem(url: str) -> Optional[str]:
    """
    Busca a melhor imagem do produto.
    Prioridade: og:image > JSON-LD > maior <img> na página.
    Prefere imagens grandes (4K quando disponível).
    """
    hdrs = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "pt-BR,pt;q=0.9",
    }
    for t in range(1, 4):
        try:
            async with aiohttp.ClientSession(headers=hdrs) as s:
                async with s.get(
                    url, allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=12)
                ) as r:
                    ct = r.headers.get("content-type", "")
                    if "image" in ct:
                        return str(r.url)

                    html = await r.text(errors="ignore")
                    soup = BeautifulSoup(html, "html.parser")

                    # og:image (geralmente alta resolução)
                    for attr in [
                        {"property": "og:image"},
                        {"property": "og:image:secure_url"},
                        {"name": "twitter:image"},
                    ]:
                        tag = soup.find("meta", attrs=attr)
                        if tag and tag.get("content", "").startswith("http"):
                            img_url = tag["content"]
                            # Remove parâmetros de resize para pegar original
                            img_url = re.sub(
                                r'[?&](?:width|height|w|h|size|resize)=\d+',
                                '', img_url)
                            log_img.info(f"✅ og:image t={t}: {img_url[:70]}")
                            return img_url

                    # JSON-LD
                    for scr in soup.find_all("script",
                                              type="application/ld+json"):
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
                                        u = c.get("url", "")
                                        if u.startswith("http"): return u
                        except Exception:
                            pass

                    # Maior <img> na página
                    melhor_src   = None
                    melhor_area  = 0
                    for img_tag in soup.find_all("img", src=True):
                        src = img_tag.get("src", "")
                        if not src.startswith("http"):
                            continue
                        try:
                            w = int(img_tag.get("width",  0))
                            h = int(img_tag.get("height", 0))
                            area = w * h
                            if area > melhor_area:
                                melhor_area = area
                                melhor_src  = src
                        except (ValueError, TypeError):
                            if any(x in src.lower() for x in
                                   ["product","produto","item","image","foto",
                                    "_zoom","large","big","xl","hd"]):
                                if not melhor_src:
                                    melhor_src = src
                    if melhor_src:
                        log_img.info(f"✅ <img> t={t}: {melhor_src[:70]}")
                        return melhor_src

        except asyncio.TimeoutError:
            log_img.warning(f"⏱ Timeout imagem t={t}/3")
        except Exception as e:
            log_img.warning(f"⚠️ Imagem t={t}/3: {e}")
        if t < 3:
            await asyncio.sleep(1.0)

    log_img.warning(f"❌ Sem imagem: {url[:60]}")
    return None


async def preparar_imagem(fonte, eh_midia_telegram: bool) -> tuple:
    """
    Retorna (objeto_para_send_file, None).
    Se for media do Telegram: retorna direto (Telethon sabe baixar).
    Se for URL: baixa os bytes para memória (evita salvar em disco).
    """
    if eh_midia_telegram:
        # Telethon aceita o objeto media diretamente no send_file
        return fonte, None

    if isinstance(fonte, str) and fonte.startswith("http"):
        try:
            hdrs = {"User-Agent": random.choice(USER_AGENTS)}
            async with aiohttp.ClientSession(headers=hdrs) as s:
                async with s.get(
                    fonte, timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    if r.status == 200:
                        data = await r.read()
                        buf  = io.BytesIO(data)
                        buf.name = "produto.jpg"
                        return buf, None
        except Exception as e:
            log_img.warning(f"⚠️ Download imagem: {e}")
        return None, None

    # Caminho de arquivo local
    return fonte, None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 17 ▸ RATE-LIMIT INTERNO
# ══════════════════════════════════════════════════════════════════════════════

async def _rate_limit():
    global _ULTIMO_ENV_TS
    async with _RATE_LOCK:
        agora  = time.monotonic()
        espera = _INTERVALO_MIN - (agora - _ULTIMO_ENV_TS)
        if espera > 0:
            await asyncio.sleep(espera)
        _ULTIMO_ENV_TS = time.monotonic()


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 18 ▸ ANTI-LOOP DE EDIÇÃO
# ══════════════════════════════════════════════════════════════════════════════

_IDS_PROC: set = set()
_IDS_LOCK = asyncio.Lock()

async def _marcar(msg_id: int):
    async with _IDS_LOCK:
        _IDS_PROC.add(msg_id)
        if len(_IDS_PROC) > 3000:
            _IDS_PROC.pop()

async def _foi_processado(msg_id: int) -> bool:
    async with _IDS_LOCK:
        return msg_id in _IDS_PROC

# =========================
# MÓDULO — CAPTURA DE IMAGEM + PREPARO
# =========================

from io import BytesIO
import re
import requests
from PIL import Image


def extrair_primeiro_link(texto):
    """
    Extrai o primeiro link encontrado no texto.
    """
    if not texto:
        return None

    match = re.search(r'https?://\S+', texto)
    return match.group(0) if match else None


def baixar_imagem_do_link(url, timeout=10):
    """
    Tenta baixar imagem diretamente do link.
    """
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").lower()

        if "image" in content_type:
            return resp.content

        return None

    except Exception as e:
        print(f"[IMG LINK] Erro ao baixar imagem do link: {e}")
        return None


def preparar_imagem(img_bytes, max_size=(1080, 1080), qualidade=90):
    """
    Prepara imagem para envio:
    - converte para RGB
    - redimensiona
    - comprime
    """
    try:
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
        img.thumbnail(max_size)

        output = BytesIO()
        img.save(output, format="JPEG", quality=qualidade, optimize=True)
        output.seek(0)

        return output.getvalue()

    except Exception as e:
        print(f"[PREPARAR IMG] Erro: {e}")
        return img_bytes


async def obter_imagem(message, client):
    """
    Tenta obter imagem da mensagem.
    Ordem:
    1. Mídia da mensagem
    2. Link dentro do texto
    """

    # 1) tenta mídia do Telegram
    try:
        if message.photo or message.document:
            img_bytes = await client.download_media(message, file=bytes)

            if img_bytes:
                print("[IMG] Capturada da mensagem")
                return preparar_imagem(img_bytes)

    except Exception as e:
        print(f"[IMG MSG] Erro ao baixar mídia: {e}")

    # 2) tenta pelo link
    try:
        texto = message.message or ""
        link = extrair_primeiro_link(texto)

        if link:
            img_bytes = baixar_imagem_do_link(link)

            if img_bytes:
                print("[IMG] Capturada do link")
                return preparar_imagem(img_bytes)

    except Exception as e:
        print(f"[IMG FALLBACK] Erro no fallback: {e}")

    print("[IMG] Nenhuma imagem encontrada")
    return None

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 19 ▸ ENVIO — PRIORIDADE ABSOLUTA À IMAGEM
# ══════════════════════════════════════════════════════════════════════════════

def _tem_midia(media) -> bool:
    return media is not None and not isinstance(media, MessageMediaWebPage)

def _eh_cupom_texto(texto: str) -> bool:
    return bool(_KW_CUPOM.search(texto))

async def _enviar(msg: str, img_obj, message=None) -> object:
    # prioridade absoluta: tentar capturar imagem automaticamente
    if not img_obj and message is not None:
        try:
            img_obj = await obter_imagem(message, client)
        except Exception as e:
            log_tg.warning(f"⚠️ erro ao obter imagem: {e}")

    if img_obj:
        if len(msg) <= 1024:
            try:
                return await client.send_file(
                    GRUPO_DESTINO,
                    BytesIO(img_obj) if isinstance(img_obj, bytes) else img_obj,
                    caption=msg,
                    parse_mode="md"
                )
            except Exception as e:
                log_tg.warning(f"⚠️ send_file+caption: {e}")
                try:
                    await client.send_file(
                        GRUPO_DESTINO,
                        BytesIO(img_obj) if isinstance(img_obj, bytes) else img_obj
                    )
                    return await client.send_message(
                        GRUPO_DESTINO, msg, parse_mode="md", link_preview=True
                    )
                except Exception as e2:
                    log_tg.warning(f"⚠️ send_file sem caption: {e2}")
        else:
            try:
                await client.send_file(
                    GRUPO_DESTINO,
                    BytesIO(img_obj) if isinstance(img_obj, bytes) else img_obj
                )
                return await client.send_message(
                    GRUPO_DESTINO, msg, parse_mode="md", link_preview=False
                )
            except Exception as e:
                log_tg.warning(f"⚠️ texto longo: {e}")

    return await client.send_message(
        GRUPO_DESTINO, msg, parse_mode="md", link_preview=True
)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 20 ▸ BANCO CENTRAL — SQLite
# Tabela extra: links_cache → evita chamar API para o mesmo link original.
# ══════════════════════════════════════════════════════════════════════════════

_DB_PATH = "foguetao.db"
_db_conn: Optional[sqlite3.Connection] = None
_db_lock = Lock()

def _init_db():
    global _db_conn
    _db_conn = sqlite3.connect(
        _DB_PATH, check_same_thread=False, timeout=10, isolation_level=None)
    _db_conn.execute("PRAGMA journal_mode=WAL")
    _db_conn.execute("PRAGMA synchronous=NORMAL")
    _db_conn.execute("PRAGMA cache_size=-8000")
    _db_conn.executescript("""
        CREATE TABLE IF NOT EXISTS ofertas (
            fp     TEXT PRIMARY KEY,
            plat   TEXT NOT NULL,
            cupons TEXT,
            camp   TEXT,
            alma   TEXT,
            ts     REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS saturacao (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            plat TEXT NOT NULL,
            sku  TEXT,
            ts   REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS scheduler (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            plat  TEXT NOT NULL,
            hora  INTEGER NOT NULL,
            score REAL DEFAULT 1.0,
            ts    REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS links_cache (
            url_orig  TEXT PRIMARY KEY,
            url_conv  TEXT NOT NULL,
            plat      TEXT NOT NULL,
            ts        REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_of_plat ON ofertas(plat, ts);
        CREATE INDEX IF NOT EXISTS idx_sat     ON saturacao(plat, ts);
        CREATE INDEX IF NOT EXISTS idx_sch     ON scheduler(plat, hora);
        CREATE INDEX IF NOT EXISTS idx_lc_ts   ON links_cache(ts);
    """)
    log_sys.info(f"🗄 DB ON | {_DB_PATH}")

@contextmanager
def _db():
    with _db_lock:
        try:
            yield _db_conn
        except sqlite3.Error as e:
            log_sys.error(f"❌ DB: {e}")
            raise

# ── Links cache ───────────────────────────────────────────────────────────────

_LINK_CACHE_TTL = 6 * 3600  # 6 horas

def db_get_link(url_orig: str) -> Optional[str]:
    """Retorna link convertido do cache se existir e não tiver expirado."""
    limite = time.time() - _LINK_CACHE_TTL
    with _db() as db:
        row = db.execute(
            "SELECT url_conv FROM links_cache WHERE url_orig=? AND ts>=?",
            (url_orig, limite)
        ).fetchone()
    if row:
        log_lnk.debug(f"💾 Cache hit: {url_orig[:50]}")
        return row[0]
    return None

def db_set_link(url_orig: str, url_conv: str, plat: str):
    """Salva link convertido no cache."""
    with _db() as db:
        db.execute(
            "INSERT OR REPLACE INTO links_cache (url_orig,url_conv,plat,ts) VALUES(?,?,?,?)",
            (url_orig, url_conv, plat, time.time()))

def db_registrar_sat(plat: str, sku: str = ""):
    with _db() as db:
        db.execute("INSERT INTO saturacao (plat,sku,ts) VALUES(?,?,?)",
                   (plat, sku, time.time()))

def db_count_sat(plat: str, janela: float = 1800) -> int:
    limite = time.time() - janela
    with _db() as db:
        row = db.execute(
            "SELECT COUNT(*) FROM saturacao WHERE plat=? AND ts>=?",
            (plat, limite)).fetchone()
    return row[0] if row else 0

def db_registrar_sch(plat: str, hora: int, score: float = 1.0):
    with _db() as db:
        db.execute(
            "INSERT INTO scheduler (plat,hora,score,ts) VALUES(?,?,?,?)",
            (plat, hora, score, time.time()))

def db_score_hora(plat: str, hora: int, dias: int = 7) -> float:
    limite = time.time() - dias * 86400
    with _db() as db:
        row = db.execute(
            "SELECT AVG(score) FROM scheduler WHERE plat=? AND hora=? AND ts>=?",
            (plat, hora, limite)).fetchone()
    return float(row[0] or 1.0)

def db_limpar(dias: int = 7):
    limite = time.time() - dias * 86400
    with _db() as db:
        db.execute("DELETE FROM ofertas       WHERE ts<?", (limite,))
        db.execute("DELETE FROM saturacao     WHERE ts<?", (limite,))
        db.execute("DELETE FROM scheduler     WHERE ts<?", (limite,))
        db.execute("DELETE FROM links_cache   WHERE ts<?", (limite,))
    log_sys.info(f"🗑 DB limpeza >{dias}d")


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 21 ▸ PARSER ULTRA ROBUSTO
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ParsedLink:
    url_original: str
    url_limpa:    str
    plat:         str
    tipo:         str
    sku:          str
    valido:       bool
    motivo:       str

_TRACKING = frozenset({
    "tag","ref","ref_","smid","sprefix","sr","spla","dchild","linkcode",
    "linkid","camp","creative","pf_rd_p","pf_rd_r","pd_rd_wg","pd_rd_w",
    "content-id","pd_rd_r","pd_rd_i","ie","qid","_encoding","dib",
    "dib_tag","m","th","psc","ingress","visitid","s","ascsubtag","btn_ref",
    "af_siteid","pid","af_click_lookback","is_retargeting","deep_link_value",
    "af_dp","utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "partner_id","promoter_id","af_force_deeplink","c","isretargeting",
    "fbclid","gclid","msclkid","mc_eid","_ga","_gl",
})

_P_AMZ = [
    re.compile(r'/dp/([A-Z0-9]{10})'),
    re.compile(r'/gp/product/([A-Z0-9]{10})'),
    re.compile(r'asin=([A-Z0-9]{10})', re.I),
]
_P_SHP = [
    re.compile(r'/product/(\d+)/(\d+)'),
    re.compile(r'/i\.(\d+)\.(\d+)'),
]
_P_MGL = re.compile(r'/(?:[^/]+/)?p/([a-z0-9]{6,})/?', re.I)

def _sem_tracking(p) -> str:
    params = {k: v[0] for k, v in parse_qs(p.query, keep_blank_values=False).items()
              if k.lower() not in _TRACKING}
    return urlunparse(p._replace(
        query=urlencode(params) if params else "", fragment=""))

def parse_link(url: str) -> ParsedLink:
    url = url.strip().rstrip('.,;)>').replace(' ', '%20')
    url = re.sub(r'%25([0-9A-Fa-f]{2})', r'%\1', url)

    if not url.startswith(("http://", "https://")):
        return ParsedLink(url, url, "desconhecido", "desconhecido",
                          "", False, "sem esquema")
    try:
        p = urlparse(url)
    except Exception as e:
        return ParsedLink(url, url, "desconhecido", "desconhecido",
                          "", False, str(e))

    nl   = p.netloc.lower().replace("www.", "")
    plat = classificar(url)

    if plat in (None, "preservar"):
        return ParsedLink(url, url, plat or "desconhecido", "desconhecido",
                          "", plat == "preservar", "")

    url_limpa = _sem_tracking(p)

    if plat == "expandir":
        return ParsedLink(url, url_limpa, "expandir", "encurtado", "", True, "")

    if plat == "amazon":
        sku = ""
        for pat in _P_AMZ:
            m = pat.search(p.path + "?" + p.query)
            if m: sku = m.group(1); break
        tipo = ("produto" if sku else
                "busca"   if re.search(r'/s[/?]|/deals|/b[/?]', p.path) else
                "evento"  if re.search(r'/events/|/stores/', p.path) else
                "desconhecido")
        return ParsedLink(url, url_limpa, plat, tipo, sku, True, "")

    if plat == "shopee":
        sku = ""
        for pat in _P_SHP:
            m = pat.search(p.path + "?" + p.query)
            if m: sku = f"{m.group(1)}.{m.group(2)}"; break
        tipo = "produto" if sku else "busca"
        return ParsedLink(url, url_limpa, plat, tipo, sku, True, "")

    if plat == "magalu":
        sku  = ""
        m    = _P_MGL.search(p.path)
        if m: sku = m.group(1)
        if "sacola" in nl and not p.path.strip("/"):
            return ParsedLink(url, url_limpa, plat, "invalido",
                              sku, False, "sacola sem produto")
        tipo = ("produto"  if sku else
                "lista"    if "/l/" in p.path else
                "selecao"  if "/selecao/" in p.path else
                "desconhecido")
        return ParsedLink(url, url_limpa, plat, tipo, sku, True, "")

    return ParsedLink(url, url_limpa, "desconhecido", "desconhecido",
                      "", False, "plat não mapeada")

def parse_links_bulk(urls: List[str]) -> List[ParsedLink]:
    res     = [parse_link(u) for u in urls]
    validos = [r for r in res if r.valido]
    log_lnk.info(f"🔍 Parser {len(validos)}/{len(urls)} válidos")
    return validos


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 22 ▸ SCHEDULER INTELIGENTE
# ══════════════════════════════════════════════════════════════════════════════

_SCH_LIM_H      = 15
_SCH_NOTURNO    = (0, 7)
_SCH_DELAY_MAX  = 15.0
_SCH_DELAY_NOT  = 10.0

_sch_cnt: Dict[str, List[float]] = {}
_sch_cnt_lock = asyncio.Lock()

async def _sch_add(plat: str):
    async with _sch_cnt_lock:
        agora = time.monotonic()
        fila  = _sch_cnt.setdefault(plat, [])
        fila.append(agora)
        _sch_cnt[plat] = [t for t in fila if agora - t < 3600]

async def _sch_count(plat: str) -> int:
    async with _sch_cnt_lock:
        agora = time.monotonic()
        return sum(1 for t in _sch_cnt.get(plat, []) if agora - t < 3600)

def _sch_delay(score: float, hora: int) -> float:
    h0, h1     = _SCH_NOTURNO
    base       = _SCH_DELAY_NOT if h0 <= hora < h1 else 0.0
    if score >= 1.0: return base
    if score >= 0.5: return min(base + (1.0 - score) * 10.0, _SCH_DELAY_MAX)
    return min(base + (0.5 - score) * 20.0, _SCH_DELAY_MAX)

async def scheduler_gate(plat: str, texto: str) -> float:
    if _KW_EVENTO.search(texto): return 0.0
    hora  = int(time.strftime("%H"))
    count = await _sch_count(plat)
    if count >= _SCH_LIM_H:
        log_sys.warning(f"⚠️ Scheduler limite/h | {plat}")
        return -1.0
    score = db_score_hora(plat, hora)
    return _sch_delay(score, hora)

async def scheduler_ok(plat: str):
    hora = int(time.strftime("%H"))
    await _sch_add(plat)
    db_registrar_sch(plat, hora)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 23 ▸ ANTI-SATURAÇÃO
# S1 — Mesma plataforma muito densa (delay)
# S2 — Burst de envios rápidos (delay)
# ══════════════════════════════════════════════════════════════════════════════

_SAT_MAX_PLAT   = 8
_SAT_BURST_LIM  = 5
_SAT_BURST_JAN  = 60

_burst: List[float] = []
_burst_lock = asyncio.Lock()

async def _burst_add():
    async with _burst_lock:
        agora = time.monotonic()
        _burst.append(agora)
        while _burst and agora - _burst[0] > _SAT_BURST_JAN:
            _burst.pop(0)

async def _burst_count() -> int:
    async with _burst_lock:
        agora = time.monotonic()
        return sum(1 for t in _burst if agora - t <= _SAT_BURST_JAN)

async def antisaturacao_gate(plat: str, texto: str) -> float:
    """Retorna delay extra em segundos. Nunca bloqueia."""
    if _KW_EVENTO.search(texto):
        return 0.0
    delay = 0.0
    if db_count_sat(plat) >= _SAT_MAX_PLAT:
        delay += 8.0
    if await _burst_count() >= _SAT_BURST_LIM:
        delay += 5.0
    return delay

def antisaturacao_ok(plat: str, sku: str):
    db_registrar_sat(plat, sku)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 24 ▸ BUFFER ORCHESTRATOR + PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

_WORKERS_MAX = 4
_FILA_MAX    = 200
_COALESCE_MS = 800

_buf:     List               = []
_buf_lck  = asyncio.Lock()
_buf_evt  = asyncio.Event()
_w_ativos = 0
_w_lck    = asyncio.Lock()
_coal: Dict[str, float]     = {}

_RE_TITULO_GEN = re.compile(
    r'^\s*(?:cupons?\s+(?:shopee|amazon|magalu)|novos?\s+cupons?|'
    r'links?\s+de\s+cupom)\s*$', re.I | re.M)

def _prio(texto: str) -> int:
    tl = texto.lower()
    if "amazon" in tl: return 1
    if "shopee" in tl: return 2
    if "magalu" in tl: return 3
    return 9

def _fp_r(texto: str) -> str:
    return hashlib.sha256(
        re.sub(r'\s+', '', texto.lower())[:80].encode()
    ).hexdigest()[:12]

def _tem_contexto(texto: str) -> bool:
    linhas = [l.strip() for l in texto.splitlines()
              if l.strip() and not re.match(r'https?://', l.strip())]
    if not linhas:
        return False
    total = " ".join(linhas)
    for ind in [r'off',r'%',r'r\$',r'cupom',r'desconto',r'promoção',
                r'oferta',r'grátis',r'evento',r'live',r'relâmpago',
                r'flash',r'volta',r'normalizou',r'a\s+partir']:
        if re.search(ind, total, re.I):
            return True
    return len(total) > 20

async def _enfileirar(event, is_edit: bool):
    texto = event.message.text or ""
    if not texto.strip():
        return
    fp    = _fp_r(texto)
    agora = time.monotonic()
    async with _buf_lck:
        if not is_edit and agora - _coal.get(fp, 0.0) < _COALESCE_MS / 1000:
            return
        _coal[fp] = agora
        if len(_buf) >= _FILA_MAX:
            log_sys.warning(f"⚠️ Fila cheia — descartando {event.message.id}")
            return
        heapq.heappush(_buf, (0 if is_edit else _prio(texto), agora, event, is_edit))
    _buf_evt.set()

async def _worker_loop():
    global _w_ativos
    while True:
        await _buf_evt.wait()
        while True:
            item = None
            async with _buf_lck:
                if _buf:
                    item = heapq.heappop(_buf)
                else:
                    _buf_evt.clear()
                    break
            if item is None:
                break
            prio, ts, event, is_edit = item
            async with _w_lck:
                if _w_ativos >= _WORKERS_MAX:
                    async with _buf_lck:
                        heapq.heappush(_buf, item)
                        _buf_evt.set()
                    await asyncio.sleep(0.2)
                    break
                _w_ativos += 1
            try:
                if time.monotonic() - ts > 60:
                    continue
                await _pipeline(event, is_edit)
            except Exception as e:
                log_sys.error(f"❌ Worker: {e}", exc_info=True)
            finally:
                async with _w_lck:
                    _w_ativos -= 1

async def _pipeline(event, is_edit: bool = False):
    """
    Pipeline único de decisão.
    Analisa: cupom + texto + contexto semântico.
    Ignora: link, emoji, preço, formatação.
    """
    msg_id = event.message.id
    texto  = event.message.text or ""
    chat   = await event.get_chat()
    uname  = (chat.username or str(event.chat_id)).lower()

    log_tg.info(
        f"{'✏️' if is_edit else '📩'} @{uname} | id={msg_id} | "
        f"{len(texto)}c | fila={len(_buf)} w={_w_ativos}")

    if not texto.strip(): return

    # Anti-loop
    if not is_edit:
        if await _foi_processado(msg_id): return
    else:
        loop   = asyncio.get_event_loop()
        mapa_c = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        if str(msg_id) not in mapa_c: return

    # Filtro de texto
    if texto_bloqueado(texto): return

    # Limpeza
    tc = limpar_ruido_textual(texto)

    # Contexto mínimo
    if not _tem_contexto(tc): return

    # Extração + parser robusto
    links_c, links_p = extrair_links(tc)
    parsed           = parse_links_bulk(links_c)
    diretos          = [r.url_limpa for r in parsed if r.plat != "expandir"]
    expandir         = [r.url_limpa for r in parsed if r.plat == "expandir"]

    if not diretos and not expandir and not links_p:
        if "fadadoscupons" not in uname: return

    # Conversão paralela
    mapa, plat = await converter_links(diretos + expandir)
    if links_c and not mapa and not links_p: return

    # Extrai cupom e SKU — base da decisão
    cup = _extrair_cupom(tc)
    sku = next((f"{r.plat[:3]}_{r.sku}" for r in parsed if r.sku), ""
               ) or _extrair_sku(mapa)

    if not is_edit:
        # Anti-saturação (só delay, nunca bloqueia)
        delay_sat = await antisaturacao_gate(plat, tc)

        # Dedup semântica — decisão baseada em cupom + texto + contexto
        if not deve_enviar(plat, cup, tc):
            return

        # Scheduler
        delay_sch = await scheduler_gate(plat, tc)
        if delay_sch == -1.0:
            log_sys.warning("⚠️ Limite/h — descartando")
            return

        total = delay_sch + delay_sat
        if total > 0:
            await asyncio.sleep(total)

    # Renderização
    msg_final = renderizar(tc, mapa, links_p, plat)

    # ── Imagem ────────────────────────────────────────────────────────────
    media_orig = event.message.media
    tem_img    = _tem_midia(media_orig)
    img        = None
    eh_cup     = _eh_cupom_texto(tc)

    if tem_img:
        try:
            img, _ = await preparar_imagem(media_orig, True)
            log_img.debug("✅ Imagem da mensagem original")
        except Exception as e:
            log_img.warning(f"⚠️ Imagem original falhou: {e}")

    if img is None:
        if eh_cup:
            if plat == "shopee" and os.path.exists(_IMG_SHP):
                img = _IMG_SHP
                log_img.debug("🟣 Fallback Shopee cupom")
            elif plat == "amazon" and os.path.exists(_IMG_AMZ):
                img = _IMG_AMZ
                log_img.debug("🟠 Fallback Amazon cupom")
            elif plat == "magalu" and os.path.exists(_IMG_MGL):
                img = _IMG_MGL
                log_img.debug("🔵 Fallback Magalu cupom")
        elif mapa:
            img_url = await buscar_imagem(list(mapa.values())[0])
            if img_url:
                try:
                    img, _ = await preparar_imagem(img_url, False)
                    log_img.debug(f"✅ Imagem produto: {img_url[:60]}")
                except Exception as e:
                    log_img.warning(f"⚠️ Preparar imagem produto: {e}")

    # ── Rate-limit adaptativo ─────────────────────────────────────────────
    hora_atual = int(time.strftime("%H"))
    global _INTERVALO_MIN
    _INTERVALO_MIN = 0.8 if 8 <= hora_atual < 22 else 1.5

    await _rate_limit()

    async with _SEM_ENVIO:
        loop = asyncio.get_event_loop()
        mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        try:
            if is_edit:
                id_d = mp[str(msg_id)]
                for t in range(1, 4):
                    try:
                        await client.edit_message(GRUPO_DESTINO, id_d, msg_final)
                        break
                    except MessageNotModifiedError: break
                    except FloodWaitError as e:     await asyncio.sleep(e.seconds)
                    except Exception:
                        if t < 3: await asyncio.sleep(2 ** t)
                return

            sent = None
            for t in range(1, 4):
                try:
                    sent = await _enviar(msg_final, img)
                    break
                except FloodWaitError as e: await asyncio.sleep(e.seconds)
                except Exception:
                    if t == 1: img = None
                    elif t < 3: await asyncio.sleep(2 ** t)

            if sent:
                mp[str(msg_id)] = sent.id
                await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
                await _marcar(msg_id)
                await scheduler_ok(plat)
                antisaturacao_ok(plat, sku)
                await _burst_add()
                log_sys.info(
                    f"🚀 [OK] @{uname} → {GRUPO_DESTINO} | "
                    f"{msg_id}→{sent.id} | {plat.upper()} sku={sku}")

        except Exception as e:
            log_sys.error(f"❌ CRÍTICO: {e}", exc_info=True)

async def processar(event, is_edit: bool = False):
    await _enfileirar(event, is_edit)

async def _iniciar_orchestrator():
    log_sys.info(f"🎛 Orchestrator | workers={_WORKERS_MAX} fila={_FILA_MAX}")
    asyncio.create_task(_worker_loop())


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 25 ▸ HEALTH CHECK
# ══════════════════════════════════════════════════════════════════════════════

async def _health_check():
    while True:
        await asyncio.sleep(300)
        try:
            db_limpar(dias=7)
            log_hc.info(
                f"💚 cache={len(ler_cache())} | mapa={len(ler_mapa())} | "
                f"anti-loop={len(_IDS_PROC)} | "
                f"fila={len(_buf)} w={_w_ativos} | "
                f"PIL={'OK' if _PIL_OK else 'OFF'}")
        except Exception as e:
            log_hc.error(f"❌ Health: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 26 ▸ INICIALIZAÇÃO COM AUTO-RESTART
# ══════════════════════════════════════════════════════════════════════════════

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

async def _run():
    _init_db()
    log_sys.info("🔌 Conectando...")
    await client.connect()
    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida")
        return False

    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 Grupos: {GRUPOS_ORIGEM}")
    log_sys.info(f"📣 Destino: {GRUPO_DESTINO}")
    log_sys.info(f"🟠 Amazon: {_AMZ_TAG}")
    log_sys.info(f"🟣 Shopee: {_SHP_APP_ID}")
    log_sys.info(f"🔵 Magalu: promoter={_MGL_PROMOTER} slug={_MGL_SLUG}")
    log_sys.info(f"🖼  Pillow: {'OK' if _PIL_OK else 'pip install Pillow'}")
    log_sys.info("🚀 FOGUETÃO v74.0 — ONLINE")

    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def on_new(event):
        try:
            await processar(event, is_edit=False)
        except Exception as e:
            log_sys.error(f"❌ on_new: {e}", exc_info=True)

    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def on_edit(event):
        try:
            await processar(event, is_edit=True)
        except Exception as e:
            log_sys.error(f"❌ on_edit: {e}", exc_info=True)

    asyncio.create_task(_health_check())
    await _iniciar_orchestrator()
    await client.run_until_disconnected()
    return True

async def main():
    while True:
        try:
            await _run()
        except (AuthKeyUnregisteredError, SessionPasswordNeededError) as e:
            log_sys.error(f"❌ Auth fatal: {e}")
            break
        except Exception as e:
            log_sys.error(f"💥 Caiu: {e} — restart 15s", exc_info=True)
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())

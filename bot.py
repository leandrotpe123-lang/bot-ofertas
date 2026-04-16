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

GRUPOS_ORIGEM  = ["promotom", "fadapromos", "SamuelF3lipePromo", "paraseubaby", "fumotom", "botofera", "fadadoscupons"]
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

_IMG_SHP = "cupom-shopee.jpg"
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
    "Monitor Samsung", "Computador Home Essential", "Monitor gamer", "Fonte Mancer", "Placa de video", "Monitor LG",
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
# MÓDULO 6 ▸ DESENCURTADOR — 15 CAMADAS
# Suporta amzlink.to, amzn.to, bit.ly e qualquer outro.
# HEAD → GET completo → meta-refresh → JS → og:url → canônico
# ══════════════════════════════════════════════════════════════════════════════

# Domínios que SEMPRE precisam de GET (bloqueiam HEAD)
_FORCA_GET = frozenset({
    "amzlink.to", "amzn.to", "a.co", "amzn.com",
    "bit.ly", "tinyurl.com", "rb.gy", "is.gd",
    "cutt.ly", "ow.ly", "buff.ly",
})

async def desencurtar(url: str, sessao: aiohttp.ClientSession,
                       depth: int = 0) -> str:
    if depth > 15:
        log_lnk.warning(f"⚠️ Profundidade máx atingida d={depth}: {url[:60]}")
        return url

    url = url.strip().rstrip('.,;)>')
    if not url.startswith(("http://", "https://")):
        return url

    nl = _netloc(url)

    hdrs = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    try:
        # HEAD só para domínios que aceitam
        usar_head = nl not in _FORCA_GET and not any(
            nl.endswith("." + d) for d in _FORCA_GET)

        if usar_head:
            try:
                async with sessao.head(
                    url, headers=hdrs, allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=8),
                    max_redirects=20,
                ) as r:
                    final = str(r.url)
                    if final != url:
                        log_lnk.debug(f"  HEAD d={depth} → {final[:70]}")
                        return await desencurtar(final, sessao, depth + 1)
                    # HEAD retornou mesma URL mas pode ter JS redirect
                    # não retorna ainda, cai no GET
            except Exception:
                pass

        # GET completo — lê HTML para detectar qualquer tipo de redirect
        async with sessao.get(
            url, headers=hdrs, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=15),
            max_redirects=20,
        ) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")

            # Redirect HTTP já resolvido
            if pos != url:
                log_lnk.debug(f"  GET-redir d={depth} → {pos[:70]}")
                return await desencurtar(pos, sessao, depth + 1)

            soup = BeautifulSoup(html, "html.parser")

            # Meta refresh
            ref = soup.find(
                "meta", attrs={"http-equiv": re.compile("refresh", re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)",
                              ref["content"], re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    if novo.startswith("http"):
                        log_lnk.debug(f"  META d={depth} → {novo[:70]}")
                        return await desencurtar(novo, sessao, depth + 1)

            # JS location — amzlink.to usa window.location
            for pat in [
                r'window\.location(?:\.href)?\s*=\s*["\']([^"\']{15,})["\']',
                r'location\.replace\s*\(\s*["\']([^"\']{15,})["\']\s*\)',
                r'location\.assign\s*\(\s*["\']([^"\']{15,})["\']\s*\)',
                r'(?:var|let|const)\s+\w*[Uu]rl\w*\s*=\s*["\']([^"\']{15,})["\']',
                r'window\.open\s*\(\s*["\']([^"\']{15,})["\']',
            ]:
                mj = re.search(pat, html)
                if mj:
                    destino = mj.group(1)
                    if destino.startswith("http"):
                        log_lnk.debug(f"  JS d={depth} → {destino[:70]}")
                        return await desencurtar(destino, sessao, depth + 1)

            # og:url como fallback
            og = soup.find("meta", attrs={"property": "og:url"})
            if og and og.get("content", "").startswith("http"):
                if og["content"] != url:
                    log_lnk.debug(f"  OG:URL d={depth} → {og['content'][:70]}")
                    return await desencurtar(og["content"], sessao, depth + 1)

            # Link canônico
            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href", "").startswith("http"):
                if canon["href"] != url:
                    log_lnk.debug(f"  CANON d={depth} → {canon['href'][:70]}")
                    return await desencurtar(canon["href"], sessao, depth + 1)

            log_lnk.debug(f"  FIM d={depth}: {pos[:70]}")
            return pos

    except asyncio.TimeoutError:
        log_lnk.warning(f"⏱ Timeout d={depth}: {url[:60]}")
        return url
    except Exception as e:
        log_lnk.error(f"❌ Desencurtar d={depth}: {e} | {url[:50]}")
        return url


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 7 ▸ MOTOR AMAZON — ISOLADO
# Suporta amzlink.to + todos os formatos Amazon.
# Link limpo: só o necessário, nem curto demais nem longo demais.
# Debug isolado com log_amz.
# ══════════════════════════════════════════════════════════════════════════════

# Domínios Amazon que precisam de desencurtamento
_AMZ_ENCURTADOS = frozenset({
    "amzlink.to", "amzn.to", "a.co", "amzn.com",
})

_AMZ_LIXO = frozenset({
    "ascsubtag","btn_ref","ref_","ref","smid","sprefix","spla",
    "dchild","linkcode","linkid","camp","creative",
    "pf_rd_p","pf_rd_r","pd_rd_wg","pd_rd_w","content-id",
    "pd_rd_r","pd_rd_i","ie","qid","_encoding","dib","dib_tag",
    "m","marketplaceid","ufe","th","psc","ingress","visitid",
    "lp_context_asin","redirectasin","redirectmerchantid",
    "redirectasincustomeraction","ds","rnid","sr",
})
_AMZ_MANTER = frozenset({
    "tag","keywords","node","k","i","rh","n","field-keywords",
})


def _limpar_url_amazon(url: str) -> str:
    """
    Reconstrói URL Amazon do zero por tipo.
    Produto   → amazon.com.br/dp/ASIN?tag=
    Campanha  → path + tag
    Busca     → path + params funcionais + tag
    """
    p    = urlparse(url)
    path = p.path

    # /dp/ASIN
    m = re.match(r'(/dp/[A-Z0-9]{10})', path)
    if m:
        return urlunparse(p._replace(
            scheme="https",
            netloc="www.amazon.com.br",
            path=m.group(1),
            query=f"tag={_AMZ_TAG}",
            fragment=""))

    # /gp/product/ASIN → normaliza para /dp/ASIN
    m = re.match(r'/gp/product/([A-Z0-9]{10})', path)
    if m:
        return urlunparse(p._replace(
            scheme="https",
            netloc="www.amazon.com.br",
            path=f"/dp/{m.group(1)}",
            query=f"tag={_AMZ_TAG}",
            fragment=""))

    # Campanha /promotion/psp/
    if "/promotion/psp/" in path:
        return urlunparse(p._replace(
            query=f"tag={_AMZ_TAG}",
            fragment=""))

    # Busca, eventos, landing pages
    # Mantém params funcionais, remove rastreamento
    params: dict = {}
    for k, v in parse_qs(p.query, keep_blank_values=False).items():
        kl = k.lower()
        if kl in _AMZ_MANTER:
            params[k] = v[0]
        elif kl not in _AMZ_LIXO and len(v[0]) < 60:
            params[k] = v[0]
    params["tag"] = _AMZ_TAG

    return urlunparse(p._replace(
        query=urlencode(params),
        fragment=""))


async def motor_amazon(url: str,
                        sessao: aiohttp.ClientSession) -> Optional[str]:
    """
    Motor exclusivo Amazon.
    Sempre desencurta antes de limpar — garante URL real.
    Suporta amzlink.to, amzn.to, a.co e URL direta.
    """
    log_amz.debug(f"▶ IN: {url[:80]}")

    # Sempre desencurta — captura amzlink.to e redirects silenciosos
    try:
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)
    except Exception as e:
        log_amz.error(f"❌ Desencurtar falhou: {e} | {url[:60]}")
        return None

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
# MÓDULO 9 ▸ MOTOR MAGALU — CUTTLY COM FALLBACK + EDIÇÃO
#
# Se Cuttly falhar:
#   1. Envia com link longo (já afiliado) imediatamente
#   2. Continua tentando encurtar em background
#   3. Quando encurtar, edita a mensagem com link curto
#
# Troca MAGALU_SLUG em qualquer formato:
#   /magazineXXX/produto/...  → /magazineleo12/produto/...
#   /magazineXXX/lojista/...  → /magazineleo12/lojista/...
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
    Substitui slug do afiliado em qualquer formato.
    Preserva 100% do restante do path.
    """
    p    = urlparse(url)
    path = p.path

    # Troca /magazineQUALQUERCOISA/ pelo slug configurado
    path = re.sub(
        r'^(/magazine)[^/]+',
        rf'\1{_MGL_SLUG}',
        path,
    )
    log_mgl.debug(f"  Slug: {path[:60]}")

    params = {k: v[0] for k, v in
              parse_qs(p.query, keep_blank_values=True).items()}

    # Remove params antigos de afiliado
    for k in [
        "tag","partnerid","promoterid","afforcedeeplink",
        "deeplinkvalue","isretargeting","partner_id","promoter_id",
        "utm_source","utm_medium","utm_campaign","pid","c",
        "af_force_deeplink","deep_link_value",
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

    return urlunparse(p._replace(
        path=path, query=urlencode(params), fragment=""))


async def _cuttly_tentar(url: str,
                          sessao: aiohttp.ClientSession) -> Optional[str]:
    """
    Tenta encurtar via Cuttly.
    Retorna None se falhar — sem lançar exceção.
    """
    api = (f"https://cutt.ly/api/api.php"
           f"?key={_CUTTLY_KEY}&short={quote(url, safe='')}")
    for t in range(1, 4):
        try:
            async with _SEM_HTTP:
                async with sessao.get(
                    api, timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    if r.status != 200:
                        log_mgl.warning(f"  ⚠️ Cuttly HTTP {r.status} t={t}/3")
                        await asyncio.sleep(2 ** t)
                        continue
                    data   = await r.json(content_type=None)
                    status = data.get("url", {}).get("status")
                    if status in (7, 2):
                        short = data["url"].get("shortLink")
                        log_mgl.info(f"  ✂️ Cuttly OK t={t}: {short}")
                        return short
                    log_mgl.warning(
                        f"  ⚠️ Cuttly status={status} t={t}/3")
                    await asyncio.sleep(2 ** t)
        except asyncio.TimeoutError:
            log_mgl.warning(f"  ⏱ Cuttly timeout t={t}/3")
            await asyncio.sleep(2 ** t)
        except Exception as e:
            log_mgl.error(f"  ❌ Cuttly t={t}: {e}")
            await asyncio.sleep(2 ** t)
    return None


async def _cuttly_background(url_longo: str, msg_id_destino: int):
    """
    Tenta encurtar em background por até 5 minutos.
    Quando conseguir, edita a mensagem no canal destino.
    """
    log_mgl.info(f"  🔄 Background encurtador | msg={msg_id_destino}")

    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as sessao:
        for tentativa in range(10):    # tenta por ~5 min
            await asyncio.sleep(30)   # espera 30s entre tentativas
            try:
                short = await _cuttly_tentar(url_longo, sessao)
                if short:
                    # Edita a mensagem trocando link longo pelo encurtado
                    loop  = asyncio.get_event_loop()
                    mapa  = await loop.run_in_executor(_EXECUTOR, ler_mapa)
                    id_dest = mapa.get(str(msg_id_destino))
                    if id_dest:
                        try:
                            msg_atual = await client.get_messages(
                                GRUPO_DESTINO, ids=id_dest)
                            if msg_atual and msg_atual.text:
                                novo_texto = msg_atual.text.replace(
                                    url_longo, short)
                                if novo_texto != msg_atual.text:
                                    await client.edit_message(
                                        GRUPO_DESTINO, id_dest, novo_texto)
                                    log_mgl.info(
                                        f"  ✅ Editado com link curto | "
                                        f"msg={id_dest}")
                        except Exception as e:
                            log_mgl.warning(f"  ⚠️ Edição background: {e}")
                    return
            except Exception as e:
                log_mgl.warning(
                    f"  ⚠️ Background t={tentativa}/10: {e}")

    log_mgl.warning(f"  ❌ Background encurtador esgotou tentativas")


async def motor_magalu(url: str,
                        sessao: aiohttp.ClientSession) -> Optional[str]:
    """
    Motor exclusivo Magalu.
    Se Cuttly falhar: retorna link longo afiliado e agenda background.
    Debug isolado com log_mgl.
    """
    log_mgl.debug(f"▶ IN: {url[:80]}")

    nl = _netloc(url)
    if "maga.lu" in nl or nl in _ENCURTADORES or classificar(url) == "expandir":
        try:
            async with _SEM_HTTP:
                url = await desencurtar(url, sessao)
            log_mgl.debug(f"  EXP: {url[:80]}")
        except Exception as e:
            log_mgl.error(f"  ❌ Desencurtar: {e}")
            return None

    if classificar(url) != "magalu":
        log_mgl.warning(f"  ⚠️ Não é Magalu: {url[:70]}")
        return None

    if not _validar_magalu(url):
        return None

    afiliado = _afiliar_magalu(url)
    log_mgl.debug(f"  AFL: {afiliado[:80]}")

    # Tenta encurtar
    short = await _cuttly_tentar(afiliado, sessao)

    if short:
        # Cache o link encurtado
        db_set_link(url, short, "magalu")
        log_mgl.info(f"  ✅ OUT (curto): {short}")
        return short

    # Cuttly falhou → retorna link longo e agenda background
    log_mgl.warning(f"  ⚠️ Cuttly falhou — enviando link longo, editará depois")
    # Retorna o link longo afiliado para envio imediato
    # O pipeline vai salvar o msg_id e o background vai editar
    return afiliado


# Função chamada pelo pipeline após envio bem-sucedido com link longo
async def _agendar_edicao_magalu(url_longo: str, msg_id_origem: int):
    """Agenda a substituição do link longo pelo encurtado após envio."""
    asyncio.create_task(
        _cuttly_background(url_longo, msg_id_origem))


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
        return {}, "desconhecido"

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

    plat_p = max(set(plats), key=plats.count) if plats else (
    classificar(links[0]) if links else "desconhecido"
    )
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
# MÓDULO 15 ▸ DEDUPLICAÇÃO — BLOQUEIA SÓ PRODUTO IGUAL
#
# REGRA CENTRAL:
#   Produto diferente → SEMPRE envia, mesmo que chegue ao mesmo tempo
#   Produto igual     → bloqueia se já enviado na janela
#
# Amazon  → chave = ASIN
# Magalu  → chave = ID do produto
# Shopee  → chave = cupom + hash semântico
# Outros  → chave = alma do texto
# ══════════════════════════════════════════════════════════════════════════════

_SIM_FORTE  = 0.88
_SIM_MEDIO  = 0.78
_SIM_EVENTO = 0.60

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

_FALSO_CUPOM = frozenset({
    "FRETE","GRÁTIS","GRATIS","AMAZON","SHOPEE","MAGALU","LINK",
    "CLIQUE","ACESSE","CONFIRA","HOJE","AGORA","PROMO","OFF",
    "BLACK","SUPER","MEGA","ULTRA","VIP","NOVO","NOVA","NUM","PRECO","PCT",
    "PS5","PS4","XBOX","USB","ATX","RGB","LED","HD","SSD","RAM",
    "APP","BOT","API","URL",
})

def _rm_ac(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", t)
                   if unicodedata.category(c) != "Mn")

def _extrair_todos_cupons(texto: str) -> frozenset:
    return frozenset(re.findall(
        r'\b([A-Z][A-Z0-9_-]{3,19})\b', texto)) - _FALSO_CUPOM

def _extrair_cupom(texto: str) -> str:
    return next(iter(sorted(_extrair_todos_cupons(texto))), "")

def _extrair_valor(texto: str) -> str:
    m = _KW_PRECO.search(texto)
    if not m: return ""
    return re.sub(r'[R$\s.,]', '', m.group(0))

def _extrair_asin(texto: str, mapa: dict) -> str:
    for url in list(mapa.values()) + re.findall(r'https?://\S+', texto):
        for pat in [r'/dp/([A-Z0-9]{10})',
                    r'/gp/product/([A-Z0-9]{10})']:
            m = re.search(pat, url)
            if m: return m.group(1)
    return ""

def _extrair_id_magalu(texto: str, mapa: dict) -> str:
    for url in list(mapa.values()) + re.findall(r'https?://\S+', texto):
        m = re.search(r'/p/([a-z0-9]{6,})/?', url, re.I)
        if m: return m.group(1)
    return ""

def _extrair_sku(mapa: dict) -> str:
    asin = _extrair_asin("", mapa)
    if asin: return f"amz_{asin}"
    id_m = _extrair_id_magalu("", mapa)
    if id_m: return f"mgl_{id_m}"
    for url in mapa.values():
        m = re.search(r'/i\.(\d+\.\d+)', url)
        if m: return f"shp_{m.group(1)}"
    return ""

def _normalizar_alma(texto: str) -> str:
    t = _rm_ac(texto.lower())
    t = re.sub(r'https?://\S+', ' ', t)
    t = _RE_EMJ_STRIP.sub(' ', t)
    t = re.sub(r'r\$\s*[\d.,]+', ' PRECO ', t)
    t = re.sub(r'\b\d+\s*%', ' PCT ', t)
    t = re.sub(r'\b\d+\b', ' NUM ', t)
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return ' '.join(sorted(
        w for w in t.split()
        if w not in _RUIDO_NORM and len(w) > 1))

def _detectar_campanha(texto: str) -> str:
    tl     = texto.lower()
    tokens = []
    m = _KW_EVENTO.search(texto)
    if m: tokens.append(f"evt_{m.group(0).lower()}")
    if "amazon app"  in tl: tokens.append("amazon_app")
    if "mastercard"  in tl: tokens.append("mastercard")
    if "prime"       in tl: tokens.append("prime")
    if "magalu app"  in tl: tokens.append("magalu_app")
    return "|".join(sorted(tokens)) if tokens else "geral"

def _eh_reativacao(texto: str) -> bool:
    return bool(re.search(
        r'\b(?:voltou|voltando|ativo\s+novamente|liberado\s+novamente|'
        r'normalizou|renovado|estoque\s+renovado|regularizou|'
        r'ainda\s+ativo|oferta\s+ativa|reativado)\b',
        texto, re.I))

# Fingerprints específicos por plataforma
def _fp_amazon(asin: str) -> str:
    return hashlib.sha256(f"amz|{asin}".encode()).hexdigest()

def _fp_magalu(id_prod: str) -> str:
    return hashlib.sha256(f"mgl|{id_prod}".encode()).hexdigest()

def _fp_shopee(cupons: frozenset, alma: str) -> str:
    return hashlib.sha256(
        f"shp|{'|'.join(sorted(cupons))}|{alma}".encode()).hexdigest()

def _fp_generico(plat: str, cupons: frozenset, alma: str) -> str:
    return hashlib.sha256(
        f"{plat}|{'|'.join(sorted(cupons))}|{alma}".encode()).hexdigest()


def deve_enviar(plat: str, cupom: str, texto: str,
                mapa_links: dict = None) -> bool:
    """
    Bloqueia SOMENTE produto igual na janela temporal.
    Produtos diferentes do mesmo grupo → SEMPRE passa.
    """
    mapa_links = mapa_links or {}

    # Reativação → sempre reenvia
    if _eh_reativacao(texto):
        log_dedup.info("✅ Reativação — reenvia")
        _registrar_dedupe(plat, cupom, texto, mapa_links)
        return True

    cupons   = _extrair_todos_cupons(texto)
    alma     = _normalizar_alma(texto)
    campanha = _detectar_campanha(texto)
    eh_evt   = bool(_KW_EVENTO.search(texto))
    asin     = _extrair_asin(texto, mapa_links)
    id_mgl   = _extrair_id_magalu(texto, mapa_links)

    # ── AMAZON: bloqueia por ASIN ─────────────────────────────────────────
    if plat == "amazon":
        if asin:
            fp = _fp_amazon(asin)
            # Checa janela rápida (10 min)
            entradas = db_buscar_dedupe_por_asin(asin, plat)
            rapidas  = [e for e in entradas
                        if time.time() - e["ts"] < JANELA_RAPIDA]
            if rapidas:
                log_dedup.info(
                    f"🔁 [AMZ] ASIN={asin} já enviado "
                    f"({len(rapidas)}x nos últimos 10min)")
                return False
            # Checa histórico 24h
            if db_get_dedupe(fp):
                log_dedup.info(f"🔁 [AMZ-24H] ASIN={asin}")
                return False
        else:
            # Sem ASIN: usa alma como fallback
            fp = _fp_generico(plat, cupons, alma)
            if db_get_dedupe(fp):
                log_dedup.info(f"🔁 [AMZ-ALMA] sem ASIN, alma repetida")
                return False

        _registrar_dedupe(plat, cupom, texto, mapa_links)
        log_dedup.debug(f"✅ Amazon nova | ASIN={asin or 'N/A'}")
        return True

    # ── MAGALU: bloqueia por ID do produto ────────────────────────────────
    if plat == "magalu":
        if id_mgl:
            entradas = db_buscar_dedupe_por_id(id_mgl, plat)
            rapidas  = [e for e in entradas
                        if time.time() - e["ts"] < JANELA_RAPIDA]
            if rapidas:
                log_dedup.info(
                    f"🔁 [MGL] ID={id_mgl} já enviado "
                    f"({len(rapidas)}x nos últimos 10min)")
                return False
            fp = _fp_magalu(id_mgl)
            if db_get_dedupe(fp):
                log_dedup.info(f"🔁 [MGL-24H] ID={id_mgl}")
                return False
        else:
            fp = _fp_generico(plat, cupons, alma)
            if db_get_dedupe(fp):
                log_dedup.info(f"🔁 [MGL-ALMA] sem ID, alma repetida")
                return False

        _registrar_dedupe(plat, cupom, texto, mapa_links)
        log_dedup.debug(f"✅ Magalu nova | ID={id_mgl or 'N/A'}")
        return True

    # ── SHOPEE: bloqueia por cupom + semântica ────────────────────────────
    if plat == "shopee":
        # Cupom exato → bloqueia direto
        if cupons:
            fp_cup = _fp_shopee(cupons, "")
            if db_get_dedupe(fp_cup):
                log_dedup.info(
                    f"🔁 [SHP-CUPOM] cupons={sorted(cupons)}")
                return False

        # Hash semântico completo
        fp_sem = _fp_shopee(cupons, alma)
        if db_get_dedupe(fp_sem):
            log_dedup.info(f"🔁 [SHP-SEM]")
            return False

        # Janela rápida — alma similar
        rapidas = db_buscar_dedupe_janela_rapida(plat)
        for e in rapidas:
            cupons_c    = frozenset(e.get("cupons", []))
            cupom_igual = bool(cupons & cupons_c) if cupons else False
            alma_c      = e.get("alma", "")
            sim         = SequenceMatcher(
                None, alma, alma_c).ratio() if alma_c else 0.0

            if cupom_igual and sim >= _SIM_MEDIO:
                log_dedup.info(
                    f"🔁 [SHP-JANELA] cupom={cupons & cupons_c} "
                    f"sim={sim:.2f}")
                return False
            if sim >= _SIM_FORTE:
                log_dedup.info(f"🔁 [SHP-SIM] sim={sim:.2f}")
                return False
            if eh_evt and cupom_igual:
                log_dedup.info(f"🔁 [SHP-EVT]")
                return False

        _registrar_dedupe(plat, cupom, texto, mapa_links)
        log_dedup.debug(f"✅ Shopee nova | cupons={sorted(cupons)}")
        return True

    # ── GENÉRICO ──────────────────────────────────────────────────────────
    fp = _fp_generico(plat, cupons, alma)
    if db_get_dedupe(fp):
        log_dedup.info(f"🔁 [GEN] plat={plat}")
        return False

    rapidas = db_buscar_dedupe_janela_rapida(plat)
    for e in rapidas:
        cupons_c    = frozenset(e.get("cupons", []))
        cupom_igual = bool(cupons & cupons_c) if cupons else False
        alma_c      = e.get("alma", "")
        sim         = SequenceMatcher(
            None, alma, alma_c).ratio() if alma_c else 0.0
        if cupom_igual and sim >= _SIM_MEDIO:
            log_dedup.info(f"🔁 [GEN-CUPOM] sim={sim:.2f}")
            return False
        if sim >= _SIM_FORTE:
            log_dedup.info(f"🔁 [GEN-SIM] sim={sim:.2f}")
            return False

    _registrar_dedupe(plat, cupom, texto, mapa_links)
    log_dedup.debug(f"✅ Genérico nova | plat={plat}")
    return True


def _registrar_dedupe(plat: str, cupom: str, texto: str,
                       mapa_links: dict = None):
    mapa_links = mapa_links or {}
    cupons     = _extrair_todos_cupons(texto)
    alma       = _normalizar_alma(texto)
    camp       = _detectar_campanha(texto)
    asin       = _extrair_asin(texto, mapa_links)
    id_mgl     = _extrair_id_magalu(texto, mapa_links)

    if plat == "amazon" and asin:
        fp = _fp_amazon(asin)
    elif plat == "magalu" and id_mgl:
        fp = _fp_magalu(id_mgl)
    elif plat == "shopee":
        fp = _fp_shopee(cupons, alma)
    else:
        fp = _fp_generico(plat, cupons, alma)

    db_set_dedupe(fp, plat, list(cupons), alma, camp, asin, id_mgl)

    # JSON legado
    try:
        cache = ler_cache()
        cache[fp] = {
            "plat": plat, "cupom": cupom.upper(),
            "cupons": list(cupons), "camp": camp,
            "alma": alma, "ts": time.time(),
        }
        salvar_cache(cache)
    except Exception:
        pass



# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 16 ▸ preparar_imagem + buscar_imagem
# Corrige: MessageMediaPhoto não aceita await direto
# usa client.download_media() corretamente
# ══════════════════════════════════════════════════════════════════════════════

async def preparar_imagem(fonte, eh_midia_telegram: bool) -> tuple:
    """
    Retorna (BytesIO | path_string | None, None).

    Telegram media → client.download_media(fonte, file=BytesIO())
    URL http      → baixa bytes para BytesIO
    Arquivo local → retorna path direto
    """
    if eh_midia_telegram:
        try:
            buf = io.BytesIO()
            # download_media é o método correto do Telethon para qualquer
            # tipo de mídia: MessageMediaPhoto, MessageMediaDocument, etc.
            await client.download_media(fonte, file=buf)
            buf.seek(0)
            buf.name = "imagem.jpg"
            log_img.debug(f"✅ Mídia Telegram baixada | {buf.getbuffer().nbytes} bytes")
            return buf, None
        except Exception as e:
            log_img.warning(f"⚠️ download_media: {e}")
            return None, None

    if isinstance(fonte, str):
        if fonte.startswith("http"):
            try:
                hdrs = {"User-Agent": random.choice(USER_AGENTS)}
                async with aiohttp.ClientSession(headers=hdrs) as s:
                    async with s.get(
                        fonte,
                        timeout=aiohttp.ClientTimeout(total=20),
                        allow_redirects=True,
                    ) as r:
                        if r.status == 200:
                            data = await r.read()
                            buf  = io.BytesIO(data)
                            buf.name = "produto.jpg"
                            log_img.debug(
                                f"✅ Imagem URL | {len(data)} bytes | {fonte[:60]}")
                            return buf, None
                        log_img.warning(f"⚠️ HTTP {r.status}: {fonte[:60]}")
            except Exception as e:
                log_img.warning(f"⚠️ Download URL: {e} | {fonte[:60]}")
            return None, None

        if os.path.exists(fonte):
            return fonte, None
        log_img.warning(f"⚠️ Arquivo não existe: {fonte}")
        return None, None

    log_img.warning(f"⚠️ Fonte inválida: {type(fonte)}")
    return None, None


async def buscar_imagem(url: str) -> Optional[str]:
    """
    Busca melhor URL de imagem do produto.
    og:image > JSON-LD > maior <img>.
    Remove parâmetros de resize para pegar resolução máxima.
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
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as r:
                    ct = r.headers.get("content-type", "")
                    if "image" in ct:
                        return str(r.url)

                    html = await r.text(errors="ignore")
                    soup = BeautifulSoup(html, "html.parser")

                    # og:image — remove resize para resolução máxima
                    for attr in [
                        {"property": "og:image"},
                        {"property": "og:image:secure_url"},
                        {"name": "twitter:image"},
                    ]:
                        tag = soup.find("meta", attrs=attr)
                        if tag and tag.get("content", "").startswith("http"):
                            img_url = tag["content"]
                            img_url = re.sub(
                                r'[?&](width|height|w|h|size|resize|'
                                r'fit|quality|q|maxwidth|maxheight)=[^&]+',
                                '', img_url).rstrip('?&')
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

                    # Maior <img>
                    melhor_src  = None
                    melhor_area = 0
                    for img_tag in soup.find_all("img", src=True):
                        src = img_tag.get("src", "")
                        if not src.startswith("http"):
                            continue
                        try:
                            w    = int(img_tag.get("width",  0))
                            h    = int(img_tag.get("height", 0))
                            area = w * h
                            if area > melhor_area:
                                melhor_area = area
                                melhor_src  = src
                        except (ValueError, TypeError):
                            if any(x in src.lower() for x in [
                                "product","produto","item","image",
                                "foto","zoom","large","xl","hd","original",
                            ]):
                                if not melhor_src:
                                    melhor_src = src
                    if melhor_src:
                        log_img.info(f"✅ <img> t={t}: {melhor_src[:70]}")
                        return melhor_src

        except asyncio.TimeoutError:
            log_img.warning(f"⏱ Timeout t={t}/3: {url[:60]}")
        except Exception as e:
            log_img.warning(f"⚠️ buscar_imagem t={t}/3: {e}")
        if t < 3:
            await asyncio.sleep(1.0)

    log_img.warning(f"❌ Sem imagem: {url[:60]}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 17 ▸ RATE-LIMIT ADAPTATIVO
# Horário comercial (8h–22h): 0.5s
# Madrugada (22h–8h): 1.2s
# Nunca bloqueia mais que o necessário.
# ══════════════════════════════════════════════════════════════════════════════

_RATE_LOCK     = asyncio.Lock()
_ULTIMO_ENV_TS = 0.0

def _intervalo_atual() -> float:
    hora = int(time.strftime("%H"))
    return 0.5 if 8 <= hora < 22 else 1.2

async def _rate_limit():
    global _ULTIMO_ENV_TS
    async with _RATE_LOCK:
        agora    = time.monotonic()
        intervalo = _intervalo_atual()
        espera   = intervalo - (agora - _ULTIMO_ENV_TS)
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
# MÓDULO 19 ▸ ENVIO LIMPO — SEM CABEÇALHO / SEM FORWARD
# Usa SOMENTE send_message e send_file.
# Nunca forward_messages — evita "Leo Indica / Ofertas Insanas" no topo.
# Valida tag Amazon antes de enviar.
# ══════════════════════════════════════════════════════════════════════════════

def _tem_midia(media) -> bool:
    return media is not None and not isinstance(media, MessageMediaWebPage)

def _eh_cupom_texto(texto: str) -> bool:
    return bool(_KW_CUPOM.search(texto))

def _validar_tag_amazon(msg: str) -> bool:
    """Garante que mensagens Amazon saíram com a tag correta."""
    if "amazon.com.br" not in msg and "amzn" not in msg:
        return True  # não é Amazon, não precisa validar
    return f"tag={_AMZ_TAG}" in msg

async def _enviar(msg: str, img_obj) -> object:
    """
    Envio limpo. Nunca forward. Sempre constrói do zero.
    Se img_obj for BytesIO ou path: envia como send_file + caption.
    Se texto > 1024 chars: envia imagem sem caption + texto separado.
    Se sem imagem: envia send_message com link_preview.
    """
    # Validação Amazon — nunca envia sem comissão
    if not _validar_tag_amazon(msg):
        log_amz.error("❌ BLOQUEIO: mensagem Amazon sem tag — abortando envio")
        raise ValueError("Amazon sem tag de afiliado")

    if img_obj:
        if len(msg) <= 1024:
            try:
                return await client.send_file(
                    GRUPO_DESTINO, img_obj,
                    caption=msg, parse_mode="md",
                    force_document=False)
            except Exception as e:
                log_tg.warning(f"⚠️ send_file+caption: {e}")
                # Tenta imagem sem caption + texto separado
                try:
                    await client.send_file(
                        GRUPO_DESTINO, img_obj,
                        force_document=False)
                    return await client.send_message(
                        GRUPO_DESTINO, msg,
                        parse_mode="md", link_preview=True)
                except Exception as e2:
                    log_tg.warning(f"⚠️ send_file sem caption: {e2}")
        else:
            # Texto longo: imagem primeiro, texto depois
            try:
                await client.send_file(
                    GRUPO_DESTINO, img_obj,
                    force_document=False)
                return await client.send_message(
                    GRUPO_DESTINO, msg,
                    parse_mode="md", link_preview=False)
            except Exception as e:
                log_tg.warning(f"⚠️ send_file longo: {e}")

    # Fallback: só texto
    return await client.send_message(
        GRUPO_DESTINO, msg,
        parse_mode="md", link_preview=True)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 20 ▸ BANCO CENTRAL — SQLite
#
# REGRA DE PERSISTÊNCIA:
#   links_cache   → NUNCA apagado automaticamente (economiza API)
#   dedupe_temp   → apagado a cada 24h (controle de duplicação)
#   saturacao     → apagado a cada 24h
#   scheduler     → apagado a cada 30 dias
#
# TTL_DEDUPE = 86400s (24h) — memória inteligente multi-grupos
# JANELA_RAPIDA = 600s (10 min) — bloqueio imediato de flood
# ══════════════════════════════════════════════════════════════════════════════

_DB_PATH       = "foguetao.db"
_db_conn: Optional[sqlite3.Connection] = None
_db_lock       = Lock()
TTL_DEDUPE     = 86400       # 24h — histórico semântico
JANELA_RAPIDA  = 600         # 10min — anti-flood imediato
TTL_SCHEDULER  = 30 * 86400  # 30 dias

def _init_db():
    global _db_conn
    _db_conn = sqlite3.connect(
        _DB_PATH,
        check_same_thread=False,
        timeout=10,
        isolation_level=None,
    )
    _db_conn.execute("PRAGMA journal_mode=WAL")
    _db_conn.execute("PRAGMA synchronous=NORMAL")
    _db_conn.execute("PRAGMA cache_size=-16000")   # 16MB cache
    _db_conn.execute("PRAGMA temp_store=MEMORY")
    _db_conn.executescript("""
        -- Links convertidos — NUNCA apagados automaticamente
        CREATE TABLE IF NOT EXISTS links_cache (
            url_orig  TEXT PRIMARY KEY,
            url_conv  TEXT NOT NULL,
            plat      TEXT NOT NULL,
            ts        REAL NOT NULL
        );

        -- Deduplicação temporária — apagada a cada 24h
        CREATE TABLE IF NOT EXISTS dedupe_temp (
            fp        TEXT PRIMARY KEY,
            plat      TEXT NOT NULL,
            cupons    TEXT,
            alma      TEXT,
            camp      TEXT,
            asin      TEXT,
            id_prod   TEXT,
            ts        REAL NOT NULL
        );

        -- Saturação de envios — apagada a cada 24h
        CREATE TABLE IF NOT EXISTS saturacao (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            plat  TEXT NOT NULL,
            sku   TEXT,
            ts    REAL NOT NULL
        );

        -- Scheduler de horários — apagado a cada 30 dias
        CREATE TABLE IF NOT EXISTS scheduler (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            plat  TEXT NOT NULL,
            hora  INTEGER NOT NULL,
            score REAL DEFAULT 1.0,
            ts    REAL NOT NULL
        );

        -- Índices
        CREATE INDEX IF NOT EXISTS idx_lc_plat  ON links_cache(plat);
        CREATE INDEX IF NOT EXISTS idx_dt_plat  ON dedupe_temp(plat, ts);
        CREATE INDEX IF NOT EXISTS idx_dt_asin  ON dedupe_temp(asin);
        CREATE INDEX IF NOT EXISTS idx_dt_id    ON dedupe_temp(id_prod);
        CREATE INDEX IF NOT EXISTS idx_sat      ON saturacao(plat, ts);
        CREATE INDEX IF NOT EXISTS idx_sch      ON scheduler(plat, hora);
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

# ── Links cache (permanente) ──────────────────────────────────────────────────

def db_get_link(url_orig: str) -> Optional[str]:
    with _db() as db:
        row = db.execute(
            "SELECT url_conv FROM links_cache WHERE url_orig=?",
            (url_orig,)
        ).fetchone()
    if row:
        log_lnk.debug(f"💾 Cache hit: {url_orig[:50]}")
        return row[0]
    return None

def db_set_link(url_orig: str, url_conv: str, plat: str):
    with _db() as db:
        db.execute(
            "INSERT OR REPLACE INTO links_cache "
            "(url_orig, url_conv, plat, ts) VALUES (?,?,?,?)",
            (url_orig, url_conv, plat, time.time()))

# ── Deduplicação (temporária 24h) ─────────────────────────────────────────────

def db_get_dedupe(fp: str) -> Optional[dict]:
    limite = time.time() - TTL_DEDUPE
    with _db() as db:
        row = db.execute(
            "SELECT plat,cupons,alma,camp,asin,id_prod,ts "
            "FROM dedupe_temp WHERE fp=? AND ts>=?",
            (fp, limite)
        ).fetchone()
    if row:
        return {
            "plat": row[0], "cupons": json.loads(row[1] or "[]"),
            "alma": row[2],  "camp":  row[3],
            "asin": row[4],  "id_prod": row[5], "ts": row[6],
        }
    return None

def db_set_dedupe(fp: str, plat: str, cupons: list, alma: str,
                   camp: str, asin: str = "", id_prod: str = ""):
    with _db() as db:
        db.execute(
            "INSERT OR REPLACE INTO dedupe_temp "
            "(fp,plat,cupons,alma,camp,asin,id_prod,ts) VALUES (?,?,?,?,?,?,?,?)",
            (fp, plat, json.dumps(cupons), alma, camp,
             asin, id_prod, time.time()))

def db_buscar_dedupe_por_asin(asin: str, plat: str) -> list:
    """Busca todas as entradas com o mesmo ASIN na janela de 24h."""
    if not asin:
        return []
    limite = time.time() - TTL_DEDUPE
    with _db() as db:
        rows = db.execute(
            "SELECT fp,cupons,alma,ts FROM dedupe_temp "
            "WHERE asin=? AND plat=? AND ts>=? ORDER BY ts DESC LIMIT 10",
            (asin, plat, limite)
        ).fetchall()
    return [{"fp": r[0], "cupons": json.loads(r[1] or "[]"),
             "alma": r[2], "ts": r[3]} for r in rows]

def db_buscar_dedupe_por_id(id_prod: str, plat: str) -> list:
    """Busca por ID de produto Magalu na janela de 24h."""
    if not id_prod:
        return []
    limite = time.time() - TTL_DEDUPE
    with _db() as db:
        rows = db.execute(
            "SELECT fp,cupons,alma,ts FROM dedupe_temp "
            "WHERE id_prod=? AND plat=? AND ts>=? ORDER BY ts DESC LIMIT 10",
            (id_prod, plat, limite)
        ).fetchall()
    return [{"fp": r[0], "cupons": json.loads(r[1] or "[]"),
             "alma": r[2], "ts": r[3]} for r in rows]

def db_buscar_dedupe_janela_rapida(plat: str) -> list:
    """Retorna entradas da janela rápida (10 min) para anti-flood."""
    limite = time.time() - JANELA_RAPIDA
    with _db() as db:
        rows = db.execute(
            "SELECT fp,cupons,alma,asin,id_prod,ts FROM dedupe_temp "
            "WHERE plat=? AND ts>=? ORDER BY ts DESC",
            (plat, limite)
        ).fetchall()
    return [{"fp": r[0], "cupons": json.loads(r[1] or "[]"),
             "alma": r[2], "asin": r[3], "id_prod": r[4], "ts": r[5]}
            for r in rows]

# ── Saturação ─────────────────────────────────────────────────────────────────

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

# ── Scheduler ─────────────────────────────────────────────────────────────────

def db_registrar_sch(plat: str, hora: int, score: float = 1.0):
    with _db() as db:
        db.execute(
            "INSERT INTO scheduler (plat,hora,score,ts) VALUES(?,?,?,?)",
            (plat, hora, score, time.time()))

def db_score_hora(plat: str, hora: int, dias: int = 7) -> float:
    limite = time.time() - dias * 86400
    with _db() as db:
        row = db.execute(
            "SELECT AVG(score) FROM scheduler "
            "WHERE plat=? AND hora=? AND ts>=?",
            (plat, hora, limite)).fetchone()
    return float(row[0] or 1.0)

# ── Limpeza automática (NUNCA toca links_cache) ───────────────────────────────

def db_limpar():
    """
    Apaga SOMENTE dados temporários.
    links_cache NUNCA é apagado.
    """
    agora  = time.time()
    lim_24h = agora - TTL_DEDUPE
    lim_30d = agora - TTL_SCHEDULER
    with _db() as db:
        db.execute("DELETE FROM dedupe_temp WHERE ts<?", (lim_24h,))
        db.execute("DELETE FROM saturacao   WHERE ts<?", (lim_24h,))
        db.execute("DELETE FROM scheduler   WHERE ts<?", (lim_30d,))
    log_sys.debug("🗑 Limpeza temp concluída (links preservados)")


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
# MÓDULO 24 ▸ BLOCO INTERNO _pipeline — substitui só os blocos marcados
# Cole esses trechos dentro de _pipeline no lugar dos blocos equivalentes
# ══════════════════════════════════════════════════════════════════════════════

    # ── Imagem ────────────────────────────────────────────────────────────
    media_orig = event.message.media
    tem_img    = _tem_midia(media_orig)
    img        = None
    eh_cup     = _eh_cupom_texto(tc)

    # 1. Imagem original da mensagem (máxima prioridade)
    if tem_img:
        try:
            img, _ = await preparar_imagem(media_orig, True)
            if img:
                log_img.debug("✅ Imagem da mensagem original")
            else:
                log_img.warning("⚠️ preparar_imagem retornou None")
        except Exception as e:
            log_img.warning(f"⚠️ Imagem original: {e}")
            img = None

    # 2. Sem imagem → busca ou fallback
    if img is None:
        if eh_cup:
            # Cupom sem imagem → fallback exclusivo por plataforma
            if plat == "shopee" and os.path.exists(_IMG_SHP):
                img = _IMG_SHP
                log_img.debug("🟣 Fallback Shopee")
            elif plat == "amazon" and os.path.exists(_IMG_AMZ):
                # Amazon: _IMG_AMZ SOMENTE para cupom Amazon
                img = _IMG_AMZ
                log_img.debug("🟠 Fallback Amazon cupom")
            elif plat == "magalu" and os.path.exists(_IMG_MGL):
                img = _IMG_MGL
                log_img.debug("🔵 Fallback Magalu")
        elif mapa:
            # Produto sem imagem → busca na página do produto
            try:
                img_url = await buscar_imagem(list(mapa.values())[0])
                if img_url:
                    img, _ = await preparar_imagem(img_url, False)
                    if img:
                        log_img.debug(f"✅ Imagem produto: {img_url[:60]}")
            except Exception as e:
                log_img.warning(f"⚠️ Busca imagem produto: {e}")
                img = None

    # ── Envio ─────────────────────────────────────────────────────────────
    await _rate_limit()

    async with _SEM_ENVIO:
        loop = asyncio.get_event_loop()
        mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        try:
            if is_edit:
                id_d = mp.get(str(msg_id))
                if not id_d:
                    return
                for t in range(1, 4):
                    try:
                        await client.edit_message(
                            GRUPO_DESTINO, id_d, msg_final)
                        log_tg.info(f"✏️ Editado | {id_d}")
                        break
                    except MessageNotModifiedError:
                        break
                    except FloodWaitError as e:
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        log_tg.error(f"❌ Edit t={t}: {e}")
                        if t < 3:
                            await asyncio.sleep(2 ** t)
                return

            sent = None
            for t in range(1, 4):
                try:
                    sent = await _enviar(msg_final, img)
                    break
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    log_tg.error(f"❌ Envio t={t}: {e}")
                    if t == 1:
                        img = None   # tenta sem imagem
                    elif t < 3:
                        await asyncio.sleep(2 ** t)

            if sent:
                mp[str(msg_id)] = sent.id
                await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
                await _marcar(msg_id)
                await scheduler_ok(plat)
                antisaturacao_ok(plat, sku)
                await _burst_add()

                # Se Magalu usou link longo → agenda edição quando encurtar
                if plat == "magalu" and mapa:
                    for url_orig, url_conv in mapa.items():
                        # link longo = tem os params partner_id completos
                        if "partner_id" in url_conv and "cutt.ly" not in url_conv:
                            await _agendar_edicao_magalu(url_conv, msg_id)

                log_sys.info(
                    f"🚀 [OK] @{uname} → {GRUPO_DESTINO} | "
                    f"{msg_id}→{sent.id} | {plat.upper()} sku={sku}")

        except Exception as e:
            log_sys.error(f"❌ CRÍTICO: {e}", exc_info=True)



# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 25 ▸ HEALTH CHECK
# Limpeza a cada 5 min — NUNCA apaga links_cache.
# ══════════════════════════════════════════════════════════════════════════════

async def _health_check():
    while True:
        await asyncio.sleep(300)   # a cada 5 min
        try:
            db_limpar()   # apaga só dedupe_temp + saturacao (24h) + scheduler (30d)

            with _db() as db:
                n_links = db.execute(
                    "SELECT COUNT(*) FROM links_cache").fetchone()[0]
                n_dedup = db.execute(
                    "SELECT COUNT(*) FROM dedupe_temp").fetchone()[0]
                n_sat   = db.execute(
                    "SELECT COUNT(*) FROM saturacao").fetchone()[0]

            log_hc.info(
                f"💚 links_cache={n_links}(permanente) | "
                f"dedupe={n_dedup} | sat={n_sat} | "
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

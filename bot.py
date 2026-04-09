"""
╔══════════════════════════════════════════════════════════════════════════╗
║            FOGUETÃO v63.0 — AMAZON + SHOPEE ELITE                       ║
║  Motor 100% Async | Desencurtador | Debug Total | Auto-Restart          ║
╚══════════════════════════════════════════════════════════════════════════╝

MODELOS DE SAÍDA:

  CUPOM SHOPEE (sem emojis originais):
    🚨 CUPOM SHOPEE 🚨

    🎟 R$10 OFF sem mínimo: CODIGO

    ✅ Resgate aqui:
    https://link-resgate

    🛒 Carrinho:
    https://link-carrinho

  CUPOM AMAZON (sem emojis originais):
    🚨 Cupons Amazon APP

    🎟 R$150 OFF em R$1499: CODIGO

    ✅ Resgate aqui:
    https://link

    #anúncio

  OFERTA COMUM (sem emojis originais):
    🔥 Nome do Produto

    ✅ R$ 1078
    🎟 Resgate cupom R$ 90 OFF aqui:
    https://link-resgate
    https://link-produto
    anúncio

  COM EMOJIS JÁ NO ORIGINAL → mantém tudo, apenas completa onde falta
"""

import os
import re
import time
import json
import asyncio
import aiohttp
import hashlib
import random
import unicodedata
import logging
import concurrent.futures
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage
from telethon.errors import (
    MessageNotModifiedError, FloodWaitError,
    AuthKeyUnregisteredError, SessionPasswordNeededError,
)
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from difflib import SequenceMatcher
from threading import Lock


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 1 ▸ LOGS COLORIDOS — UM POR PLATAFORMA/MÓDULO
# ══════════════════════════════════════════════════════════════════════════

def criar_logger(nome: str, cor: str) -> logging.Logger:
    logger = logging.getLogger(nome)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            f'\033[{cor}m[%(name)-10s]\033[0m %(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        ))
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)
    return logger

log_amz   = criar_logger('AMAZON',   '1;33')
log_shp   = criar_logger('SHOPEE',   '1;38;5;208')
log_dedup = criar_logger('DEDUP',    '1;35')
log_img   = criar_logger('IMAGEM',   '1;36')
log_tg    = criar_logger('TELEGRAM', '1;32')
log_fil   = criar_logger('FILTRO',   '1;31')
log_lnk   = criar_logger('LINKS',    '1;34')
log_sys   = criar_logger('SISTEMA',  '1;37')


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 2 ▸ CONFIGURAÇÕES GERAIS
# ══════════════════════════════════════════════════════════════════════════

API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION", "")

GRUPOS_ORIGEM = ['promotom', 'fumotom', 'botofera', 'fadadoscupons']
GRUPO_DESTINO = '@ofertap'

AMAZON_TAG    = os.environ.get("AMAZON_TAG",    "leo21073-20")
SHOPEE_APP_ID = os.environ.get("SHOPEE_APP_ID", "18348480261")
SHOPEE_SECRET = os.environ.get("SHOPEE_SECRET", "SGC7FQQQ4R5QCFULPXIBCANATLP272B3")

IMG_AMAZON = "cupom-amazon.jpg"
IMG_SHOPEE = "IMG_20260404_180150.jpg"

ARQUIVO_CACHE      = "cache_dedup.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

envio_lock = asyncio.Semaphore(5)
_executor  = concurrent.futures.ThreadPoolExecutor(max_workers=4)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 3 ▸ FILTRO BLINDADO
# ══════════════════════════════════════════════════════════════════════════

FILTRO_PALAVRAS = [
    "Monitor Samsung", "Fonte Mancer", "Placa de video", "Monitor LG",
    "PC home Essential", "Suporte articulado", "Gabinetes em oferta",
    "VHAGAR", "Superframe", "AM5", "AM4", "GTX", "DDR5", "DDR4",
    "Dram", "Monitor Safe", "Monitor Redragon", "CL18", "CL16", "CL32",
    "MT/s", "MHz", "RX 580", "Ryzen", "Placa Mãe", "Gabinete Gamer",
    "Water Cooler", "Air Cooler",
]

def texto_bloqueado(texto: str) -> bool:
    tl = texto.lower()
    for p in FILTRO_PALAVRAS:
        if p.lower() in tl:
            log_fil.debug(f"🚫 Palavra bloqueada: '{p}'")
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 4 ▸ DEDUPLICAÇÃO — 3 CAMADAS (TEXTO + CUPOM, NUNCA PELO LINK)
#
#  • Mesmo texto + mesmo cupom + mesmo preço  → BLOQUEIA
#  • Texto mudou OU cupom mudou               → PASSA (envia de novo)
# ══════════════════════════════════════════════════════════════════════════

LOCK_DEDUP         = Lock()
TTL_SEGUNDOS       = 120 * 60
JANELA_ANTISPAM    = 900
SIM_TEXTO          = 0.85
SIM_TEXTO_ALTA     = 0.92

PALAVRAS_RUIDO = {
    "promo", "promocao", "promoção", "oferta", "desconto", "cupom",
    "corre", "aproveita", "urgente", "gratis", "grátis", "frete",
    "hoje", "agora", "relampago", "relâmpago",
}


def remover_acentos(t: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', t)
        if unicodedata.category(c) != 'Mn'
    )


def normalizar_texto(texto: str) -> str:
    if not texto:
        return ""
    texto = remover_acentos(texto.lower())
    texto = re.sub(r"http\S+|www\S+", " ", texto)
    texto = re.sub(r"[^\w\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return " ".join(sorted(p for p in texto.split() if p not in PALAVRAS_RUIDO))


def _ler_cache() -> dict:
    if not os.path.exists(ARQUIVO_CACHE):
        return {}
    try:
        with open(ARQUIVO_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_dedup.error(f"Erro ao ler cache: {e}")
        return {}


def _gravar_cache(cache: dict):
    try:
        with open(ARQUIVO_CACHE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_dedup.error(f"Erro ao gravar cache: {e}")


def deve_enviar_oferta(plataforma: str, produto_id: str, preco: str,
                        cupom: str = "", texto: str = "") -> bool:
    with LOCK_DEDUP:
        cache = _ler_cache()
        agora = time.time()
        cache = {k: v for k, v in cache.items()
                 if agora - v["timestamp"] < TTL_SEGUNDOS}

        tnorm      = normalizar_texto(texto)
        cupom_norm = cupom.strip().upper()
        h = hashlib.sha256(
            f"{plataforma}|{produto_id}|{preco}|{cupom_norm}|{tnorm}".encode()
        ).hexdigest()

        # C1 — hash exato
        if h in cache:
            log_dedup.info(f"🔁 [C1] Hash exato | prod={produto_id} cupom={cupom_norm}")
            return False

        for entrada in cache.values():
            if (agora - entrada["timestamp"]) >= JANELA_ANTISPAM:
                continue

            sim         = SequenceMatcher(None, tnorm, entrada.get("texto", "")).ratio()
            cupom_igual = entrada.get("cupom", "") == cupom_norm.lower()
            prod_igual  = str(entrada.get("produto_id")) == str(produto_id) and produto_id != "0"
            preco_igual = str(entrada.get("preco")) == str(preco) and preco != "0"

            # C2 — mesmo produto+preço+cupom, texto similar
            if prod_igual and preco_igual and cupom_igual and sim >= SIM_TEXTO:
                log_dedup.info(
                    f"🔁 [C2] Similar | prod={produto_id} "
                    f"cupom={cupom_norm} sim={sim:.2f}"
                )
                return False

            # C3 — cupom igual + texto quase idêntico (sem produto_id)
            if cupom_igual and sim >= SIM_TEXTO_ALTA:
                log_dedup.info(f"🔁 [C3] Cupom+texto idêntico | sim={sim:.2f}")
                return False

        cache[h] = {
            "plataforma": plataforma, "produto_id": str(produto_id),
            "preco": str(preco),      "cupom": cupom_norm.lower(),
            "texto": tnorm,           "timestamp": agora,
        }
        _gravar_cache(cache)
        log_dedup.debug(f"✅ Nova oferta | plat={plataforma} prod={produto_id}")
        return True


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 5 ▸ DESENCURTADOR — ATÉ O OSSO (12 níveis)
# ══════════════════════════════════════════════════════════════════════════

async def desencurtar(url: str, sessao: aiohttp.ClientSession, depth: int = 0) -> str:
    if depth > 12:
        log_lnk.warning(f"⚠️ Prof. máx | {url[:60]}")
        return url
    hdrs = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    }
    try:
        try:
            async with sessao.head(url, headers=hdrs, allow_redirects=True,
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                final = str(r.url)
                if final != url:
                    log_lnk.debug(f"  [HEAD d={depth}] → {final[:60]}")
                    return await desencurtar(final, sessao, depth + 1)
                return final
        except Exception:
            pass

        async with sessao.get(url, headers=hdrs, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")
            soup = BeautifulSoup(html, "html.parser")

            tag_ref = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
            if tag_ref and tag_ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)", tag_ref["content"], re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    log_lnk.debug(f"  [META d={depth}] → {novo[:60]}")
                    return await desencurtar(novo, sessao, depth + 1)

            m_js = re.search(
                r'window\.location(?:\.href)?\s*=\s*["\']([^"\']+)["\']', html)
            if m_js:
                log_lnk.debug(f"  [JS d={depth}] → {m_js.group(1)[:60]}")
                return await desencurtar(m_js.group(1), sessao, depth + 1)

            if pos != url:
                log_lnk.debug(f"  [GET d={depth}] → {pos[:60]}")
                return await desencurtar(pos, sessao, depth + 1)
            return pos

    except asyncio.TimeoutError:
        log_lnk.warning(f"⏱ Timeout: {url[:60]}")
        return url
    except Exception as e:
        log_lnk.error(f"❌ Erro: {url[:60]}: {e}")
        return url


def classificar_plataforma(url: str) -> str | None:
    try:
        d = urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return None
    if any(x in d for x in ["amazon.com", "amzn.to", "amzn.com", "a.co"]):
        return "amazon"
    if any(x in d for x in ["shopee.com", "s.shopee", "shope.ee"]):
        return "shopee"
    return None


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 6 ▸ MOTOR AMAZON — TAG + URL LIMPA E CLICÁVEL
# ══════════════════════════════════════════════════════════════════════════

_PARAMS_LIXO_AMZ = {
    "ascsubtag", "btn_ref", "ref_", "ref", "smid", "sprefix", "sr",
    "spla", "dchild", "linkcode", "linkid", "camp", "creative",
    "pf_rd_p", "pf_rd_r", "pd_rd_wg", "pd_rd_w", "content-id",
    "pd_rd_r", "pd_rd_i", "ie", "qid", "_encoding",
}


def _url_amazon_limpa(url_exp: str) -> str:
    """Remove parâmetros lixo, corta /ref=... do path, injeta tag."""
    p    = urlparse(url_exp)
    path = re.sub(r'(/dp/[A-Z0-9]{10}).*',         r'\1', p.path)
    path = re.sub(r'(/gp/product/[A-Z0-9]{10}).*', r'\1', path)
    params = {k: v for k, v in parse_qs(p.query, keep_blank_values=False).items()
              if k.lower() not in _PARAMS_LIXO_AMZ}
    params["tag"] = [AMAZON_TAG]
    return urlunparse(p._replace(path=path, query=urlencode(params, doseq=True)))


async def motor_amazon(url: str, sessao: aiohttp.ClientSession) -> str:
    log_amz.debug(f"🔗 Iniciando | {url[:70]}")
    exp = await desencurtar(url, sessao)
    log_amz.debug(f"📦 Expandida | {exp[:70]}")
    if "amazon" not in exp.lower():
        log_amz.warning(f"⚠️ Não é Amazon: {exp[:60]}")
        return url
    final = _url_amazon_limpa(exp)
    log_amz.info(f"✅ Convertida | {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 7 ▸ MOTOR SHOPEE — API DE AFILIADO
# ══════════════════════════════════════════════════════════════════════════

async def motor_shopee(url: str, sessao: aiohttp.ClientSession) -> str:
    log_shp.debug(f"🔗 Iniciando | {url[:70]}")
    exp = await desencurtar(url, sessao)
    log_shp.debug(f"📦 Expandida | {exp[:70]}")
    ts      = str(int(time.time()))
    payload = json.dumps(
        {"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{exp}" }}) '
                  f'{{ shortLink }} }}'},
        separators=(",", ":")
    )
    sig = hashlib.sha256(
        f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()
    ).hexdigest()
    try:
        async with sessao.post(
            "https://open-api.affiliate.shopee.com.br/graphql",
            data=payload,
            headers={
                "Authorization": f"SHA256 Credential={SHOPEE_APP_ID},"
                                 f"Timestamp={ts},Signature={sig}",
                "Content-Type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=12),
        ) as r:
            res  = await r.json()
            link = res["data"]["generateShortLink"]["shortLink"]
            log_shp.info(f"✅ Convertida | {link}")
            return link
    except Exception as e:
        log_shp.error(f"❌ API Shopee: {e} | url={exp[:60]}")
        return url


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ CONVERSOR GERAL + MASSIVO PARALELO (50 LINKS)
# ══════════════════════════════════════════════════════════════════════════

async def converter_link(url: str, sessao: aiohttp.ClientSession) -> tuple:
    ul = url.lower()
    log_lnk.debug(f"🔍 {url[:70]}")

    if any(d in ul for d in ["amazon.com", "amzn.to", "amzn.com", "a.co"]):
        novo = await motor_amazon(url, sessao)
        return (novo, "amazon") if classificar_plataforma(novo) == "amazon" else (None, None)

    if any(d in ul for d in ["shopee.com", "s.shopee", "shope.ee"]):
        return await motor_shopee(url, sessao), "shopee"

    # Encurtador genérico
    log_lnk.debug(f"🔄 Expandindo genérico: {url[:60]}")
    exp  = await desencurtar(url, sessao)
    plat = classificar_plataforma(exp)
    if plat == "amazon":
        novo = await motor_amazon(exp, sessao)
        return (novo, "amazon") if classificar_plataforma(novo) == "amazon" else (None, None)
    if plat == "shopee":
        return await motor_shopee(exp, sessao), "shopee"

    log_lnk.info(f"🗑 Descartado: {exp[:60]}")
    return None, None


async def converter_todos_links(links: list) -> tuple:
    log_lnk.info(f"🚀 Conversão massiva: {len(links)} link(s)")
    conn = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False)
    async with aiohttp.ClientSession(
        connector=conn,
        timeout=aiohttp.ClientTimeout(total=30, connect=10),
        headers={"User-Agent": random.choice(USER_AGENTS)},
    ) as sessao:
        resultados = await asyncio.gather(
            *[converter_link(l, sessao) for l in links[:50]],
            return_exceptions=True,
        )
    mapa, plats = {}, []
    for i, res in enumerate(resultados):
        if isinstance(res, Exception):
            log_lnk.error(f"❌ link[{i}] {links[i][:50]}: {res}")
            continue
        novo, plat = res
        if novo and plat:
            mapa[links[i]] = novo
            plats.append(plat)
            log_lnk.debug(f"  [{plat.upper()}] {links[i][:40]} → {novo[:40]}")
    plat_p = max(set(plats), key=plats.count) if plats else "amazon"
    log_lnk.info(f"✅ {len(mapa)}/{len(links)} convertidos | plat={plat_p}")
    return mapa, plat_p


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 9 ▸ HELPERS DE DETECÇÃO
# ══════════════════════════════════════════════════════════════════════════

_RE_EMOJI = re.compile(
    r"[\U0001F300-\U0001FAFF"
    r"\U00002600-\U000027BF"
    r"\U0001F900-\U0001F9FF"
    r"\u2B50\u2B55\u231A\u231B"
    r"\u25A0-\u25FF]",
    flags=re.UNICODE,
)
_RE_CUPOM_KW = re.compile(
    r'\b(?:cupom|cupon|off|resgate|codigo|coupon|desconto)\b', re.I)
_RE_PRECO = re.compile(r'R\$\s?[\d.,]+')


def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI.search(s))

def _contar_emojis(s: str) -> int:
    return len(_RE_EMOJI.findall(s))

def _eh_cupom(texto: str) -> bool:
    return bool(_RE_CUPOM_KW.search(texto))

def _eh_cupom_shopee(texto: str, plat: str) -> bool:
    return _eh_cupom(texto) and plat == "shopee"

def _tem_midia_real(media) -> bool:
    return media is not None and not isinstance(media, MessageMediaWebPage)


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 10 ▸ FORMATAÇÃO — CORAÇÃO DO BOT
#
#  ESTRATÉGIA:
#    • Se texto original veio com >= 2 emojis → modo "complementar"
#      (mantém tudo, só adiciona emoji onde claramente falta)
#    • Se texto veio sem emojis (< 2) → modo "layout completo"
#      (reconstrói com os modelos definidos acima)
# ══════════════════════════════════════════════════════════════════════════

def _enfeitar_linha(linha: str) -> str:
    """Adiciona emoji numa linha sem emoji, conforme o conteúdo."""
    l = linha.strip()
    if not l or _tem_emoji(l) or re.match(r'^https?://', l):
        return linha
    if _RE_PRECO.search(l):
        return "🔥 " + l
    if _RE_CUPOM_KW.search(l):
        return "🎟 " + l
    return linha


def _layout_cupom_shopee(linhas_texto: list, links: list) -> str:
    """
    Monta:
      🚨 CUPOM SHOPEE 🚨
      (vazio)
      🎟 <linhas de desconto / código>
      (vazio)
      ✅ Resgate aqui:
      <link 1>
      (vazio)
      🛒 Carrinho:
      <link 2>
      <demais linhas: hashtags, anúncio, etc.>
    """
    links_shp   = [l for l in links if classificar_plataforma(l) == "shopee"]
    nao_links   = [l for l in linhas_texto if not re.match(r'^https?://', l)]

    linhas_desc  = [l for l in nao_links
                    if _RE_PRECO.search(l) or _RE_CUPOM_KW.search(l)]
    linhas_extra = [l for l in nao_links if l not in linhas_desc]

    out = ["🚨 CUPOM SHOPEE 🚨", ""]

    for ld in linhas_desc:
        out.append(f"🎟 {ld}" if not _tem_emoji(ld) else ld)

    if links_shp:
        out.extend(["", "✅ Resgate aqui:", links_shp[0]])
    if len(links_shp) >= 2:
        out.extend(["", "🛒 Carrinho:", links_shp[1]])
    for lk in links_shp[2:]:
        out.append(lk)

    for le in linhas_extra:
        out.append(le)

    return "\n".join(out)


def _layout_cupom_amazon(linhas_texto: list, links: list) -> str:
    """
    Monta:
      🚨 Cupons Amazon APP
      (vazio)
      🎟 <linhas de desconto / código>
      (vazio)
      ✅ Resgate aqui:
      <links>
      <demais linhas>
    """
    links_amz   = [l for l in links if classificar_plataforma(l) == "amazon"]
    nao_links   = [l for l in linhas_texto if not re.match(r'^https?://', l)]

    linhas_desc  = [l for l in nao_links
                    if _RE_PRECO.search(l) or _RE_CUPOM_KW.search(l)]
    linhas_extra = [l for l in nao_links if l not in linhas_desc]

    out = ["🚨 Cupons Amazon APP", ""]

    for ld in linhas_desc:
        out.append(f"🎟 {ld}")

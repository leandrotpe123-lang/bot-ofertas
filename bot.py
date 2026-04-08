"""
╔══════════════════════════════════════════════════════════════════════════╗
║            FOGUETÃO v62.0 — AMAZON + SHOPEE ELITE                       ║
║  Motor: 100% Assíncrono | Desencurtador Profissional | Debug Completo   ║
╚══════════════════════════════════════════════════════════════════════════╝

REGRAS DE FORMATAÇÃO:
  - Texto original copiado fielmente
  - Emojis originais mantidos; se não vier emoji, adiciona conforme contexto
  - 🛒 Carrinho: adicionado nos cupons Shopee (e só neles)
  - Links Amazon enviados como URL clicável limpa (sem parâmetros lixo)
  - Zero crases em cupons
  - Links não-Amazon/Shopee removidos silenciosamente do texto
  - Se mudar o texto ou o cupom → pode mandar de novo
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
from telethon.errors import MessageNotModifiedError, FloodWaitError
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from difflib import SequenceMatcher
from threading import Lock


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 1 ▸ LOGS PROFISSIONAIS — DEBUG EM TODAS AS PLATAFORMAS
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

log_amz   = criar_logger('AMAZON',   '1;33')        # Amarelo
log_shp   = criar_logger('SHOPEE',   '1;38;5;208')  # Laranja
log_dedup = criar_logger('DEDUP',    '1;35')        # Roxo
log_img   = criar_logger('IMAGEM',   '1;36')        # Ciano
log_tg    = criar_logger('TELEGRAM', '1;32')        # Verde
log_fil   = criar_logger('FILTRO',   '1;31')        # Vermelho
log_lnk   = criar_logger('LINKS',    '1;34')        # Azul
log_sys   = criar_logger('SISTEMA',  '1;37')        # Branco


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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
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
# MÓDULO 4 ▸ DEDUPLICAÇÃO PROFISSIONAL — 3 CAMADAS
#
#  REGRA PRINCIPAL:
#    • Mesmo texto + mesmo cupom + mesmo preço  → BLOQUEIA
#    • Texto mudou OU cupom mudou               → DEIXA PASSAR
#    • A checagem é pelo TEXTO normalizado, não pelo link
# ══════════════════════════════════════════════════════════════════════════

LOCK_DEDUP               = Lock()
TTL_SEGUNDOS             = 120 * 60   # memória de 120 min
JANELA_ANTISPAM_SEGUNDOS = 900        # janela de 15 min para camadas 2 e 3
SIMILARIDADE_TEXTO       = 0.85       # limiar para "texto igual"

PALAVRAS_RUIDO = {
    "promo", "promocao", "promoção", "oferta", "desconto", "cupom",
    "corre", "aproveita", "urgente", "gratis", "grátis", "frete",
    "hoje", "agora", "relampago", "relâmpago",
}


def remover_acentos(texto: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )


def normalizar_texto(texto: str) -> str:
    """
    Normaliza para fingerprint:
    - Remove acentos, links, símbolos e palavras de ruído
    - Ordena tokens para que textos reordenados ainda batam
    """
    if not texto:
        return ""
    texto = remover_acentos(texto.lower())
    texto = re.sub(r"http\S+|www\S+", " ", texto)
    texto = re.sub(r"[^\w\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    tokens = sorted(p for p in texto.split() if p not in PALAVRAS_RUIDO)
    return " ".join(tokens)


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
    """
    CAMADA 1 — Hash exato (plataforma + produto + preço + cupom + texto norm).
               Bloqueia repetição 100% idêntica.

    CAMADA 2 — Mesmo produto + preço + cupom E texto similar >= 85%
               na janela de 15 min.
               Bloqueia mesma oferta com pequenas variações de redação.

    CAMADA 3 — Texto muito similar >= 92% na janela de 15 min,
               mesmo sem produto_id. Para cupons que chegam sem link.

    SE o texto OU o cupom mudou de verdade → a oferta PASSA.
    """
    with LOCK_DEDUP:
        cache = _ler_cache()
        agora = time.time()

        # Expira entradas velhas
        cache = {k: v for k, v in cache.items()
                 if agora - v["timestamp"] < TTL_SEGUNDOS}

        texto_norm = normalizar_texto(texto)
        cupom_norm = cupom.strip().upper()
        chave = f"{plataforma}|{produto_id}|{preco}|{cupom_norm}|{texto_norm}"
        h = hashlib.sha256(chave.encode("utf-8")).hexdigest()

        # CAMADA 1
        if h in cache:
            log_dedup.info(f"🔁 [C1] Hash exato | prod={produto_id} cupom={cupom_norm}")
            return False

        for entrada in cache.values():
            na_janela = (agora - entrada["timestamp"]) < JANELA_ANTISPAM_SEGUNDOS
            if not na_janela:
                continue

            sim = SequenceMatcher(None, texto_norm, entrada.get("texto", "")).ratio()
            cupom_igual  = entrada.get("cupom", "") == cupom_norm.lower()
            prod_igual   = str(entrada.get("produto_id")) == str(produto_id) and produto_id != "0"
            preco_igual  = str(entrada.get("preco")) == str(preco) and preco != "0"

            # CAMADA 2
            if prod_igual and preco_igual and cupom_igual and sim >= SIMILARIDADE_TEXTO:
                log_dedup.info(
                    f"🔁 [C2] Oferta similar | prod={produto_id} "
                    f"cupom={cupom_norm} sim={sim:.2f}"
                )
                return False

            # CAMADA 3 — só bloqueia se o cupom for igual E texto quase idêntico
            if cupom_igual and sim >= 0.92:
                log_dedup.info(
                    f"🔁 [C3] Cupom + texto idêntico | cupom={cupom_norm} sim={sim:.2f}"
                )
                return False

        # Registra nova entrada
        cache[h] = {
            "plataforma": plataforma,
            "produto_id": str(produto_id),
            "preco":      str(preco),
            "cupom":      cupom_norm.lower(),
            "texto":      texto_norm,
            "timestamp":  agora,
        }
        _gravar_cache(cache)
        log_dedup.debug(f"✅ Nova oferta registrada | plat={plataforma} prod={produto_id}")
        return True


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 5 ▸ DESENCURTADOR PROFISSIONAL — ATÉ O OSSO
# ══════════════════════════════════════════════════════════════════════════

async def desencurtar(url: str, sessao: aiohttp.ClientSession, depth: int = 0) -> str:
    """
    Segue redirects recursivamente até 12 níveis.
    HEAD → GET + meta-refresh → JS window.location
    """
    if depth > 12:
        log_lnk.warning(f"⚠️ Profundidade max ({depth}) | {url[:60]}")
        return url

    hdrs = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    }

    try:
        # Tentativa 1: HEAD
        try:
            async with sessao.head(
                url, headers=hdrs, allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                final = str(r.url)
                if final != url:
                    log_lnk.debug(f"  [HEAD d={depth}] → {final[:60]}")
                    return await desencurtar(final, sessao, depth + 1)
                return final
        except Exception:
            pass

        # Tentativa 2: GET completo
        async with sessao.get(
            url, headers=hdrs, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            pos_redir = str(r.url)
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
                r'window\.location(?:\.href)?\s*=\s*["\']([^"\']+)["\']', html
            )
            if m_js:
                novo = m_js.group(1)
                log_lnk.debug(f"  [JS d={depth}] → {novo[:60]}")
                return await desencurtar(novo, sessao, depth + 1)

            if pos_redir != url:
                log_lnk.debug(f"  [GET d={depth}] → {pos_redir[:60]}")
                return await desencurtar(pos_redir, sessao, depth + 1)

            return pos_redir

    except asyncio.TimeoutError:
        log_lnk.warning(f"⏱ Timeout: {url[:60]}")
        return url
    except Exception as e:
        log_lnk.error(f"❌ Erro desencurtando {url[:60]}: {e}")
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
# MÓDULO 6 ▸ MOTOR AMAZON — DESENCURTA + TAG + URL LIMPA E CLICÁVEL
# ══════════════════════════════════════════════════════════════════════════

# Parâmetros de rastreamento de terceiros — FORA
_PARAMS_LIXO = {
    "ascsubtag", "btn_ref", "ref_", "ref", "smid", "sprefix", "sr",
    "spla", "dchild", "linkcode", "linkid", "camp", "creative",
    "pf_rd_p", "pf_rd_r", "pd_rd_wg", "pd_rd_w", "content-id",
    "pd_rd_r", "pd_rd_i", "ie", "qid", "_encoding",
}


def _montar_url_amazon_limpa(url_exp: str) -> str:
    """
    Monta URL Amazon limpa:
    - Mantém apenas: dp (no path), th, psc, tag, node, k, keywords
    - Remove todos os parâmetros de rastreamento
    - Garante que a tag do afiliado está presente
    - Resultado: URL curta e CLICÁVEL no Telegram
    """
    p = urlparse(url_exp)

    # Reconstrói path limpando sufixos desnecessários
    # ex: /dp/B09B8MQSGS/ref=... → /dp/B09B8MQSGS
    path_limpo = re.sub(r'(/dp/[A-Z0-9]{10}).*', r'\1', p.path)
    if path_limpo == p.path:
        # Tenta /gp/product/ASIN
        path_limpo = re.sub(r'(/gp/product/[A-Z0-9]{10}).*', r'\1', p.path)

    params_orig = parse_qs(p.query, keep_blank_values=False)
    params_limpos = {}
    for k, v in params_orig.items():
        if k.lower() not in _PARAMS_LIXO:
            params_limpos[k] = v

    # Injeta nossa tag (sobrescreve qualquer tag anterior)
    params_limpos["tag"] = [AMAZON_TAG]

    nova_query = urlencode(params_limpos, doseq=True)
    return urlunparse(p._replace(path=path_limpo, query=nova_query))


async def motor_amazon(url_original: str, sessao: aiohttp.ClientSession) -> str:
    log_amz.debug(f"🔗 Iniciando | {url_original[:70]}")

    url_exp = await desencurtar(url_original, sessao)
    log_amz.debug(f"📦 Expandida | {url_exp[:70]}")

    if "amazon" not in url_exp.lower():
        log_amz.warning(f"⚠️ Não é Amazon após expansão: {url_exp[:60]}")
        return url_original

    url_final = _montar_url_amazon_limpa(url_exp)
    log_amz.info(f"✅ Convertida | {url_final}")
    return url_final


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 7 ▸ MOTOR SHOPEE — DESENCURTA + API AFILIADO
# ══════════════════════════════════════════════════════════════════════════

async def motor_shopee(url_original: str, sessao: aiohttp.ClientSession) -> str:
    log_shp.debug(f"🔗 Iniciando | {url_original[:70]}")

    url_exp = await desencurtar(url_original, sessao)
    log_shp.debug(f"📦 Expandida | {url_exp[:70]}")

    ts      = str(int(time.time()))
    payload = json.dumps(
        {"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{url_exp}" }}) {{ shortLink }} }}'},
        separators=(",", ":")
    )
    sig = hashlib.sha256(
        f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()
    ).hexdigest()
    hdrs_api = {
        "Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}",
        "Content-Type": "application/json",
    }

    try:
        async with sessao.post(
            "https://open-api.affiliate.shopee.com.br/graphql",
            data=payload, headers=hdrs_api,
            timeout=aiohttp.ClientTimeout(total=12),
        ) as r:
            res  = await r.json()
            link = res["data"]["generateShortLink"]["shortLink"]
            log_shp.info(f"✅ Convertida | {link}")
            return link
    except Exception as e:
        log_shp.error(f"❌ Falha API Shopee: {e} | url={url_exp[:60]}")
        return url_original


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ CONVERSOR GERAL + CONVERSÃO MASSIVA PARALELA (ATÉ 50 LINKS)
# ══════════════════════════════════════════════════════════════════════════

async def converter_link(url: str, sessao: aiohttp.ClientSession) -> tuple:
    """
    Converte 1 link. Amazon → tag afiliado. Shopee → API afiliado.
    Qualquer outro → descartado (None, None).
    Encurtadores genéricos são expandidos primeiro.
    """
    url_lower = url.lower()
    log_lnk.debug(f"🔍 Analisando: {url[:70]}")

    # Detecção direta Amazon
    if any(d in url_lower for d in ["amazon.com", "amzn.to", "amzn.com", "a.co"]):
        novo = await motor_amazon(url, sessao)
        if classificar_plataforma(novo) == "amazon":
            return novo, "amazon"
        log_lnk.info(f"🗑 Amazon não confirmada pós-conversão: {novo[:60]}")
        return None, None

    # Detecção direta Shopee
    if any(d in url_lower for d in ["shopee.com", "s.shopee", "shope.ee"]):
        novo = await motor_shopee(url, sessao)
        return novo, "shopee"

    # Encurtador genérico — expande e reclassifica
    log_lnk.debug(f"🔄 Expandindo genérico: {url[:60]}")
    expandida = await desencurtar(url, sessao)
    plat      = classificar_plataforma(expandida)

    if plat == "amazon":
        log_lnk.debug(f"🎯 Amazon identificada após expansão")
        novo = await motor_amazon(expandida, sessao)
        if classificar_plataforma(novo) == "amazon":
            return novo, "amazon"

    if plat == "shopee":
        log_lnk.debug(f"🎯 Shopee identificada após expansão")
        novo = await motor_shopee(expandida, sessao)
        return novo, "shopee"

    log_lnk.info(f"🗑 Descartado (não é Amazon nem Shopee): {expandida[:60]}")
    return None, None


async def converter_todos_links(links: list) -> tuple:
    """
    Converte até 50 links em paralelo.
    Todos os 50 são processados, Amazon e Shopee misturados.
    Retorna (mapa_links, plataforma_principal).
    """
    log_lnk.info(f"🚀 Conversão massiva: {len(links)} link(s)")

    conector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False)
    timeout  = aiohttp.ClientTimeout(total=30, connect=10)

    async with aiohttp.ClientSession(
        connector=conector, timeout=timeout,
        headers={"User-Agent": random.choice(USER_AGENTS)},
    ) as sessao:
        tarefas    = [converter_link(l, sessao) for l in links[:50]]
        resultados = await asyncio.gather(*tarefas, return_exceptions=True)

    mapa_links  = {}
    plataformas = []

    for i, res in enumerate(resultados):
        if isinstance(res, Exception):
            log_lnk.error(f"❌ Exceção link[{i}] {links[i][:50]}: {res}")
            continue
        novo, plat = res
        if novo and plat:
            mapa_links[links[i]] = novo
            plataformas.append(plat)
            log_lnk.debug(f"  [{plat.upper()}] {links[i][:40]} → {novo[:40]}")

    plat_principal = (
        max(set(plataformas), key=plataformas.count) if plataformas else "amazon"
    )
    log_lnk.info(
        f"✅ Finalizado: {len(mapa_links)}/{len(links)} convertidos | "
        f"plataforma={plat_principal}"
    )
    return mapa_links, plat_principal


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 9 ▸ FORMATAÇÃO DO TEXTO
#
#  REGRAS:
#  1. Copia o texto original fielmente
#  2. Troca links antigos pelos novos (limpos, clicáveis)
#  3. Remove links que não são Amazon/Shopee
#  4. Mantém emojis que já vieram; adiciona emojis onde não tem
#  5. Cupom Shopee → insere "🛒 Carrinho:" antes do link
#  6. Zero crases
# ══════════════════════════════════════════════════════════════════════════

# Detecta se já tem algum emoji na linha
_RE_TEM_EMOJI = re.compile(
    "[\U00010000-\U0010ffff"
    "\U0001F300-\U0001F9FF"
    "\u2600-\u26FF"
    "\u2700

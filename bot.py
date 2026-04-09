"""
╔══════════════════════════════════════════════════════════════════════════╗
║  FOGUETÃO v64.0 — AMAZON + SHOPEE — NÍVEL CHINA/RÚSSIA                 ║
╠══════════════════════════════════════════════════════════════════════════╣
║  PILARES:                                                               ║
║  • Multi-links massivo (50 paralelos)                                   ║
║  • Pipeline por etapas com fila async                                   ║
║  • Deduplicação ultra-forte (3 camadas, por texto/cupom — nunca link)   ║
║  • Anti-loop: edição nunca vira nova mensagem                           ║
║  • System de retry com backoff exponencial                              ║
║  • Logs profissionais por módulo/plataforma                             ║
║  • Módulos isolados por plataforma (Amazon / Shopee)                    ║
║  • Fail-safe em cada etapa                                              ║
║  • Filtro inteligente: só Amazon e Shopee passam                        ║
║  • Queue/fila com concorrência controlada                               ║
║  • Formatação automática (monta mensagem bonita)                        ║
║  • Rate-limit / proteção FloodWait                                      ║
║  • Health-check periódico                                               ║
║  • Persistência: mapeamento survives restart                            ║
║  • Monitoramento multi-canal                                            ║
║  • Auto-restart em queda de rede                                        ║
╚══════════════════════════════════════════════════════════════════════════╝

PROBLEMAS CORRIGIDOS NESTA VERSÃO:
  1. Anti-loop edição: edição de mensagem que já foi enviada → EDITA (nunca
     reenvia). Edição de mensagem que ainda não existe no mapa → ignora.
  2. Filtro de links: links que não são Amazon nem Shopee são removidos do
     texto final. Se não sobrar NENHUM link válido → descarta a oferta.
  3. Formatação automática: copia o texto original linha a linha, troca os
     links pelos de afiliado, remove os estranhos e aplica layout bonito.
  4. Multi-links real: Amazon E Shopee na mesma mensagem, até 50 links,
     todos em paralelo.
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
from collections import defaultdict
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
# MÓDULO 1 ▸ LOGS PROFISSIONAIS — UM POR MÓDULO/PLATAFORMA
# ══════════════════════════════════════════════════════════════════════════

def _logger(nome: str, cor: str) -> logging.Logger:
    lg = logging.getLogger(nome)
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            f'\033[{cor}m[%(name)-10s]\033[0m %(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        ))
        lg.addHandler(h)
        lg.setLevel(logging.DEBUG)
    return lg

log_amz   = _logger('AMAZON',   '1;33')        # Amarelo
log_shp   = _logger('SHOPEE',   '1;38;5;208')  # Laranja
log_dedup = _logger('DEDUP',    '1;35')        # Roxo
log_img   = _logger('IMAGEM',   '1;36')        # Ciano
log_tg    = _logger('TELEGRAM', '1;32')        # Verde
log_fil   = _logger('FILTRO',   '1;31')        # Vermelho
log_lnk   = _logger('LINKS',    '1;34')        # Azul
log_fmt   = _logger('FORMAT',   '1;38;5;51')   # Turquesa
log_sys   = _logger('SISTEMA',  '1;37')        # Branco
log_hc    = _logger('HEALTH',   '1;38;5;118')  # Verde-lima


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
ARQUIVO_MAPEAMENTO = "map_mensagens.json"   # {str(msg_id_origem): int(msg_id_destino)}

# Concorrência controlada
_SEM_ENVIO  = asyncio.Semaphore(3)   # máx 3 envios simultâneos
_SEM_HTTP   = asyncio.Semaphore(20)  # máx 20 requisições HTTP simultâneas
_EXECUTOR   = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# Rate-limit interno: intervalo mínimo entre mensagens para o mesmo destino
_RATE_LOCK       = asyncio.Lock()
_ULTIMO_ENVIO_TS = 0.0
_INTERVALO_MIN   = 1.5   # segundos entre envios

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
# MÓDULO 3 ▸ PERSISTÊNCIA (MAPEAMENTO + CACHE) — SURVIVES RESTART
# ══════════════════════════════════════════════════════════════════════════

_MAP_LOCK   = Lock()
_CACHE_LOCK = Lock()


def _ler_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_sys.error(f"❌ Erro ao ler {path}: {e}")
        return {}


def _gravar_json(path: str, data: dict, lock: Lock):
    with lock:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log_sys.error(f"❌ Erro ao gravar {path}: {e}")


def ler_mapa() -> dict:
    with _MAP_LOCK:
        return _ler_json(ARQUIVO_MAPEAMENTO)


def salvar_mapa(mapa: dict):
    _gravar_json(ARQUIVO_MAPEAMENTO, mapa, _MAP_LOCK)


def ler_cache() -> dict:
    with _CACHE_LOCK:
        return _ler_json(ARQUIVO_CACHE)


def salvar_cache(cache: dict):
    _gravar_json(ARQUIVO_CACHE, cache, _CACHE_LOCK)


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 4 ▸ FILTRO BLINDADO
# ══════════════════════════════════════════════════════════════════════════

_FILTRO = [
    "Monitor Samsung", "Fonte Mancer", "Placa de video", "Monitor LG",
    "PC home Essential", "Suporte articulado", "Gabinetes em oferta",
    "VHAGAR", "Superframe", "AM5", "AM4", "GTX", "DDR5", "DDR4",
    "Dram", "Monitor Safe", "Monitor Redragon", "CL18", "CL16", "CL32",
    "MT/s", "MHz", "RX 580", "Ryzen", "Placa Mãe", "Gabinete Gamer",
    "Water Cooler", "Air Cooler",
]


def texto_bloqueado(texto: str) -> bool:
    tl = texto.lower()
    for p in _FILTRO:
        if p.lower() in tl:
            log_fil.debug(f"🚫 Filtro: '{p}'")
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 5 ▸ DEDUPLICAÇÃO ULTRA-FORTE — 3 CAMADAS
#
#  PRINCÍPIO: nunca olha para o link — só texto normalizado, preço e cupom.
#  • Texto mudou  → envia de novo
#  • Cupom mudou  → envia de novo
#  • Link mudou mas texto/cupom iguais → BLOQUEIA (anti-reenvio eterno)
# ══════════════════════════════════════════════════════════════════════════

_TTL            = 120 * 60   # 120 min
_JANELA         = 900        # 15 min de janela antispam
_SIM_NORMAL     = 0.85
_SIM_ALTA       = 0.92

_RUIDO = {
    "promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora",
    "relampago","relâmpago",
}


def _rm_acentos(t: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', t)
                   if unicodedata.category(c) != 'Mn')


def _norm(texto: str) -> str:
    if not texto:
        return ""
    t = _rm_acentos(texto.lower())
    t = re.sub(r"http\S+|www\S+", " ", t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return " ".join(sorted(w for w in t.split() if w not in _RUIDO))


def deve_enviar(plataforma: str, prod_id: str, preco: str,
                cupom: str = "", texto: str = "") -> bool:
    cache = ler_cache()
    agora = time.time()

    # Limpa expirados
    cache = {k: v for k, v in cache.items()
             if agora - v.get("ts", 0) < _TTL}

    tnorm      = _norm(texto)
    cnorm      = cupom.strip().upper()
    fingerprint = f"{plataforma}|{prod_id}|{preco}|{cnorm}|{tnorm}"
    h = hashlib.sha256(fingerprint.encode()).hexdigest()

    # C1 — hash exato
    if h in cache:
        log_dedup.info(f"🔁 [C1] Hash exato | prod={prod_id} cupom={cnorm}")
        return False

    for entrada in cache.values():
        if agora - entrada.get("ts", 0) >= _JANELA:
            continue
        sim         = SequenceMatcher(None, tnorm, entrada.get("txt", "")).ratio()
        c_igual     = entrada.get("cupom", "") == cnorm.lower()
        prod_igual  = str(entrada.get("prod")) == str(prod_id) and prod_id != "0"
        preco_igual = str(entrada.get("preco")) == str(preco)  and preco  != "0"

        # C2 — mesmo produto/preço/cupom + texto parecido
        if prod_igual and preco_igual and c_igual and sim >= _SIM_NORMAL:
            log_dedup.info(f"🔁 [C2] Similar prod={prod_id} sim={sim:.2f}")
            return False

        # C3 — cupom igual + texto quase idêntico (sem prod_id, ex: cupons genéricos)
        if c_igual and sim >= _SIM_ALTA:
            log_dedup.info(f"🔁 [C3] Cupom+texto idêntico sim={sim:.2f}")
            return False

    cache[h] = {
        "plat": plataforma, "prod": str(prod_id),
        "preco": str(preco), "cupom": cnorm.lower(),
        "txt": tnorm, "ts": agora,
    }
    salvar_cache(cache)
    log_dedup.debug(f"✅ Nova oferta registrada | plat={plataforma}")
    return True


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 6 ▸ DESENCURTADOR — ATÉ O OSSO (12 níveis, HEAD+GET+meta+JS)
# ══════════════════════════════════════════════════════════════════════════

async def desencurtar(url: str, sessao: aiohttp.ClientSession,
                       depth: int = 0) -> str:
    if depth > 12:
        return url
    hdrs = {"User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Accept-Language": "pt-BR,pt;q=0.9"}
    try:
        # HEAD rápido
        try:
            async with sessao.head(url, headers=hdrs, allow_redirects=True,
                                   timeout=aiohttp.ClientTimeout(total=8)) as r:
                final = str(r.url)
                if final != url:
                    log_lnk.debug(f"  HEAD d={depth} → {final[:70]}")
                    return await desencurtar(final, sessao, depth + 1)
                return final
        except Exception:
            pass

        # GET completo
        async with sessao.get(url, headers=hdrs, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=12)) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")
            soup = BeautifulSoup(html, "html.parser")

            # meta refresh
            ref = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)", ref["content"], re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    log_lnk.debug(f"  META d={depth} → {novo[:70]}")
                    return await desencurtar(novo, sessao, depth + 1)

            # window.location JS
            mj = re.search(r'window\.location(?:\.href)?\s*=\s*["\']([^"\']+)["\']', html)
            if mj:
                log_lnk.debug(f"  JS d={depth} → {mj.group(1)[:70]}")
                return await desencurtar(mj.group(1), sessao, depth + 1)

            if pos != url:
                return await desencurtar(pos, sessao, depth + 1)
            return pos
    except asyncio.TimeoutError:
        log_lnk.warning(f"⏱ Timeout desencurtando: {url[:70]}")
        return url
    except Exception as e:
        log_lnk.error(f"❌ Erro desencurtando {url[:60]}: {e}")
        return url


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 7 ▸ CLASSIFICAÇÃO DE PLATAFORMA
# ══════════════════════════════════════════════════════════════════════════

_DOM_AMAZON  = {"amazon.com", "amzn.to", "amzn.com", "a.co"}
_DOM_SHOPEE  = {"shopee.com", "s.shopee", "shope.ee"}
# Domínios que devem ser SEMPRE descartados (nunca expandir para afiliado)
_DOM_LIXO    = {
    "magazineluiza.com.br", "magazinevoce.com.br", "maga.lu",
    "casasbahia.com.br", "americanas.com.br", "submarino.com.br",
    "shoptime.com.br", "extra.com.br", "pontofrio.com.br",
    "mercadolivre.com", "mercadopago.com", "meli.la",
    "aliexpress.com", "ali.ski", "kabum.com.br", "pichau.com.br",
    "terabyteshop.com.br", "fastshop.com.br", "leroy.com.br",
    "t.me", "telegram.me",
}


def classificar(url: str) -> str | None:
    """Retorna 'amazon', 'shopee' ou None (descartado)."""
    try:
        netloc = urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return None

    for d in _DOM_AMAZON:
        if d in netloc:
            return "amazon"
    for d in _DOM_SHOPEE:
        if d in netloc:
            return "shopee"
    # Checa lixo explícito
    for d in _DOM_LIXO:
        if d in netloc:
            log_lnk.debug(f"🗑 Domínio lixo: {netloc}")
            return None
    return None   # desconhecido → será expandido


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ MOTOR AMAZON — TAG + URL LIMPA E CLICÁVEL
# ══════════════════════════════════════════════════════════════════════════

_LIXO_AMZ_PARAMS = {
    "ascsubtag","btn_ref","ref_","ref","smid","sprefix","sr","spla",
    "dchild","linkcode","linkid","camp","creative","pf_rd_p","pf_rd_r",
    "pd_rd_wg","pd_rd_w","content-id","pd_rd_r","pd_rd_i","ie","qid",
    "_encoding",
}


def _limpar_amazon(url_exp: str) -> str:
    """Corta /ref=... do path, remove params lixo, injeta tag."""
    p    = urlparse(url_exp)
    path = re.sub(r'(/dp/[A-Z0-9]{10}).*',         r'\1', p.path)
    path = re.sub(r'(/gp/product/[A-Z0-9]{10}).*', r'\1', path)
    prms = {k: v for k, v in parse_qs(p.query, keep_blank_values=False).items()
            if k.lower() not in _LIXO_AMZ_PARAMS}
    prms["tag"] = [AMAZON_TAG]
    return urlunparse(p._replace(path=path, query=urlencode(prms, doseq=True)))


async def _motor_amazon(url: str, sessao: aiohttp.ClientSession) -> str | None:
    """Desencurta + limpa + injeta tag. Retorna None se não for Amazon."""
    log_amz.debug(f"🔗 Amazon iniciando | {url[:70]}")
    async with _SEM_HTTP:
        exp = await desencurtar(url, sessao)
    log_amz.debug(f"  Expandida | {exp[:70]}")

    if classificar(exp) != "amazon":
        log_amz.warning(f"  ⚠️ Não é Amazon após expansão: {exp[:60]}")
        return None

    final = _limpar_amazon(exp)
    log_amz.info(f"  ✅ Convertida | {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 9 ▸ MOTOR SHOPEE — API DE AFILIADO COM RETRY
# ══════════════════════════════════════════════════════════════════════════

async def _motor_shopee(url: str, sessao: aiohttp.ClientSession) -> str | None:
    """Desencurta + API afiliado Shopee. Retorna None em falha total."""
    log_shp.debug(f"🔗 Shopee iniciando | {url[:70]}")
    async with _SEM_HTTP:
        exp = await desencurtar(url, sessao)
    log_shp.debug(f"  Expandida | {exp[:70]}")

    ts      = str(int(time.time()))
    payload = json.dumps(
        {"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{exp}" }}) '
                  f'{{ shortLink }} }}'},
        separators=(",", ":")
    )
    sig = hashlib.sha256(
        f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()
    ).hexdigest()
    hdrs = {
        "Authorization": f"SHA256 Credential={SHOPEE_APP_ID},"
                         f"Timestamp={ts},Signature={sig}",
        "Content-Type": "application/json",
    }

    for tentativa in range(1, 4):
        try:
            async with _SEM_HTTP:
                async with sessao.post(
                    "https://open-api.affiliate.shopee.com.br/graphql",
                    data=payload, headers=hdrs,
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as r:
                    res  = await r.json()
                    link = res["data"]["generateShortLink"]["shortLink"]
                    log_shp.info(f"  ✅ Convertida | {link}")
                    return link
        except Exception as e:
            log_shp.warning(f"  ⚠️ Tentativa {tentativa}/3 falhou: {e}")
            await asyncio.sleep(2 ** tentativa)

    log_shp.error(f"  ❌ API Shopee falhou 3x | url={exp[:60]}")
    return None


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 10 ▸ PIPELINE DE CONVERSÃO — TODOS OS LINKS EM PARALELO
# ══════════════════════════════════════════════════════════════════════════

async def _converter_um(url: str, sessao: aiohttp.ClientSession) -> tuple:
    """
    Converte um único link.
    Retorna (url_convertida, plataforma) ou (None, None) se descartado.
    """
    ul   = url.lower()
    plat = classificar(url)

    # Identificação direta
    if plat == "amazon":
        novo = await _motor_amazon(url, sessao)
        return (novo, "amazon") if novo else (None, None)

    if plat == "shopee":
        novo = await _motor_shopee(url, sessao)
        return (novo, "shopee") if novo else (None, None)

    if plat is None and not any(d in ul for d in _DOM_LIXO):
        # Pode ser encurtador genérico — expande e reclassifica
        log_lnk.debug(f"🔄 Expandindo desconhecido: {url[:60]}")
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)
        plat2 = classificar(exp)
        if plat2 == "amazon":
            novo = await _motor_amazon(exp, sessao)
            return (novo, "amazon") if novo else (None, None)
        if plat2 == "shopee":
            novo = await _motor_shopee(exp, sessao)
            return (novo, "shopee") if novo else (None, None)

    log_lnk.info(f"🗑 Link descartado: {url[:70]}")
    return None, None


async def converter_links(links: list) -> tuple:
    """
    Converte até 50 links em paralelo.
    Retorna (mapa {original: convertido}, plataforma_principal).
    """
    log_lnk.info(f"🚀 Conversão paralela: {len(links)} link(s)")
    conn = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False)
    async with aiohttp.ClientSession(
        connector=conn,
        timeout=aiohttp.ClientTimeout(total=30, connect=8),
        headers={"User-Agent": random.choice(USER_AGENTS)},
    ) as sessao:
        resultados = await asyncio.gather(
            *[_converter_um(l, sessao) for l in links[:50]],
            return_exceptions=True,
        )

    mapa, plats = {}, []
    for i, res in enumerate(resultados):
        if isinstance(res, Exception):
            log_lnk.error(f"  ❌ Exceção link[{i}] {links[i][:50]}: {res}")
            continue
        novo, plat = res
        if novo and plat:
            mapa[links[i]] = novo
            plats.append(plat)
            log_lnk.debug(f"  [{plat.upper()}] {links[i][:40]} → {novo[:50]}")

    plat_p = max(set(plats), key=plats.count) if plats else "amazon"
    log_lnk.info(f"✅ Convertidos: {len(mapa)}/{len(links)} | plat={plat_p}")
    return mapa, plat_p


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 11 ▸ FORMATAÇÃO AUTOMÁTICA
#
#  ESTRATÉGIA:
#  1. Pega o texto original LINHA POR LINHA
#  2. Troca cada link pelo afiliado correspondente
#  3. Remove linhas que contêm apenas links descartados
#  4. Aplica layout bonito mantendo a estrutura original do texto
#     (não inventa linhas que não existiam — só enfeita o que está lá)
# ══════════════════════════════════════════════════════════════════════════

_RE_URL      = re.compile(r'https?://[^\s\)\]>,"\']+')
_RE_PRECO    = re.compile(r'R\$\s?[\d.,]+')
_RE_CUPOM_KW = re.compile(
    r'\b(?:cupom|cupon|off|resgate|codigo|coupon|desconto|assine)\b', re.I)
_RE_EMOJI    = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF"
    r"\U0001F900-\U0001F9FF\u2B50\u2B55\u231A\u231B\u25A0-\u25FF]",
    flags=re.UNICODE,
)


def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI.search(s))


def formatar(texto_original: str, mapa: dict, plat: str) -> str:
    """
    Formata a mensagem final:
    1. Processa linha a linha
    2. Se a linha contém um link:
       - Troca pelo afiliado se convertido
       - Remove completamente se não convertido (lixo)
    3. Aplica emoji de contexto nas linhas sem emoji
    4. Remove linhas vazias excessivas
    """
    aceitos = set(mapa.values())
    linhas_orig = texto_original.split('\n')
    saida       = []
    primeira_linha_texto = True

    for linha in linhas_orig:
        linha_strip = linha.strip()
        if not linha_strip:
            saida.append("")
            continue

        # Verifica se a linha é UM link isolado
        if _RE_URL.match(linha_strip) and not _RE_URL.sub("", linha_strip).strip():
            url_bruta = linha_strip.rstrip('.,;)')
            if url_bruta in mapa:
                # Linha é só um link convertido → coloca o link novo
                saida.append(mapa[url_bruta])
            else:
                # Link não convertido → descarta a linha inteira
                log_fmt.debug(f"🗑 Linha-link descartada: {url_bruta[:60]}")
            continue

        # Linha com texto (pode ter link inline)
        nova_linha = linha_strip

        # Substitui links inline dentro da linha
        def _sub_link(match):
            url = match.group(0).rstrip('.,;)')
            if url in mapa:
                return mapa[url]
            # Link não convertido dentro de linha de texto → remove só o link
            log_fmt.debug(f"🗑 Link inline descartado: {url[:60]}")
            return ""

        nova_linha_sub = _RE_URL.sub(_sub_link, nova_linha)

        # Se depois de substituir a linha ficou só espaço → descarta
        if not nova_linha_sub.strip():
            continue

        nova_linha = nova_linha_sub.strip()

        # Aplica emoji de contexto SE a linha não tem emoji próprio
        if not _tem_emoji(nova_linha):
            tem_preco = bool(_RE_PRECO.search(nova_linha))
            tem_cupom = bool(_RE_CUPOM_KW.search(nova_linha))
            eh_link   = bool(_RE_URL.match(nova_linha))

            if not eh_link:
                if primeira_linha_texto and not tem_preco and not tem_cupom:
                    # Primeira linha de texto = título → 🔥
                    nova_linha = "🔥 " + nova_linha
                    primeira_linha_texto = False
                elif tem_preco and tem_cupom:
                    nova_linha = "🎟 " + nova_linha
                elif tem_preco:
                    nova_linha = "✅ " + nova_linha
                elif tem_cupom:
                    nova_linha = "🎟 " + nova_linha
                else:
                    # Linha de texto simples após a primeira → deixa como está
                    if primeira_linha_texto:
                        primeira_linha_texto = False
        else:
            # Tem emoji → só marca que passou a primeira linha
            if primeira_linha_texto and not _RE_URL.match(nova_linha):
                primeira_linha_texto = False

        saida.append(nova_linha)

    # Remove linhas vazias consecutivas (máx 1 vazia seguida)
    resultado = []
    ultima_vazia = False
    for l in saida:
        if l.strip() == "":
            if not ultima_vazia:
                resultado.append("")
            ultima_vazia = True
        else:
            ultima_vazia = False
            resultado.append(l)

    texto_final = "\n".join(resultado).strip()
    log_fmt.debug(f"✅ Formatação concluída | {len(texto_final)} chars")
    return texto_final


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 12 ▸ BUSCA DE IMAGEM — 3 TENTATIVAS + FALLBACK PREVIEW
# ══════════════════════════════════════════════════════════════════════════

async def buscar_imagem(url: str) -> str | None:
    for t in range(1, 4):
        log_img.debug(f"🖼 Tentativa {t}/3 | {url[:60]}")
        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": random.choice(USER_AGENTS)}
            ) as s:
                async with s.get(url, allow_redirects=True,
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    soup = BeautifulSoup(await r.text(errors="ignore"), "html.parser")
                    tag  = (soup.find("meta", property="og:image") or
                            soup.find("meta", attrs={"name": "twitter:image"}))
                    if tag and tag.get("content"):
                        img = tag["content"]
                        log_img.info(f"✅ Imagem (t={t}): {img[:70]}")
                        return img
        except asyncio.TimeoutError:
            log_img.warning(f"⏱ Timeout t={t}/3")
        except Exception as e:
            log_img.warning(f"⚠️ Erro t={t}/3: {e}")
        await asyncio.sleep(1.5 * t)
    log_img.warning("❌ Sem imagem → link preview")
    return None


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 13 ▸ FINGERPRINT DA OFERTA
# ══════════════════════════════════════════════════════════════════════════

def _prod_id(mapa: dict) -> str:
    if not mapa:
        return "0"
    primeiro = list(mapa.values())[0]
    for pat in [r'/dp/([A-Z0-9]{10})', r'/gp/product/([A-Z0-9]{10})',
                r'/i\.(\d+\.\d+)',     r'/product/(\d+)']:
        m = re.search(pat, primeiro)
        if m:
            return m.group(1)
    return primeiro[-20:]

def _preco(t: str) -> str:
    m = _RE_PRECO.search(t)
    return m.group(0).strip() if m else "0"

def _cupom(t: str) -> str:
    m = re.search(r'\b([A-Z][A-Z0-9]{3,19})\b', t)
    return m.group(1) if m else ""

def _eh_cupom(t: str) -> bool:
    return bool(_RE_CUPOM_KW.search(t))

def _eh_cupom_shopee(t: str, plat: str) -> bool:
    return _eh_cupom(t) and plat == "shopee"

def _tem_midia(media) -> bool:
    return media is not None and not isinstance(media, MessageMediaWebPage)


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ RATE-LIMIT INTERNO
# ══════════════════════════════════════════════════════════════════════════

async def _aguardar_rate_limit():
    """Garante intervalo mínimo entre envios para não sofrer flood."""
    global _ULTIMO_ENVIO_TS
    async with _RATE_LOCK:
        agora    = time.monotonic()
        espera   = _INTERVALO_MIN - (agora - _ULTIMO_ENVIO_TS)
        if espera > 0:
            await asyncio.sleep(espera)
        _ULTIMO_ENVIO_TS = time.monotonic()


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 15 ▸ ANTI-LOOP — CONTROLE DE MENSAGENS EDITADAS
#
#  PROBLEMA: quando o bot envia uma mensagem, o Telegram às vezes dispara
#  um evento MessageEdited logo após (para atualizar o preview do link).
#  Isso faz o bot processar a mensagem DE NOVO como se fosse nova.
#
#  SOLUÇÃO:
#  • Mantemos um set de IDs de mensagens de ORIGEM que já foram processadas.
#  • Se chegar NewMessage com id que já está no set → ignora.
#  • Se chegar MessageEdited com id que NÃO está no mapa → ignora também
#    (nunca enviamos essa mensagem, então não há o que editar).
# ══════════════════════════════════════════════════════════════════════════

_IDS_PROCESSADOS: set = set()   # IDs de mensagens de origem já enviadas
_IDS_LOCK = asyncio.Lock()

async def _marcar_processado(msg_id: int):
    async with _IDS_LOCK:
        _IDS_PROCESSADOS.add(msg_id)
        # Limita o tamanho do set (guarda os últimos 2000)
        if len(_IDS_PROCESSADOS) > 2000:
            excesso = len(_IDS_PROCESSADOS) - 2000
            for _ in range(excesso):
                _IDS_PROCESSADOS.pop()

async def _ja_processado(msg_id: int) -> bool:
    async with _IDS_LOCK:
        return msg_id in _IDS_PROCESSADOS


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 16 ▸ PROCESSAMENTO PRINCIPAL — PIPELINE COMPLETO
# ══════════════════════════════════════════════════════════════════════════

async def processar(event, is_edit: bool = False):
    msg_id      = event.message.id
    texto_bruto = event.message.text or ""
    chat        = await event.get_chat()
    username    = (chat.username or str(event.chat_id)).lower()

    log_tg.info(f"{'✏️ EDIT' if is_edit else '📩 NEW'} | @{username} | id={msg_id} | "
                f"chars={len(texto_bruto)}")

    # ── ETAPA 1: Validação básica ─────────────────────────────────────
    if not texto_bruto.strip():
        log_sys.debug("⏩ Vazio")
        return

    # ── ETAPA 2: Anti-loop ────────────────────────────────────────────
    if not is_edit:
        # Nova mensagem: se já processamos → ignora (previne loop de preview)
        if await _ja_processado(msg_id):
            log_sys.debug(f"⏩ Anti-loop: id={msg_id} já processado")
            return
    else:
        # Edição: só processa se já temos o id no mapeamento
        mapa_atual = await asyncio.get_event_loop().run_in_executor(
            _EXECUTOR, ler_mapa)
        if str(msg_id) not in mapa_atual:
            log_sys.debug(f"⏩ Edição sem mapa (preview Telegram?): id={msg_id}")
            return

    # ── ETAPA 3: Filtro blindado ──────────────────────────────────────
    if texto_bloqueado(texto_bruto):
        log_fil.warning(f"🚫 Filtro | @{username}")
        return

    # ── ETAPA 4: Extrai links ─────────────────────────────────────────
    links_raw = _RE_URL.findall(texto_bruto)
    # Limpa pontuação no final de cada link
    links_raw = [l.rstrip('.,;)>') for l in links_raw]
    log_lnk.info(f"🔗 {len(links_raw)} link(s) encontrado(s)")

    if not links_raw and "fadadoscupons" not in username:
        log_sys.debug("⏩ Sem links e não é fadadoscupons")
        return

    # ── ETAPA 5: Conversão paralela de todos os links ─────────────────
    mapa_links, plat_p = {}, "amazon"
    if links_raw:
        mapa_links, plat_p = await converter_links(links_raw)

    # Se havia links mas NENHUM é Amazon/Shopee → descarta
    if links_raw and not mapa_links:
        log_sys.warning(f"🚫 Zero links Amazon/Shopee | @{username}")
        return

    # ── ETAPA 6: Deduplicação ─────────────────────────────────────────
    prod = _prod_id(mapa_links)
    prec = _preco(texto_bruto)
    cup  = _cupom(texto_bruto)
    log_dedup.debug(f"🔬 prod={prod} preco={prec} cupom={cup} plat={plat_p}")

    if not is_edit:
        if not deve_enviar(plat_p, prod, prec, cup, texto_bruto):
            log_dedup.info("🚫 Duplicata bloqueada")
            return

    # ── ETAPA 7: Formatação ───────────────────────────────────────────
    msg_final = formatar(texto_bruto, mapa_links, plat_p)
    log_fmt.debug(f"📝 Mensagem final ({len(msg_final)} chars):\n{msg_final[:200]}")

    # ── ETAPA 8: Imagem ───────────────────────────────────────────────
    media_orig = event.message.media
    tem_img    = _tem_midia(media_orig)
    imagem     = None

    if _eh_cupom(texto_bruto):
        if tem_img:
            imagem = media_orig
            log_img.debug("🖼 Cupom: imagem real do grupo")
        elif plat_p == "shopee":
            imagem = IMG_SHOPEE if os.path.exists(IMG_SHOPEE) else None
            log_img.info(f"🖼 Cupom Shopee → fallback: {IMG_SHOPEE}")
        else:
            imagem = IMG_AMAZON if os.path.exists(IMG_AMAZON) else None
            log_img.info(f"🖼 Cupom Amazon → fallback: {IMG_AMAZON}")
    else:
        if tem_img:
            imagem = media_orig
            log_img.debug("🖼 Oferta: imagem real")
        elif mapa_links:
            imagem = await buscar_imagem(list(mapa_links.values())[0])

    # ── ETAPA 9: Envio / Edição ───────────────────────────────────────
    await _aguardar_rate_limit()

    async with _SEM_ENVIO:
        loop = asyncio.get_event_loop()
        mapa = await loop.run_in_executor(_EXECUTOR, ler_mapa)

        try:
            # MODO EDIÇÃO
            if is_edit:
                id_dest = mapa[str(msg_id)]
                log_tg.info(f"✏️ Editando destino id={id_dest}")
                for tentativa in range(1, 4):
                    try:
                        await client.edit_message(GRUPO_DESTINO, id_dest, msg_final)
                        log_tg.info("✅ Edição ok")
                        break
                    except MessageNotModifiedError:
                        log_tg.debug("⏩ Conteúdo idêntico")
                        break
                    except FloodWaitError as e:
                        log_tg.warning(f"⏳ FloodWait edição: {e.seconds}s")
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        log_tg.error(f"❌ Erro edição t={tentativa}: {e}")
                        if tentativa < 3:
                            await asyncio.sleep(2 ** tentativa)
                return

            # MODO ENVIO — com retry
            sent = None
            for tentativa in range(1, 4):
                try:
                    if imagem:
                        if len(msg_final) > 1024:
                            f_img = await client.send_file(GRUPO_DESTINO, imagem)
                            sent  = await client.send_message(
                                GRUPO_DESTINO, msg_final, reply_to=f_img.id)
                        else:
                            sent = await client.send_file(
                                GRUPO_DESTINO, imagem, caption=msg_final)
                    else:
                        sent = await client.send_message(
                            GRUPO_DESTINO, msg_final, link_preview=True)
                    break  # Sucesso
                except FloodWaitError as e:
                    log_tg.warning(f"⏳ FloodWait envio: {e.seconds}s")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    log_tg.error(f"❌ Envio t={tentativa}: {e}")
                    if tentativa == 1 and imagem:
                        # Retry sem imagem
                        imagem = None
                    elif tentativa < 3:
                        await asyncio.sleep(2 ** tentativa)

            if sent:
                mapa[str(msg_id)] = sent.id
                await loop.run_in_executor(_EXECUTOR, salvar_mapa, mapa)
                await _marcar_processado(msg_id)
                log_sys.info(
                    f"🚀 [OK] @{username} → {GRUPO_DESTINO} | "
                    f"origem={msg_id} destino={sent.id} | plat={plat_p.upper()}")

        except Exception as e:
            log_sys.error(f"❌ ERRO CRÍTICO: {e}", exc_info=True)


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 17 ▸ HEALTH CHECK PERIÓDICO
# ══════════════════════════════════════════════════════════════════════════

async def _health_check():
    """A cada 5 minutos loga o status do bot."""
    while True:
        await asyncio.sleep(300)
        cache = ler_cache()
        mapa  = ler_mapa()
        log_hc.info(
            f"💚 Health | cache={len(cache)} entradas | "
            f"mapa={len(mapa)} msgs | "
            f"processados={len(_IDS_PROCESSADOS)}"
        )


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 18 ▸ INICIALIZAÇÃO COM AUTO-RESTART
# ══════════════════════════════════════════════════════════════════════════

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


async def _run():
    log_sys.info("🔌 Conectando ao Telegram...")
    await client.connect()

    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida! Verifique TELEGRAM_SESSION no Railway.")
        return False

    me = await client.get_me()
    log_sys.info(f"✅ Conectado: {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 Grupos monitorados: {GRUPOS_ORIGEM}")
    log_sys.info(f"📣 Destino: {GRUPO_DESTINO}")
    log_sys.info(f"🏷  Amazon tag: {AMAZON_TAG} | Shopee App: {SHOPEE_APP_ID}")
    log_sys.info("🚀 FOGUETÃO v64.0 ONLINE — AMAZON + SHOPEE!")

    # Registra handlers
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

    # Inicia health-check em background
    asyncio.create_task(_health_check())

    await client.run_until_disconnected()
    return True


async def main():
    """Auto-restart: reinicia em 15s em qualquer queda de rede/flood."""
    while True:
        try:
            await _run()
        except (AuthKeyUnregisteredError, SessionPasswordNeededError) as e:
            log_sys.error(f"❌ Erro de autenticação — encerrando: {e}")
            break
        except Exception as e:
            log_sys.error(f"💥 Bot caiu: {e} — reiniciando em 15s...", exc_info=True)
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(15)


if __name__ == "__main__":
    asyncio.run(main())

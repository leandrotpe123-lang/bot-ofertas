"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  FOGUETÃO v68.0 — ARQUITETURA PROFISSIONAL ISOLADA POR PLATAFORMA          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  PRINCÍPIOS ARQUITETURAIS:                                                  ║
║                                                                             ║
║  1. ISOLAMENTO TOTAL: Amazon / Shopee / Magalu são módulos completamente    ║
║     independentes. Nenhuma tag, template ou estado cruza plataformas.       ║
║                                                                             ║
║  2. MAGALU: SEMPRE substitui parâmetros com os do sistema (nunca preserva   ║
║     parâmetros externos). Limpa → aplica meus params → Cuttly → envia.     ║
║                                                                             ║
║  3. AMAZON: tag leo21073-20 SOMENTE em _limpar_url_amazon(). Em nenhuma     ║
║     outra função do código.                                                 ║
║                                                                             ║
║  4. SHOPEE: sem desencurtamento. API direta. Totalmente isolada.            ║
║                                                                             ║
║  5. DEDUPLICAÇÃO por mudança real: chave = plataforma + cupom + campanha.   ║
║     PERMITE reenvio quando cupom ou campanha mudam.                         ║
║     BLOQUEIA apenas quando identicamente igual.                             ║
║     Sem bloqueio cego por tempo.                                            ║
║                                                                             ║
║  6. RENDERIZADOR por plataforma: Amazon tem template próprio, Shopee tem    ║
║     template próprio, Magalu tem template próprio. Nunca compartilhados.   ║
║                                                                             ║
║  7. ESTADO LIMPO: cada processamento inicia com contexto zerado.           ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os, re, time, json, asyncio, aiohttp, hashlib, random, io
import unicodedata, logging, concurrent.futures
from dataclasses import dataclass, field
from typing import Optional
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage
from telethon.errors import (
    MessageNotModifiedError, FloodWaitError,
    AuthKeyUnregisteredError, SessionPasswordNeededError,
)
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, quote
from difflib import SequenceMatcher
from threading import Lock

try:
    from PIL import Image
    _PIL_OK = True
except ImportError:
    _PIL_OK = False


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 1 ▸ LOGS — UM POR PLATAFORMA E POR MÓDULO (ISOLADOS)
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

# Logs completamente separados por plataforma
log_amz  = _mk_log('AMAZON',   '1;33')        # Amarelo
log_shp  = _mk_log('SHOPEE',   '1;38;5;208')  # Laranja
log_mgl  = _mk_log('MAGALU',   '1;34')        # Azul
log_dedup= _mk_log('DEDUP',    '1;35')        # Roxo
log_img  = _mk_log('IMAGEM',   '1;36')        # Ciano
log_tg   = _mk_log('TELEGRAM', '1;32')        # Verde
log_fil  = _mk_log('FILTRO',   '1;31')        # Vermelho
log_lnk  = _mk_log('LINKS',    '1;38;5;51')   # Azul-claro
log_sys  = _mk_log('SISTEMA',  '1;37')        # Branco
log_hc   = _mk_log('HEALTH',   '1;38;5;118')  # Verde-lima


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 2 ▸ CONFIGURAÇÕES GERAIS
# ══════════════════════════════════════════════════════════════════════════════

API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION", "")

GRUPOS_ORIGEM  = ['promotom', 'fumotom', 'botofera', 'fadadoscupons']
GRUPO_DESTINO  = '@ofertap'

# ─── Tags e credenciais POR PLATAFORMA (nunca misturar) ──────────────────────

# AMAZON — tag usada SOMENTE em motor_amazon → _limpar_url_amazon()
_AMZ_TAG       = os.environ.get("AMAZON_TAG",    "leo21073-20")

# SHOPEE — API isolada
_SHP_APP_ID    = os.environ.get("SHOPEE_APP_ID", "18348480261")
_SHP_SECRET    = os.environ.get("SHOPEE_SECRET", "SGC7FQQQ4R5QCFULPXIBCANATLP272B3")

# MAGALU — promoter/partner isolados
_MGL_PARTNER   = os.environ.get("MAGALU_PARTNER_ID",  "3440")
_MGL_PROMOTER  = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
_MGL_PID       = os.environ.get("MAGALU_PID",         "magazinevoce")
_MGL_SLUG      = os.environ.get("MAGALU_SLUG",        "magazineleo12")
_CUTTLY_KEY    = os.environ.get("CUTTLY_API_KEY",     "8d2afd3c7f72869f42d23cf0d849c72172509")

# Imagens de fallback
_IMG_AMZ = "cupom-amazon.jpg"
_IMG_SHP = "IMG_20260404_180150.jpg"
_IMG_MGL = "magalu_promo.jpg"

ARQUIVO_CACHE      = "cache_dedup.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

_SEM_ENVIO       = asyncio.Semaphore(3)
_SEM_HTTP        = asyncio.Semaphore(20)
_EXECUTOR        = concurrent.futures.ThreadPoolExecutor(max_workers=4)
_RATE_LOCK       = asyncio.Lock()
_ULTIMO_ENVIO_TS = 0.0
_INTERVALO_MIN   = 1.5

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 3 ▸ PERSISTÊNCIA — SURVIVES RESTART
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
# MÓDULO 4 ▸ FILTRO BLINDADO DE TEXTO
# ══════════════════════════════════════════════════════════════════════════════

_FILTRO_TEXTO = [
    "Monitor Samsung", "Fonte Mancer", "Placa de video", "Monitor LG",
    "PC home Essential", "Suporte articulado", "Gabinetes em oferta",
    "VHAGAR", "Superframe", "AM5", "AM4", "GTX", "DDR5", "DDR4",
    "Dram", "Monitor Safe", "Monitor Redragon", "CL18", "CL16", "CL32",
    "MT/s", "MHz", "RX 580", "Ryzen", "Placa Mãe", "Gabinete Gamer",
    "Water Cooler", "Air Cooler",
]

def texto_bloqueado(texto: str) -> bool:
    tl = texto.lower()
    for p in _FILTRO_TEXTO:
        if p.lower() in tl:
            log_fil.debug(f"🚫 '{p}'")
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 5 ▸ WHITELIST E CLASSIFICAÇÃO
#
# Whitelist positiva: SOMENTE esses domínios passam.
# Domínio não listado → descartado antes de qualquer processamento.
# ══════════════════════════════════════════════════════════════════════════════

_WHITELIST: dict[str, str] = {
    # Amazon
    "amazon.com.br":               "amazon",
    "amzn.to":                     "amazon",
    "amzn.com":                    "amazon",
    "a.co":                        "amazon",
    # Shopee
    "shopee.com.br":               "shopee",
    "s.shopee.com.br":             "shopee",
    "shopee.com":                  "shopee",
    "shope.ee":                    "shopee",
    # Magalu
    "magazineluiza.com.br":        "magalu",
    "sacola.magazineluiza.com.br": "magalu",
    "magazinevoce.com.br":         "magalu",
    "maga.lu":                     "magalu",
}

# Encurtadores que precisam ser expandidos para reclassificar
_ENCURTADORES = frozenset(["bit.ly", "tinyurl.com", "t.co", "ow.ly",
                             "goo.gl", "rb.gy", "is.gd", "tiny.cc", "buff.ly"])

def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""

def classificar(url: str) -> Optional[str]:
    """
    Retorna 'amazon', 'shopee', 'magalu', 'expandir' ou None.
    None = fora da whitelist = descartado.
    """
    nl = _netloc(url)
    for dom, plat in _WHITELIST.items():
        if dom in nl:
            return plat
    for enc in _ENCURTADORES:
        if enc in nl:
            return "expandir"
    log_lnk.debug(f"🗑 Whitelist: {nl}")
    return None

def _eh_vitrine_magalu(url: str) -> bool:
    return bool(re.search(
        r'(magazineluiza\.com\.br|magazinevoce\.com\.br)'
        r'.*(vitrine|categoria|lista|promo|stores|loja)',
        url, re.I))


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 6 ▸ DESENCURTADOR UNIVERSAL
# Chamado apenas por motor_amazon (amzn.to), motor_magalu (maga.lu) e
# pelo pipeline quando encontra encurtador genérico.
# Motor Shopee NUNCA chama desencurtar().
# ══════════════════════════════════════════════════════════════════════════════

async def desencurtar(url: str, sessao: aiohttp.ClientSession, depth: int = 0) -> str:
    if depth > 12:
        return url
    hdrs = {"User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Accept-Language": "pt-BR,pt;q=0.9"}
    try:
        try:
            async with sessao.head(url, headers=hdrs, allow_redirects=True,
                                   timeout=aiohttp.ClientTimeout(total=8)) as r:
                final = str(r.url)
                if final != url:
                    return await desencurtar(final, sessao, depth + 1)
                return final
        except Exception:
            pass
        async with sessao.get(url, headers=hdrs, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=14)) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            ref  = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)", ref["content"], re.I)
                if m:
                    return await desencurtar(m.group(1).strip().strip("'\""),
                                             sessao, depth + 1)
            mj = re.search(r'window\.location(?:\.href)?\s*=\s*["\']([^"\']+)["\']', html)
            if mj:
                return await desencurtar(mj.group(1), sessao, depth + 1)
            if pos != url:
                return await desencurtar(pos, sessao, depth + 1)
            return pos
    except asyncio.TimeoutError:
        log_lnk.warning(f"⏱ Timeout: {url[:70]}")
        return url
    except Exception as e:
        log_lnk.error(f"❌ Desencurtar {url[:60]}: {e}")
        return url


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 7 ▸ MOTOR AMAZON (COMPLETAMENTE ISOLADO)
#
# REGRAS:
#  • amzn.to / a.co → DESENCURTA primeiro, depois limpa
#  • amazon.com.br → vai direto para limpeza
#  • A tag _AMZ_TAG é injetada SOMENTE em _limpar_url_amazon()
#  • Nunca chama APIs, params ou vars de outras plataformas
# ══════════════════════════════════════════════════════════════════════════════

_AMZ_PARAMS_LIXO = frozenset({
    "ascsubtag","btn_ref","ref_","ref","smid","sprefix","sr","spla",
    "dchild","linkcode","linkid","camp","creative","pf_rd_p","pf_rd_r",
    "pd_rd_wg","pd_rd_w","content-id","pd_rd_r","pd_rd_i","ie","qid",
    "_encoding","dib","dib_tag","m","marketplaceid","ufe",
    "th","psc","ingress","visitid","lp_context_asin","s",
})
_AMZ_PARAMS_MANTER = frozenset({"tag", "keywords", "node", "k", "i", "rh"})


def _limpar_url_amazon(url_bruta: str) -> str:
    """
    Limpa a URL Amazon e injeta a tag de afiliado.
    *** Esta é a ÚNICA função no código que usa _AMZ_TAG ***
    Resultado: https://www.amazon.com.br/dp/ASIN?tag=leo21073-20
    """
    p    = urlparse(url_bruta)
    path = re.sub(r'(/dp/[A-Z0-9]{10})(/.*)?$',         r'\1', p.path)
    path = re.sub(r'(/gp/product/[A-Z0-9]{10})(/.*)?$', r'\1', path)

    params = {}
    for k, v in parse_qs(p.query, keep_blank_values=False).items():
        kl = k.lower()
        if kl in _AMZ_PARAMS_MANTER:
            params[k] = v
        elif kl not in _AMZ_PARAMS_LIXO and len(v[0]) < 30:
            params[k] = v

    params["tag"] = [_AMZ_TAG]  # ← ÚNICA INJEÇÃO DA TAG AMAZON NO CÓDIGO INTEIRO
    return urlunparse(p._replace(path=path, query=urlencode(params, doseq=True)))


async def motor_amazon(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """Motor exclusivo Amazon. Não acessa vars de Shopee nem Magalu."""
    nl = _netloc(url)

    if any(d in nl for d in ("amzn.to", "a.co", "amzn.com")):
        # Encurtado → expande primeiro
        log_amz.debug(f"🔗 Expandindo: {url[:80]}")
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)
        if classificar(exp) != "amazon":
            log_amz.warning(f"  ⚠️ Não-Amazon após expansão: {exp[:60]}")
            return None
        final = _limpar_url_amazon(exp)
    elif any(d in nl for d in ("amazon.com.br", "amazon.com")):
        # URL direta → limpa direto
        log_amz.debug(f"🔗 Direta: {url[:80]}")
        final = _limpar_url_amazon(url)
    else:
        log_amz.warning(f"  ⚠️ URL fora do domínio Amazon: {url[:60]}")
        return None

    log_amz.info(f"  ✅ {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ MOTOR SHOPEE (COMPLETAMENTE ISOLADO)
#
# REGRAS:
#  • Shopee chega pronta — NUNCA chama desencurtar()
#  • API GraphQL direta com retry 3x
#  • Usa SOMENTE _SHP_APP_ID e _SHP_SECRET
#  • Nunca usa vars de Amazon nem Magalu
# ══════════════════════════════════════════════════════════════════════════════

async def motor_shopee(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """Motor exclusivo Shopee. Não acessa vars de Amazon nem Magalu."""
    log_shp.debug(f"🔗 Direta: {url[:80]}")

    # URL chega pronta — sem desencurtamento
    ts      = str(int(time.time()))
    payload = json.dumps(
        {"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{url}" }}) '
                  f'{{ shortLink }} }}'},
        separators=(",", ":")
    )
    sig  = hashlib.sha256(f"{_SHP_APP_ID}{ts}{payload}{_SHP_SECRET}".encode()).hexdigest()
    hdrs = {
        "Authorization": f"SHA256 Credential={_SHP_APP_ID},Timestamp={ts},Signature={sig}",
        "Content-Type": "application/json",
    }
    for t in range(1, 4):
        try:
            async with _SEM_HTTP:
                async with sessao.post(
                    "https://open-api.affiliate.shopee.com.br/graphql",
                    data=payload, headers=hdrs,
                    timeout=aiohttp.ClientTimeout(total=12)) as r:
                    res  = await r.json()
                    link = res["data"]["generateShortLink"]["shortLink"]
                    log_shp.info(f"  ✅ {link}")
                    return link
        except Exception as e:
            log_shp.warning(f"  ⚠️ t={t}/3: {e}")
            await asyncio.sleep(2 ** t)

    log_shp.error("  ❌ API falhou 3x — retornando original")
    return url  # fallback: URL original funciona mesmo sem afiliado


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 9 ▸ MOTOR MAGALU (COMPLETAMENTE ISOLADO)
#
# REGRAS:
#  • SEMPRE desencurta (maga.lu, encurtadores) OU confirma domínio
#  • SEMPRE substitui parâmetros pelos do sistema (nunca preserva externos)
#  • Vitrine/categoria → substitui slug por _MGL_SLUG
#  • Encurta via Cuttly
#  • Usa SOMENTE _MGL_PARTNER, _MGL_PROMOTER, _MGL_PID, _MGL_SLUG, _CUTTLY_KEY
#  • Nunca usa vars de Amazon nem Shopee
# ══════════════════════════════════════════════════════════════════════════════

def _construir_url_magalu(url_base: str) -> str:
    """
    Constrói URL Magalu com TODOS os parâmetros de comissão do sistema.
    SEMPRE sobrescreve — nunca herda parâmetros externos.
    """
    p    = urlparse(url_base)
    path = p.path

    # Vitrine/categoria → substitui slug
    if _eh_vitrine_magalu(url_base):
        path = re.sub(r'(/(?:lojas|magazinevoce)/)[^/]+', rf'\1{_MGL_SLUG}', path)
        log_mgl.debug(f"  Slug vitrine → {path}")

    # URL limpa do produto (sem query)
    base_limpa = urlunparse(p._replace(path=path, query="", fragment=""))

    # Monta deeplink value
    deeplink = (f"{base_limpa}?utm_source=divulgador&utm_medium=magalu"
                f"&partnerid={_MGL_PARTNER}&promoterid={_MGL_PROMOTER}"
                f"&utm_campaign={_MGL_PROMOTER}")

    params = {
        "utm_source":      "divulgador",
        "utm_medium":      "magalu",
        "partnerid":       _MGL_PARTNER,
        "promoterid":      _MGL_PROMOTER,
        "utm_campaign":    _MGL_PROMOTER,
        "afforcedeeplink": "true",
        "isretargeting":   "true",
        "pid":             _MGL_PID,
        "c":               _MGL_PROMOTER,
        "deeplinkvalue":   deeplink,
    }
    return urlunparse(p._replace(path=path, query=urlencode(params), fragment=""))


async def _cuttly(url: str, sessao: aiohttp.ClientSession) -> str:
    """Encurta via Cuttly. Retorna original em caso de falha."""
    api = f"https://cutt.ly/api/api.php?key={_CUTTLY_KEY}&short={quote(url, safe='')}"
    try:
        async with _SEM_HTTP:
            async with sessao.get(api, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data   = await r.json(content_type=None)
                status = data.get("url", {}).get("status")
                if status == 7:
                    short = data["url"]["shortLink"]
                    log_mgl.info(f"  ✂️ Cuttly: {short}")
                    return short
                log_mgl.warning(f"  ⚠️ Cuttly status={status}")
    except Exception as e:
        log_mgl.warning(f"  ⚠️ Cuttly: {e}")
    return url


async def motor_magalu(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """Motor exclusivo Magalu. Não acessa vars de Amazon nem Shopee."""
    log_mgl.debug(f"🔗 {url[:80]}")

    # Etapa 1: Desencurtar quando necessário
    nl = _netloc(url)
    if "maga.lu" in nl or nl in _ENCURTADORES:
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)
        log_mgl.debug(f"  📦 {exp[:80]}")
    else:
        exp = url

    # Etapa 2: Confirmar que é Magalu
    if classificar(exp) != "magalu":
        log_mgl.warning(f"  ⚠️ Não-Magalu após expansão: {exp[:60]}")
        return None

    # Etapa 3: SEMPRE aplica os parâmetros do sistema (nunca preserva externos)
    # Isso garante que parâmetros de outros afiliados nunca chegam ao destino
    url_com_params = _construir_url_magalu(exp)
    log_mgl.debug(f"  🏷 {url_com_params[:80]}")

    # Etapa 4: Encurta via Cuttly
    final = await _cuttly(url_com_params, sessao)
    log_mgl.info(f"  ✅ {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 10 ▸ PIPELINE DE CONVERSÃO — PARALELO COM WHITELIST
# ══════════════════════════════════════════════════════════════════════════════

async def _converter_um(url: str, sessao: aiohttp.ClientSession) -> tuple:
    """
    Converte 1 link. Whitelist verificada antes de qualquer motor.
    Roteamento estrito: plataforma → motor exclusivo daquela plataforma.
    """
    plat = classificar(url)

    if plat == "amazon":
        r = await motor_amazon(url, sessao)
        return (r, "amazon") if r else (None, None)

    if plat == "shopee":
        r = await motor_shopee(url, sessao)
        return (r, "shopee") if r else (None, None)

    if plat == "magalu":
        r = await motor_magalu(url, sessao)
        return (r, "magalu") if r else (None, None)

    if plat == "expandir":
        # Encurtador genérico → expande → reclassifica → roteamento
        log_lnk.debug(f"🔄 {url[:70]}")
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)
        plat2 = classificar(exp)
        if plat2 == "amazon":
            r = await motor_amazon(exp, sessao)
            return (r, "amazon") if r else (None, None)
        if plat2 == "shopee":
            r = await motor_shopee(exp, sessao)
            return (r, "shopee") if r else (None, None)
        if plat2 == "magalu":
            r = await motor_magalu(exp, sessao)
            return (r, "magalu") if r else (None, None)
        log_lnk.info(f"🗑 Expandido mas não na whitelist: {exp[:70]}")
        return None, None

    # None → fora da whitelist → descartado
    log_lnk.debug(f"🗑 Fora whitelist: {url[:70]}")
    return None, None


async def converter_links(links: list) -> tuple:
    """Converte até 50 links em paralelo. Retorna (mapa, plataforma_principal)."""
    log_lnk.info(f"🚀 {len(links)} link(s)")
    conn = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False)
    async with aiohttp.ClientSession(
        connector=conn,
        timeout=aiohttp.ClientTimeout(total=35, connect=8),
        headers={"User-Agent": random.choice(USER_AGENTS)},
    ) as sessao:
        resultados = await asyncio.gather(
            *[_converter_um(l, sessao) for l in links[:50]],
            return_exceptions=True,
        )

    mapa, plats = {}, []
    for i, res in enumerate(resultados):
        if isinstance(res, Exception):
            log_lnk.error(f"  ❌ [{i}]: {res}")
            continue
        novo, plat = res
        if novo and plat:
            mapa[links[i]] = novo
            plats.append(plat)
            log_lnk.debug(f"  [{plat.upper()}] {links[i][:40]} → {novo[:50]}")

    plat_p = max(set(plats), key=plats.count) if plats else "amazon"
    log_lnk.info(f"✅ {len(mapa)}/{len(links)} | plat={plat_p}")
    return mapa, plat_p


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 11 ▸ DEDUPLICAÇÃO INTELIGENTE — BASEADA EM MUDANÇA REAL
#
# CHAVE DE IDENTIFICAÇÃO: plataforma + cupom + campanha/texto_norm
# PREÇO e LINK não são usados como chave principal.
#
# PERMITE REENVIO quando:
#  • cupom mudou (ex: PARAVOCE → PARAVOCE2)
#  • campanha mudou (ex: Amazon APP → Amazon APP Mastercard)
#  • texto mudou significativamente
#
# BLOQUEIA apenas quando:
#  • cupom IGUAL e texto IDÊNTICO (hash exato)
#  • cupom IGUAL e texto muito similar (>= 92%)
#  • contexto semântico igual e texto similar (>= 78%)
#
# SEM bloqueio cego por tempo fixo.
# ══════════════════════════════════════════════════════════════════════════════

_TTL_CACHE   = 120 * 60   # 120 min de memória
_SIM_CUPOM   = 0.92       # cupom igual + texto muito parecido → bloqueia
_SIM_CAMP    = 0.78       # contexto igual + texto parecido → bloqueia

_RUIDO_DEDUP = {
    "promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora",
    "relampago","relâmpago","click","clique","veja","confira","app",
}

def _rm_acentos(t: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', t)
                   if unicodedata.category(c) != 'Mn')

def _normalizar(texto: str) -> str:
    t = _rm_acentos(texto.lower())
    t = re.sub(r"http\S+|www\S+", " ", t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return " ".join(sorted(w for w in t.split() if w not in _RUIDO_DEDUP))

def _extrair_campanha(texto: str) -> str:
    """
    Extrai a campanha/contexto da oferta para usar como chave de dedup.
    Procura por padrões como "Amazon APP", "Mastercard", "Frete Grátis", etc.
    """
    texto_lower = texto.lower()
    campanhas = []
    if "amazon app" in texto_lower or "app amazon" in texto_lower:
        campanhas.append("amazon_app")
    if "mastercard" in texto_lower:
        campanhas.append("mastercard")
    if "frete" in texto_lower and ("grátis" in texto_lower or "gratis" in texto_lower):
        campanhas.append("frete_gratis")
    if "prime" in texto_lower:
        campanhas.append("prime")
    if "black friday" in texto_lower or "blackfriday" in texto_lower:
        campanhas.append("blackfriday")
    return "|".join(sorted(campanhas)) if campanhas else "geral"

def deve_enviar(plat: str, prod: str, cupom: str = "", texto: str = "") -> bool:
    """
    Verifica se a oferta deve ser enviada.
    Chave: plataforma + cupom + campanha.
    Bloqueia só quando realmente idêntica — nunca por tempo fixo.
    """
    cache = ler_cache()
    agora = time.time()
    # Expira entradas antigas
    cache = {k: v for k, v in cache.items() if agora - v.get("ts", 0) < _TTL_CACHE}

    tnorm    = _normalizar(texto)
    cnorm    = cupom.strip().upper()
    campanha = _extrair_campanha(texto)

    # Hash de identificação: plataforma + cupom + campanha + texto
    h = hashlib.sha256(f"{plat}|{cnorm}|{campanha}|{tnorm}".encode()).hexdigest()

    # C1 — hash exato
    if h in cache:
        log_dedup.info(f"🔁 [C1] Idêntico | plat={plat} cupom={cnorm} camp={campanha}")
        return False

    # C2/C3 — similaridade dentro da janela recente (30 min)
    janela = 30 * 60
    for entrada in cache.values():
        if agora - entrada.get("ts", 0) >= janela:
            continue
        if entrada.get("plat") != plat:
            continue  # plataformas diferentes nunca se bloqueiam entre si

        sim        = SequenceMatcher(None, tnorm, entrada.get("txt", "")).ratio()
        c_igual    = entrada.get("cupom", "") == cnorm.lower()
        camp_igual = entrada.get("camp", "") == campanha

        # C2: mesmo cupom + texto quase idêntico → bloqueia
        if c_igual and sim >= _SIM_CUPOM:
            log_dedup.info(f"🔁 [C2] Cupom igual + texto similar | sim={sim:.2f}")
            return False

        # C3: mesma campanha + texto parecido → bloqueia
        if camp_igual and campanha != "geral" and sim >= _SIM_CAMP:
            log_dedup.info(f"🔁 [C3] Campanha igual + similar | camp={campanha} sim={sim:.2f}")
            return False

    # Nova oferta — registra
    cache[h] = {
        "plat": plat, "prod": str(prod),
        "cupom": cnorm.lower(), "camp": campanha,
        "txt": tnorm, "ts": agora,
    }
    salvar_cache(cache)
    log_dedup.debug(f"✅ Nova | plat={plat} cupom={cnorm} camp={campanha}")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 12 ▸ RENDERIZADORES POR PLATAFORMA (COMPLETAMENTE ISOLADOS)
#
# Cada plataforma tem seu próprio renderer.
# Estado, emojis e templates não são compartilhados.
# ══════════════════════════════════════════════════════════════════════════════

_RE_URL      = re.compile(r'https?://[^\s\)\]>,"\'<]+')
_RE_PRECO    = re.compile(r'R\$\s?[\d.,]+')
_RE_CUPOM_KW = re.compile(
    r'\b(?:cupom|cupon|off|resgate|codigo|coupon|desconto|assine)\b', re.I)
_RE_EMOJI    = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF"
    r"\U0001F900-\U0001F9FF\u2B50\u2B55\u231A\u231B\u25A0-\u25FF]",
    flags=re.UNICODE,
)
_RE_COD_CUPOM = re.compile(r'(?<![`"\'])\b([A-Z][A-Z0-9_-]{3,19})\b(?![`"\'"])')

# Marcadores técnicos para limpar
_RE_LIXO_VISUAL = re.compile(
    r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:|[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?)\s*',
    re.I)

def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI.search(s))

def _aplicar_crases(linha: str) -> str:
    """Coloca crases em torno do código do cupom se ainda não tiver."""
    def _sub(m):
        cod, ini, fim = m.group(1), m.start(1), m.end(1)
        s = m.string
        antes  = s[ini - 1] if ini > 0 else ""
        depois = s[fim]     if fim < len(s) else ""
        if antes == "`" or depois == "`":
            return cod
        return f"`{cod}`"
    return _RE_COD_CUPOM.sub(_sub, linha)

def _limpar_visual(linha: str) -> str:
    return _RE_LIXO_VISUAL.sub("", linha).strip()


# ── Emojis FIXOS por tipo de linha (consistentes, não aleatórios) ────────────
#   Amazon usa seu conjunto, Shopee o seu, Magalu o seu.
#   Dentro de cada plataforma, o emoji por linha é determinístico pelo contexto.

_EMOJI_POR_PLAT = {
    "amazon": {"titulo": "🔥", "preco": "✅", "cupom": "🎟", "frete": "🚚",
               "anuncio": "📢", "estoque": "📦", "link": ""},
    "shopee": {"titulo": "🛒", "preco": "💰", "cupom": "🎁", "frete": "🚚",
               "anuncio": "📢", "estoque": "📦", "link": ""},
    "magalu": {"titulo": "🔵", "preco": "✅", "cupom": "🏷", "frete": "🚚",
               "anuncio": "📢", "estoque": "🛍", "link": ""},
}

# Emojis extras por categoria de produto (detectados no título)
_PROD_CAT = [
    (["whey","proteína","proteina","suplemento","creatina","bcaa","colágeno"], "💪"),
    (["tênis","tenis","sapato","sandália","sandalia","sapatênis","chinelo","bota"], "👟"),
    (["álbum","album","figurinha","card","cards","panini","pokemon","sticker"],    "📚"),
    (["celular","smartphone","iphone","galaxy","xiaomi","motorola","redmi"],       "📱"),
    (["notebook","computador","laptop","pc gamer","desktop"],                      "💻"),
    (["smart tv","televisão","tv ","soundbar","projetor","caixa de som","fone"],   "📺"),
    (["shampoo","condicionador","creme","sérum","perfume","hidratante","maquiagem"],"💄"),
    (["cerveja","refrigerante","suco","energético","whisky","vinho"],               "🥤"),
    (["chocolate","biscoito","café","açúcar","arroz","leite","frango","carne"],    "🍫"),
    (["furadeira","parafusadeira","ferramenta","chave","alicate"],                 "🔧"),
    (["game","jogo","ps5","xbox","nintendo","controle","headset gamer"],           "🎮"),
    (["livro","e-book","ebook","mangá","manga","literatura"],                      "📖"),
    (["brinquedo","boneca","lego","pelúcia","carrinho","quebra-cabeça"],           "🧸"),
    (["camiseta","camisa","calça","vestido","moletom","jaqueta","bermuda"],        "👕"),
    (["bebê","bebe","fraldas","mamadeira","chupeta"],                              "👶"),
    (["pet","cachorro","gato","ração","aquário","coleira"],                        "🐾"),
    (["geladeira","fogão","microondas","lavadora","lava-roupas","ar-condicionado"],"🏠"),
]

def _emoji_titulo_produto(titulo: str, plat: str) -> str:
    """Retorna emoji específico para o produto, ou o padrão da plataforma."""
    tl = titulo.lower()
    for palavras, emoji in _PROD_CAT:
        if any(p in tl for p in palavras):
            return emoji
    return _EMOJI_POR_PLAT.get(plat, _EMOJI_POR_PLAT["amazon"])["titulo"]

def _emoji_contexto_linha(linha: str, plat: str) -> Optional[str]:
    """Retorna emoji de contexto para linhas que não são título."""
    ll = linha.lower()
    ep = _EMOJI_POR_PLAT.get(plat, _EMOJI_POR_PLAT["amazon"])

    if any(x in ll for x in ["frete grátis","frete gratis","entrega grátis",
                               "entrega gratis","sem frete","frete free"]):
        return ep["frete"]
    if any(x in ll for x in ["cupom","cupon","código","codigo","off",
                               "resgate","desconto","coupon"]):
        return ep["cupom"]
    if _RE_PRECO.search(linha):
        return ep["preco"]
    if any(x in ll for x in ["anúncio","anuncio","publicidade"]):
        return ep["anuncio"]
    if any(x in ll for x in ["estoque","unidades","disponível","disponivel"]):
        return ep["estoque"]
    return None


def _renderizar(texto_original: str, mapa: dict, plat: str) -> str:
    """
    Renderer isolado por plataforma.
    Estado limpo a cada chamada — nenhuma variável global compartilhada.
    """
    # Estado local (não global)
    primeira_linha_texto = True

    linhas = texto_original.split('\n')
    saida  = []

    for linha in linhas:
        ls = linha.strip()
        if not ls:
            saida.append("")
            continue

        # ── Linha só de link(s) ───────────────────────────────────────────
        urls_raw  = _RE_URL.findall(ls)
        sem_links = _RE_URL.sub("", ls).strip()

        if urls_raw and not sem_links:
            for u in urls_raw:
                uc = u.rstrip('.,;)>')
                if uc in mapa:
                    saida.append(mapa[uc])
                # Link não convertido = fora da whitelist = removido
            continue

        # ── Linha com texto ───────────────────────────────────────────────
        nova = ls

        def _sub(m):
            uc = m.group(0).rstrip('.,;)>')
            return mapa[uc] if uc in mapa else ""

        nova = _RE_URL.sub(_sub, nova).strip()
        if not nova:
            continue

        # Limpa marcadores técnicos
        nova = _limpar_visual(nova)
        if not nova:
            continue

        # Aplica crases no código do cupom
        if _RE_CUPOM_KW.search(nova) or _RE_COD_CUPOM.search(nova):
            nova = _aplicar_crases(nova)

        # Aplica emoji (só onde não há emoji próprio)
        if not _tem_emoji(nova) and not _RE_URL.match(nova):
            if primeira_linha_texto:
                emoji = _emoji_titulo_produto(nova, plat)
                nova  = f"{emoji} {nova}"
                primeira_linha_texto = False
            else:
                ec = _emoji_contexto_linha(nova, plat)
                if ec:
                    nova = f"{ec} {nova}"
        elif _tem_emoji(nova) and not _RE_URL.match(nova):
            if primeira_linha_texto:
                primeira_linha_texto = False

        saida.append(nova)

    # Remove linhas vazias consecutivas
    final, pv = [], False
    for l in saida:
        if l.strip() == "":
            if not pv:
                final.append("")
            pv = True
        else:
            pv = False
            final.append(l)

    return "\n".join(final).strip()


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 13 ▸ IMAGEM — WATERMARK + FALLBACK
# ══════════════════════════════════════════════════════════════════════════════

def _processar_bytes(dados: bytes) -> bytes:
    if not _PIL_OK:
        return dados
    try:
        img = Image.open(io.BytesIO(dados)).convert("RGB")
        w, h = img.size
        mx, my = int(w * 0.05), int(h * 0.05)
        cropped = img.crop((mx, my, w - mx, h - my))
        buf = io.BytesIO()
        cropped.save(buf, format="JPEG", quality=92)
        log_img.debug(f"✅ Crop: {w}x{h}→{cropped.size[0]}x{cropped.size[1]}")
        return buf.getvalue()
    except Exception as e:
        log_img.error(f"❌ Processar: {e}")
        return dados

def _tem_watermark(dados: bytes) -> bool:
    if not _PIL_OK:
        return False
    try:
        img    = Image.open(io.BytesIO(dados)).convert("RGBA")
        w, h   = img.size
        px     = img.load()
        count  = sum(1 for x in range(max(0, w - 80), w)
                     for y in range(max(0, h - 40), h)
                     if px[x, y][3] < 200 and sum(px[x, y][:3]) > 400)
        return count > 50
    except Exception:
        return False

async def _baixar_bytes(url: str) -> Optional[bytes]:
    try:
        async with aiohttp.ClientSession(
            headers={"User-Agent": random.choice(USER_AGENTS)}
        ) as s:
            async with s.get(url, allow_redirects=True,
                              timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status == 200:
                    return await r.read()
    except Exception as e:
        log_img.warning(f"⚠️ Download: {e}")
    return None

async def preparar_imagem(media_ou_url, tem_real: bool):
    if not media_ou_url:
        return None, False
    if tem_real:
        return media_ou_url, False
    if isinstance(media_ou_url, str):
        dados = await _baixar_bytes(media_ou_url)
        if not dados:
            return None, False
        if _tem_watermark(dados):
            dados = _processar_bytes(dados)
        return io.BytesIO(dados), True
    return media_ou_url, False

async def buscar_imagem(url: str) -> Optional[str]:
    for t in range(1, 4):
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
                        log_img.info(f"✅ t={t}: {tag['content'][:70]}")
                        return tag["content"]
        except asyncio.TimeoutError:
            log_img.warning(f"⏱ t={t}")
        except Exception as e:
            log_img.warning(f"⚠️ t={t}: {e}")
        await asyncio.sleep(1.5 * t)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ FINGERPRINT
# ══════════════════════════════════════════════════════════════════════════════

def _prod_id(mapa: dict) -> str:
    if not mapa:
        return "0"
    p = list(mapa.values())[0]
    for pat in [r'/dp/([A-Z0-9]{10})', r'/gp/product/([A-Z0-9]{10})',
                r'/i\.(\d+\.\d+)',     r'/p/(\d+)/',  r'/product/(\d+)']:
        m = re.search(pat, p)
        if m:
            return m.group(1)
    return p[-20:]

def _extrair_preco(t: str) -> str:
    m = _RE_PRECO.search(t)
    return m.group(0).strip() if m else "0"

def _extrair_cupom(t: str) -> str:
    m = re.search(r'\b([A-Z][A-Z0-9_-]{3,19})\b', t)
    return m.group(1) if m else ""

def _eh_cupom(t: str) -> bool:
    return bool(_RE_CUPOM_KW.search(t))

def _tem_midia(media) -> bool:
    return media is not None and not isinstance(media, MessageMediaWebPage)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 15 ▸ RATE-LIMIT INTERNO
# ══════════════════════════════════════════════════════════════════════════════

async def _rate_limit():
    global _ULTIMO_ENVIO_TS
    async with _RATE_LOCK:
        agora  = time.monotonic()
        espera = _INTERVALO_MIN - (agora - _ULTIMO_ENVIO_TS)
        if espera > 0:
            await asyncio.sleep(espera)
        _ULTIMO_ENVIO_TS = time.monotonic()


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 16 ▸ ANTI-LOOP DE EDIÇÃO
# ══════════════════════════════════════════════════════════════════════════════

_IDS_PROC: set = set()
_IDS_LOCK = asyncio.Lock()

async def _marcar(msg_id: int):
    async with _IDS_LOCK:
        _IDS_PROC.add(msg_id)
        if len(_IDS_PROC) > 3000:
            _IDS_PROC.pop()

async def _processado(msg_id: int) -> bool:
    async with _IDS_LOCK:
        return msg_id in _IDS_PROC


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 17 ▸ ENVIO — 1 MENSAGEM SEMPRE
# ══════════════════════════════════════════════════════════════════════════════

async def _enviar(msg: str, img_obj) -> object:
    if img_obj and len(msg) <= 1024:
        try:
            return await client.send_file(
                GRUPO_DESTINO, img_obj, caption=msg, parse_mode="md")
        except Exception as e:
            log_tg.warning(f"⚠️ send_file falhou: {e}")
    return await client.send_message(
        GRUPO_DESTINO, msg, parse_mode="md", link_preview=True)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 18 ▸ PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

async def processar(event, is_edit: bool = False):
    """
    Pipeline completo com contexto limpo a cada execução.
    Estado não é compartilhado entre processamentos.
    """
    msg_id = event.message.id
    texto  = event.message.text or ""
    chat   = await event.get_chat()
    uname  = (chat.username or str(event.chat_id)).lower()

    log_tg.info(f"{'✏️ EDIT' if is_edit else '📩 NEW'} | @{uname} | id={msg_id} | {len(texto)}c")

    # ── E1: Validação básica ──────────────────────────────────────────────
    if not texto.strip():
        return

    # ── E2: Anti-loop ─────────────────────────────────────────────────────
    if not is_edit:
        if await _processado(msg_id):
            log_sys.debug(f"⏩ Anti-loop: id={msg_id}")
            return
    else:
        loop_exec  = asyncio.get_event_loop()
        mapa_check = await loop_exec.run_in_executor(_EXECUTOR, ler_mapa)
        if str(msg_id) not in mapa_check:
            log_sys.debug(f"⏩ Edit ignorada (preview): id={msg_id}")
            return

    # ── E3: Filtro de texto ────────────────────────────────────────────────
    if texto_bloqueado(texto):
        return

    # ── E4: Extrai links ──────────────────────────────────────────────────
    links_raw = [l.rstrip('.,;)>') for l in _RE_URL.findall(texto)]
    log_lnk.info(f"🔗 {len(links_raw)} link(s)")

    if not links_raw and "fadadoscupons" not in uname:
        log_sys.debug("⏩ Sem links")
        return

    # ── E5: Conversão paralela (whitelist + motores isolados) ──────────────
    mapa_links, plat_p = {}, "amazon"
    if links_raw:
        mapa_links, plat_p = await converter_links(links_raw)

    if links_raw and not mapa_links:
        log_sys.warning(f"🚫 Zero links na whitelist | @{uname}")
        return

    # ── E6: Deduplicação inteligente por mudança real ─────────────────────
    prod = _prod_id(mapa_links)
    cup  = _extrair_cupom(texto)
    log_dedup.debug(f"🔬 prod={prod} cupom={cup} plat={plat_p}")

    if not is_edit:
        if not deve_enviar(plat_p, prod, cup, texto):
            return

    # ── E7: Renderização isolada por plataforma ───────────────────────────
    # Estado zerado — _renderizar() não usa variáveis globais de contexto
    msg_final = _renderizar(texto, mapa_links, plat_p)
    log_tg.debug(f"📝 [{plat_p.upper()}]\n{msg_final[:400]}")

    # ── E8: Imagem (isolada por plataforma) ───────────────────────────────
    media_orig = event.message.media
    tem_img    = _tem_midia(media_orig)
    img_obj    = None

    if _eh_cupom(texto):
        if tem_img:
            img_obj, _ = await preparar_imagem(media_orig, True)
        else:
            img_fallback = {
                "amazon": _IMG_AMZ, "shopee": _IMG_SHP, "magalu": _IMG_MGL
            }.get(plat_p, _IMG_AMZ)
            img_obj = img_fallback if os.path.exists(img_fallback) else None
    else:
        if tem_img:
            img_obj, _ = await preparar_imagem(media_orig, True)
        elif mapa_links:
            img_url = await buscar_imagem(list(mapa_links.values())[0])
            if img_url:
                img_obj, _ = await preparar_imagem(img_url, False)

    # ── E9: Rate-limit ────────────────────────────────────────────────────
    await _rate_limit()

    # ── E10: Envio / Edição com retry ────────────────────────────────────
    async with _SEM_ENVIO:
        loop = asyncio.get_event_loop()
        mapa = await loop.run_in_executor(_EXECUTOR, ler_mapa)

        try:
            if is_edit:
                id_dest = mapa[str(msg_id)]
                log_tg.info(f"✏️ Editando id={id_dest}")
                for t in range(1, 4):
                    try:
                        await client.edit_message(GRUPO_DESTINO, id_dest, msg_final)
                        log_tg.info("✅ Edição ok")
                        break
                    except MessageNotModifiedError:
                        break
                    except FloodWaitError as e:
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        log_tg.error(f"❌ t={t}: {e}")
                        if t < 3:
                            await asyncio.sleep(2 ** t)
                return

            sent = None
            for t in range(1, 4):
                try:
                    sent = await _enviar(msg_final, img_obj)
                    break
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    log_tg.error(f"❌ Envio t={t}: {e}")
                    if t == 1:
                        img_obj = None
                    elif t < 3:
                        await asyncio.sleep(2 ** t)

            if sent:
                mapa[str(msg_id)] = sent.id
                await loop.run_in_executor(_EXECUTOR, salvar_mapa, mapa)
                await _marcar(msg_id)
                log_sys.info(
                    f"🚀 [OK] @{uname} → {GRUPO_DESTINO} | "
                    f"origem={msg_id} destino={sent.id} | {plat_p.upper()}")

        except Exception as e:
            log_sys.error(f"❌ CRÍTICO: {e}", exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 19 ▸ HEALTH CHECK (a cada 5 min)
# ══════════════════════════════════════════════════════════════════════════════

async def _health_check():
    while True:
        await asyncio.sleep(300)
        try:
            log_hc.info(
                f"💚 cache={len(ler_cache())} | mapa={len(ler_mapa())} | "
                f"anti-loop={len(_IDS_PROC)} | PIL={'OK' if _PIL_OK else 'OFF'}")
        except Exception as e:
            log_hc.error(f"❌ {e}")


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 20 ▸ INICIALIZAÇÃO COM AUTO-RESTART
# ══════════════════════════════════════════════════════════════════════════════

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

async def _run():
    log_sys.info("🔌 Conectando...")
    await client.connect()
    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida! Verifique TELEGRAM_SESSION no Railway.")
        return False

    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 Grupos: {GRUPOS_ORIGEM}")
    log_sys.info(f"📣 Destino: {GRUPO_DESTINO}")
    log_sys.info(f"🟠 Amazon tag: {_AMZ_TAG}")
    log_sys.info(f"🟣 Shopee app: {_SHP_APP_ID}")
    log_sys.info(f"🔵 Magalu promo: {_MGL_PROMOTER} | slug: {_MGL_SLUG}")
    log_sys.info(f"🖼  Pillow: {'OK' if _PIL_OK else 'NÃO (pip install Pillow)'}")
    log_sys.info("🚀 FOGUETÃO v68.0 ONLINE — ARQUITETURA ISOLADA POR PLATAFORMA!")

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
    await client.run_until_disconnected()
    return True

async def main():
    while True:
        try:
            await _run()
        except (AuthKeyUnregisteredError, SessionPasswordNeededError) as e:
            log_sys.error(f"❌ Auth — encerrando: {e}")
            break
        except Exception as e:
            log_sys.error(f"💥 Caiu: {e} — restart em 15s...", exc_info=True)
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())

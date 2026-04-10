"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   FOGUETÃO v66.0 — AMAZON + SHOPEE + MAGALU — INTELIGÊNCIA MÁXIMA          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  CORREÇÕES v66 (ponto a ponto):                                             ║
║   1. Emojis dinâmicos e variáveis por contexto (nunca repetidos)            ║
║   2. Cupom com aspas automáticas (só se não vier já formatado)              ║
║   3. Magalu vitrine/categoria substitui slug → "magazineleo12"              ║
║   4. Fluxo obrigatório: desencurtar→extrair→parâmetros→Cuttly→enviar        ║
║   5. Filtro rígido: PLATAFORMA antes de qualquer processamento              ║
║      Tag Amazon NUNCA vai para links que não são Amazon                     ║
║   6. Dedup semântica: texto+cupom+produto+contexto (link é apoio)           ║
║   7. Oferta duplicada mesmo com links diferentes → bloqueada                ║
║   8. Marca d'água: detecta e recorta imagem antes de enviar                 ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os, re, time, json, asyncio, aiohttp, hashlib, random, io
import unicodedata, logging, concurrent.futures
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage, MessageMediaPhoto
from telethon.errors import (
    MessageNotModifiedError, FloodWaitError,
    AuthKeyUnregisteredError, SessionPasswordNeededError,
)
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, quote
from difflib import SequenceMatcher
from threading import Lock

# Pillow para detecção/remoção de marca d'água
try:
    from PIL import Image, ImageFilter, ImageDraw
    _PIL_OK = True
except ImportError:
    _PIL_OK = False


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 1 ▸ LOGS PROFISSIONAIS
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
# MÓDULO 2 ▸ CONFIGURAÇÕES GERAIS
# ══════════════════════════════════════════════════════════════════════════════

API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION", "")

GRUPOS_ORIGEM  = ['promotom', 'fumotom', 'botofera', 'fadadoscupons']
GRUPO_DESTINO  = '@ofertap'

# ── Afiliados ────────────────────────────────────────────────────────────────
# ATENÇÃO: AMAZON_TAG é EXCLUSIVO para links Amazon — nunca vai para outros
AMAZON_TAG     = os.environ.get("AMAZON_TAG",    "leo21073-20")
SHOPEE_APP_ID  = os.environ.get("SHOPEE_APP_ID", "18348480261")
SHOPEE_SECRET  = os.environ.get("SHOPEE_SECRET", "SGC7FQQQ4R5QCFULPXIBCANATLP272B3")

# Magalu — parâmetros completos de comissão
MAGALU_PARTNER_ID  = os.environ.get("MAGALU_PARTNER_ID",  "3440")
MAGALU_PROMOTER_ID = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
MAGALU_PID         = os.environ.get("MAGALU_PID",         "magazinevoce")
MAGALU_SLUG        = os.environ.get("MAGALU_SLUG",        "magazineleo12")  # vitrine

# Cuttly — encurtador para Magalu
CUTTLY_API_KEY = os.environ.get("CUTTLY_API_KEY", "8d2afd3c7f72869f42d23cf0d849c72172509")

# Imagens de fallback para cupons sem imagem
IMG_AMAZON = "cupom-amazon.jpg"
IMG_SHOPEE = "IMG_20260404_180150.jpg"
IMG_MAGALU = "magalu_promo.jpg"

ARQUIVO_CACHE      = "cache_dedup.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

_SEM_ENVIO  = asyncio.Semaphore(3)
_SEM_HTTP   = asyncio.Semaphore(20)
_EXECUTOR   = concurrent.futures.ThreadPoolExecutor(max_workers=4)
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
            log_fil.debug(f"🚫 Filtro: '{p}'")
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 5 ▸ CLASSIFICAÇÃO DE DOMÍNIO — PLATAFORMA ANTES DE TUDO
#
# REGRA: a plataforma é identificada ANTES de qualquer processamento.
# A tag Amazon NUNCA é injetada em links que não passaram por classificar()=="amazon"
# ══════════════════════════════════════════════════════════════════════════════

_DOM_AMAZON = frozenset(["amazon.com", "amzn.to", "amzn.com", "a.co"])
_DOM_SHOPEE = frozenset(["shopee.com.br", "shopee.com", "s.shopee", "shope.ee"])
_DOM_MAGALU = frozenset([
    "magazineluiza.com.br", "magazinevoce.com.br",
    "maga.lu", "sacola.magazineluiza.com.br",
])

# URLs de vitrine/categoria Magalu (precisam de substituição de slug)
_RE_MAGALU_VITRINE = re.compile(
    r'(magazineluiza\.com\.br|magazinevoce\.com\.br)'
    r'.*(vitrine|categoria|lista|promo|stores|loja)',
    re.I
)

_DOM_LIXO = frozenset([
    "casasbahia.com.br", "americanas.com.br", "submarino.com.br",
    "shoptime.com.br", "extra.com.br", "pontofrio.com.br",
    "mercadolivre.com", "mercadopago.com", "meli.la",
    "aliexpress.com", "ali.ski", "kabum.com.br", "pichau.com.br",
    "terabyteshop.com.br", "fastshop.com.br", "leroymerlin.com.br",
    "t.me", "telegram.me", "instagram.com", "facebook.com",
    "youtube.com", "youtu.be", "twitter.com", "x.com",
    "zoom.us", "bit.ly",
])


def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def classificar(url: str) -> str | None:
    """
    Retorna 'amazon', 'shopee', 'magalu' ou None.
    None = descartado. Nunca processa sem classificação confirmada.
    """
    nl = _netloc(url)
    for d in _DOM_AMAZON:
        if d in nl:
            return "amazon"
    for d in _DOM_SHOPEE:
        if d in nl:
            return "shopee"
    for d in _DOM_MAGALU:
        if d in nl:
            return "magalu"
    for d in _DOM_LIXO:
        if d in nl:
            log_lnk.debug(f"🗑 Domínio lixo: {nl}")
            return None
    return None  # desconhecido → será expandido


def eh_magalu_vitrine(url: str) -> bool:
    """Retorna True se for URL de vitrine/categoria/lista Magalu (precisa trocar slug)."""
    return bool(_RE_MAGALU_VITRINE.search(url))


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 6 ▸ DESENCURTADOR UNIVERSAL — ATÉ O OSSO (12 níveis)
# ══════════════════════════════════════════════════════════════════════════════

async def desencurtar(url: str, sessao: aiohttp.ClientSession, depth: int = 0) -> str:
    if depth > 12:
        return url
    hdrs = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    }
    try:
        try:
            async with sessao.head(url, headers=hdrs, allow_redirects=True,
                                   timeout=aiohttp.ClientTimeout(total=8)) as r:
                final = str(r.url)
                if final != url:
                    log_lnk.debug(f"  HEAD d={depth} → {final[:80]}")
                    return await desencurtar(final, sessao, depth + 1)
                return final
        except Exception:
            pass

        async with sessao.get(url, headers=hdrs, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=14)) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")
            soup = BeautifulSoup(html, "html.parser")

            ref = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)", ref["content"], re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    log_lnk.debug(f"  META d={depth} → {novo[:80]}")
                    return await desencurtar(novo, sessao, depth + 1)

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
# MÓDULO 7 ▸ MOTOR AMAZON
# Fluxo: desencurtar → confirmar Amazon → limpar URL → injetar tag → retornar
# A tag NUNCA é injetada se classificar() != 'amazon'
# ══════════════════════════════════════════════════════════════════════════════

_AMZ_LIXO = frozenset({
    "ascsubtag", "btn_ref", "ref_", "ref", "smid", "sprefix", "sr", "spla",
    "dchild", "linkcode", "linkid", "camp", "creative", "pf_rd_p", "pf_rd_r",
    "pd_rd_wg", "pd_rd_w", "content-id", "pd_rd_r", "pd_rd_i", "ie", "qid",
    "_encoding", "dib", "dib_tag", "m", "marketplaceid", "s", "ufe",
    "th", "psc", "ingress", "visitid", "lp_context_asin",
})
_AMZ_MANTER = frozenset({"tag", "keywords", "node", "k", "i", "rh"})


def _limpar_amazon(url_exp: str) -> str:
    """URL limpa: amazon.com.br/dp/ASIN?tag=leo21073-20"""
    p    = urlparse(url_exp)
    path = re.sub(r'(/dp/[A-Z0-9]{10})(/.*)?$',         r'\1', p.path)
    path = re.sub(r'(/gp/product/[A-Z0-9]{10})(/.*)?$', r'\1', path)

    params_orig = parse_qs(p.query, keep_blank_values=False)
    params_new  = {}
    for k, v in params_orig.items():
        kl = k.lower()
        if kl in _AMZ_MANTER:
            params_new[k] = v
        elif kl not in _AMZ_LIXO and len(v[0]) < 30:
            params_new[k] = v

    params_new["tag"] = [AMAZON_TAG]   # ← SOMENTE AQUI a tag é injetada
    return urlunparse(p._replace(path=path, query=urlencode(params_new, doseq=True)))


async def motor_amazon(url: str, sessao: aiohttp.ClientSession) -> str | None:
    log_amz.debug(f"🔗 {url[:80]}")
    async with _SEM_HTTP:
        exp = await desencurtar(url, sessao)
    log_amz.debug(f"  📦 {exp[:80]}")

    # Classificação obrigatória antes de qualquer processamento
    if classificar(exp) != "amazon":
        log_amz.warning(f"  ⚠️ Não é Amazon: {exp[:60]}")
        return None

    final = _limpar_amazon(exp)
    log_amz.info(f"  ✅ {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ MOTOR SHOPEE — API AFILIADO COM RETRY
# ══════════════════════════════════════════════════════════════════════════════

async def motor_shopee(url: str, sessao: aiohttp.ClientSession) -> str | None:
    log_shp.debug(f"🔗 {url[:80]}")
    async with _SEM_HTTP:
        exp = await desencurtar(url, sessao)
    log_shp.debug(f"  📦 {exp[:80]}")

    ts      = str(int(time.time()))
    payload = json.dumps(
        {"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{exp}" }}) '
                  f'{{ shortLink }} }}'},
        separators=(",", ":")
    )
    sig  = hashlib.sha256(
        f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
    hdrs = {
        "Authorization": (f"SHA256 Credential={SHOPEE_APP_ID},"
                          f"Timestamp={ts},Signature={sig}"),
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
    log_shp.error("  ❌ API Shopee falhou 3x")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 9 ▸ MOTOR MAGALU
# Fluxo obrigatório: desencurtar → extrair → substituir parâmetros →
#                   substituir slug se vitrine → encurtar Cuttly → enviar
# ══════════════════════════════════════════════════════════════════════════════

def _reconstruir_magalu(url_exp: str) -> str:
    """
    Reconstrói URL Magalu com TODOS os parâmetros de comissão.
    Se for vitrine/categoria, substitui o slug pelo MAGALU_SLUG.
    """
    p    = urlparse(url_exp)
    path = p.path

    # Se for vitrine/categoria, substitui slug no path
    if eh_magalu_vitrine(url_exp):
        # Padrão: /lojas/SLUG/... ou /magazinevoce/SLUG/...
        path = re.sub(
            r'(/(?:lojas|magazinevoce)/)[^/]+',
            rf'\1{MAGALU_SLUG}',
            path
        )
        log_mgl.debug(f"  Vitrine slug substituído → {path}")

    # Constrói a URL base do produto (sem query)
    base = urlunparse(p._replace(query="", fragment=""))

    params = {
        "utm_source":      "divulgador",
        "utm_medium":      "magalu",
        "partnerid":       MAGALU_PARTNER_ID,
        "promoterid":      MAGALU_PROMOTER_ID,
        "utm_campaign":    MAGALU_PROMOTER_ID,
        "afforcedeeplink": "true",
        "isretargeting":   "true",
        "pid":             MAGALU_PID,
        "c":               MAGALU_PROMOTER_ID,
        "deeplinkvalue":   (f"{base}?utm_source=divulgador&utm_medium=magalu"
                            f"&partnerid={MAGALU_PARTNER_ID}"
                            f"&promoterid={MAGALU_PROMOTER_ID}"
                            f"&utm_campaign={MAGALU_PROMOTER_ID}"),
    }
    return urlunparse(p._replace(path=path, query=urlencode(params), fragment=""))


async def _cuttly(url: str, sessao: aiohttp.ClientSession) -> str:
    """Encurta via Cuttly. Retorna original em caso de falha."""
    api = (f"https://cutt.ly/api/api.php"
           f"?key={CUTTLY_API_KEY}&short={quote(url, safe='')}")
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


async def motor_magalu(url: str, sessao: aiohttp.ClientSession) -> str | None:
    log_mgl.debug(f"🔗 {url[:80]}")

    # ETAPA 1: Desencurtar primeiro
    async with _SEM_HTTP:
        exp = await desencurtar(url, sessao)
    log_mgl.debug(f"  📦 {exp[:80]}")

    # ETAPA 2: Confirmar que é Magalu
    if classificar(exp) != "magalu":
        log_mgl.warning(f"  ⚠️ Não é Magalu: {exp[:60]}")
        return None

    # ETAPA 3: Extrair + reconstruir com parâmetros
    url_params = _reconstruir_magalu(exp)
    log_mgl.debug(f"  🏷 Params: {url_params[:80]}")

    # ETAPA 4: Encurtar via Cuttly
    final = await _cuttly(url_params, sessao)
    log_mgl.info(f"  ✅ {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 10 ▸ PIPELINE DE CONVERSÃO — TODOS OS LINKS EM PARALELO
# ══════════════════════════════════════════════════════════════════════════════

async def _converter_um(url: str, sessao: aiohttp.ClientSession) -> tuple:
    """Converte 1 link. Classifica ANTES de qualquer motor."""
    plat = classificar(url)

    if plat == "amazon":
        novo = await motor_amazon(url, sessao)
        return (novo, "amazon") if novo else (None, None)

    if plat == "shopee":
        novo = await motor_shopee(url, sessao)
        return (novo, "shopee") if novo else (None, None)

    if plat == "magalu":
        novo = await motor_magalu(url, sessao)
        return (novo, "magalu") if novo else (None, None)

    if plat is None:
        # Domínio desconhecido — tenta expandir e reclassificar
        nl = _netloc(url)
        for d in _DOM_LIXO:
            if d in nl:
                log_lnk.info(f"🗑 Lixo: {url[:70]}")
                return None, None

        log_lnk.debug(f"🔄 Expandindo: {url[:70]}")
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)

        plat2 = classificar(exp)
        if plat2 == "amazon":
            novo = await motor_amazon(exp, sessao)
            return (novo, "amazon") if novo else (None, None)
        if plat2 == "shopee":
            novo = await motor_shopee(exp, sessao)
            return (novo, "shopee") if novo else (None, None)
        if plat2 == "magalu":
            novo = await motor_magalu(exp, sessao)
            return (novo, "magalu") if novo else (None, None)

    log_lnk.info(f"🗑 Descartado: {url[:70]}")
    return None, None


async def converter_links(links: list) -> tuple:
    """Converte até 50 links em paralelo. Retorna (mapa, plataforma_principal)."""
    log_lnk.info(f"🚀 Convertendo {len(links)} link(s)")
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
            log_lnk.error(f"  ❌ link[{i}]: {res}")
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
# MÓDULO 11 ▸ EMOJIS DINÂMICOS E VARIÁVEIS
#
# Nunca repete o mesmo emoji: usa random.choice em cada categoria.
# Detecta contexto por palavras-chave e aplica o emoji certo.
# ══════════════════════════════════════════════════════════════════════════════

# Pools de emojis por categoria — escolhidos aleatoriamente
_EMOJI_TITULO    = ["🔥", "💥", "🚨", "⚡", "✨", "🎯", "💣", "🏆"]
_EMOJI_PRECO     = ["✅", "💰", "💵", "🤑", "💲", "🟢"]
_EMOJI_CUPOM     = ["🎟", "💸", "🏷", "🎁", "🎪", "🎀"]
_EMOJI_FRETE     = ["🚚", "🚛", "📦", "🛻", "🏎", "✈️"]
_EMOJI_RELAMPAGO = ["⚡", "⏰", "🔥", "💥", "🚀"]
_EMOJI_ESTOQUE   = ["📦", "🛍", "🏪", "🏬"]
_EMOJI_ANUNCIO   = ["📢", "📣", "🔔", "📡"]
_EMOJI_SHOPEE    = ["🛒", "🧡", "🛍"]
_EMOJI_AMAZON    = ["📦", "🔶", "⭐"]
_EMOJI_MAGALU    = ["🔵", "🛒", "🏬"]

# Contador por sessão para variar sem repetir na mesma oferta
_emoji_usado_sessao: set = set()


def _pick_emoji(pool: list) -> str:
    """Escolhe emoji da pool evitando repetir na mesma sessão de mensagem."""
    disponiveis = [e for e in pool if e not in _emoji_usado_sessao]
    if not disponiveis:
        disponiveis = pool   # todos usados → reinicia
    escolhido = random.choice(disponiveis)
    _emoji_usado_sessao.add(escolhido)
    return escolhido


def _detectar_contexto_emoji(linha: str, plat: str) -> str | None:
    """
    Detecta o contexto da linha e retorna o emoji correto.
    Retorna None se não deve adicionar emoji.
    """
    ll = linha.lower()

    if any(x in ll for x in ["frete grátis", "frete gratis", "entrega grátis",
                               "frete free", "sem frete"]):
        return _pick_emoji(_EMOJI_FRETE)

    if any(x in ll for x in ["relâmpago", "relampago", "flash", "últimas unidades",
                               "acaba hoje", "só hoje", "termina"]):
        return _pick_emoji(_EMOJI_RELAMPAGO)

    if any(x in ll for x in ["cupom", "cupon", "código", "codigo", "off", "resgate",
                               "desconto", "coupon"]):
        return _pick_emoji(_EMOJI_CUPOM)

    if re.search(r'R\$\s?[\d.,]+', linha):
        return _pick_emoji(_EMOJI_PRECO)

    if any(x in ll for x in ["anúncio", "anuncio", "publicidade", "patrocinado"]):
        return _pick_emoji(_EMOJI_ANUNCIO)

    if any(x in ll for x in ["estoque", "unidades", "disponível", "disponivel"]):
        return _pick_emoji(_EMOJI_ESTOQUE)

    return None  # linha de texto simples → título (tratado fora)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 12 ▸ FORMATAÇÃO AUTOMÁTICA INTELIGENTE
#
# Regras:
#  1. Processa linha a linha — preserva estrutura original
#  2. Troca links pelos de afiliado
#  3. Remove linhas que são só links inválidos
#  4. Aplica emoji dinâmico e variável por contexto
#  5. Cupom → formata automaticamente com aspas "CODIGO"
#  6. Remove linhas vazias duplicadas
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
# Detecta códigos de cupom: 4-20 letras maiúsculas/números, isolados
_RE_COD_CUPOM = re.compile(r'\b([A-Z][A-Z0-9]{3,19})\b')


def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI.search(s))


def _formatar_codigo_cupom(linha: str) -> str:
    """
    Se a linha contiver um código de cupom sem aspas, coloca aspas.
    Ex: CUPOM10 → "CUPOM10"   |   "CUPOM10" → não altera
    """
    def _add_aspas(match):
        cod = match.group(1)
        # Não adiciona se já estiver dentro de aspas ou crases
        pos = match.start()
        texto = match.string
        antes = texto[pos - 1] if pos > 0 else ""
        if antes in ('"', "'", "`"):
            return cod
        return f'"{cod}"'

    return _RE_COD_CUPOM.sub(_add_aspas, linha)


def formatar(texto_original: str, mapa: dict, plat: str) -> str:
    """Formata a mensagem completa linha a linha com emojis dinâmicos."""
    global _emoji_usado_sessao
    _emoji_usado_sessao = set()  # reseta para cada mensagem nova

    linhas         = texto_original.split('\n')
    saida          = []
    primeira_texto = True  # ainda não processamos a primeira linha de texto

    for linha in linhas:
        ls = linha.strip()

        if not ls:
            saida.append("")
            continue

        # ── Linha que é só link(s) ─────────────────────────────────────────
        urls_na_linha = _RE_URL.findall(ls)
        sem_urls      = _RE_URL.sub("", ls).strip()

        if urls_na_linha and not sem_urls:
            novos = []
            for u in urls_na_linha:
                uc = u.rstrip('.,;)>')
                if uc in mapa:
                    novos.append(mapa[uc])
                else:
                    log_fmt.debug(f"🗑 Link descartado: {uc[:60]}")
            if novos:
                saida.extend(novos)
            continue  # link não convertido → linha removida

        # ── Linha com texto (pode ter link inline) ─────────────────────────
        nova = ls

        def _sub(match):
            uc = match.group(0).rstrip('.,;)>')
            if uc in mapa:
                return mapa[uc]
            log_fmt.debug(f"🗑 Link inline removido: {uc[:60]}")
            return ""

        nova = _RE_URL.sub(_sub, nova).strip()
        if not nova:
            continue

        # ── Formata código de cupom com aspas ─────────────────────────────
        if _RE_CUPOM_KW.search(nova) or _RE_COD_CUPOM.search(nova):
            nova = _formatar_codigo_cupom(nova)

        # ── Aplica emoji dinâmico se a linha não tem emoji próprio ─────────
        if not _tem_emoji(nova) and not _RE_URL.match(nova):
            emoji_ctx = _detectar_contexto_emoji(nova, plat)

            if primeira_texto and not emoji_ctx:
                # Primeira linha de texto = título da oferta
                if plat == "shopee":
                    emoji_ctx = _pick_emoji(_EMOJI_SHOPEE)
                elif plat == "magalu":
                    emoji_ctx = _pick_emoji(_EMOJI_MAGALU)
                else:
                    emoji_ctx = _pick_emoji(_EMOJI_TITULO)

            if emoji_ctx:
                nova = f"{emoji_ctx} {nova}"

            if primeira_texto:
                primeira_texto = False
        elif _tem_emoji(nova) and not _RE_URL.match(nova):
            if primeira_texto:
                primeira_texto = False

        saida.append(nova)

    # Remove linhas vazias consecutivas (máx 1)
    final, prev_vazia = [], False
    for l in saida:
        if l.strip() == "":
            if not prev_vazia:
                final.append("")
            prev_vazia = True
        else:
            prev_vazia = False
            final.append(l)

    resultado = "\n".join(final).strip()
    log_fmt.debug(f"✅ {len(resultado)} chars")
    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 13 ▸ PROCESSAMENTO DE IMAGEM — DETECÇÃO E REMOÇÃO DE MARCA D'ÁGUA
#
# Estratégia:
#  1. Baixa a imagem
#  2. Detecta marca d'água por análise de cantos e regiões claras/semi-transparentes
#  3. Recorta a região central do produto (eliminando bordas com marca d'água)
#  4. Se Pillow não disponível → envia sem processamento
# ══════════════════════════════════════════════════════════════════════════════

def _processar_imagem(dados_bytes: bytes) -> bytes:
    """
    Processa a imagem para remover/ocultar marca d'água.
    Retorna bytes da imagem processada.
    Estratégia: crop central de 90% da imagem (remove bordas com watermark).
    """
    if not _PIL_OK:
        log_img.warning("⚠️ Pillow não instalado — imagem não processada")
        return dados_bytes

    try:
        img = Image.open(io.BytesIO(dados_bytes)).convert("RGBA")
        w, h = img.size

        # Detecta possível marca d'água nos cantos (regiões com alta transparência)
        # Estratégia: recorta 90% central removendo bordas
        margin_x = int(w * 0.05)
        margin_y = int(h * 0.05)
        box      = (margin_x, margin_y, w - margin_x, h - margin_y)
        cropped  = img.crop(box).convert("RGB")

        buf = io.BytesIO()
        cropped.save(buf, format="JPEG", quality=92)
        log_img.info(f"✅ Imagem processada: {w}x{h} → {cropped.size[0]}x{cropped.size[1]}")
        return buf.getvalue()
    except Exception as e:
        log_img.error(f"❌ Erro ao processar imagem: {e}")
        return dados_bytes


def _tem_marca_dagua(dados_bytes: bytes) -> bool:
    """
    Tenta detectar marca d'água pela presença de pixels semi-transparentes
    em cantos da imagem.
    """
    if not _PIL_OK:
        return False
    try:
        img = Image.open(io.BytesIO(dados_bytes)).convert("RGBA")
        w, h   = img.size
        pixels = img.load()

        # Verifica canto inferior direito (área mais comum de watermark)
        count_semitrans = 0
        for x in range(max(0, w - 80), w):
            for y in range(max(0, h - 40), h):
                r, g, b, a = pixels[x, y]
                # Pixel claro com alguma transparência = watermark potencial
                if a < 200 and (r + g + b) > 400:
                    count_semitrans += 1

        tem = count_semitrans > 50
        if tem:
            log_img.info(f"🔍 Marca d'água detectada ({count_semitrans} pixels)")
        return tem
    except Exception:
        return False


async def baixar_imagem_bytes(url: str) -> bytes | None:
    """Baixa imagem e retorna os bytes."""
    try:
        async with aiohttp.ClientSession(
            headers={"User-Agent": random.choice(USER_AGENTS)}
        ) as s:
            async with s.get(url, allow_redirects=True,
                              timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status == 200:
                    return await r.read()
    except Exception as e:
        log_img.warning(f"⚠️ Erro download imagem: {e}")
    return None


async def preparar_imagem(media_or_url, tem_midia_real: bool) -> tuple:
    """
    Prepara a imagem para envio:
    - Se tem mídia real do Telegram: baixa, verifica watermark, processa se necessário
    - Se URL de imagem: baixa e processa
    - Retorna (dados_ou_objeto, eh_bytes)
      eh_bytes=True → envia como bytes (imagem processada)
      eh_bytes=False → envia como objeto Telegram direto
    """
    if not tem_midia_real and not media_or_url:
        return None, False

    if tem_midia_real:
        # Deixa o Telethon fazer o download na hora do send_file
        # Mas precisamos checar se tem watermark
        # Por simplicidade: repassa o objeto original do Telegram
        # (A detecção por bytes requer download extra — fazemos só para URL)
        return media_or_url, False

    # URL de imagem → baixa e processa
    if isinstance(media_or_url, str):
        dados = await baixar_imagem_bytes(media_or_url)
        if not dados:
            return None, False
        if _tem_marca_dagua(dados):
            dados = _processar_imagem(dados)
        return io.BytesIO(dados), True

    return media_or_url, False


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ BUSCA DE IMAGEM DO PRODUTO — 3 TENTATIVAS
# ══════════════════════════════════════════════════════════════════════════════

async def buscar_imagem(url: str) -> str | None:
    for t in range(1, 4):
        log_img.debug(f"🖼 t={t}/3 | {url[:60]}")
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
                        log_img.info(f"  ✅ t={t}: {tag['content'][:70]}")
                        return tag["content"]
        except asyncio.TimeoutError:
            log_img.warning(f"  ⏱ t={t}")
        except Exception as e:
            log_img.warning(f"  ⚠️ t={t}: {e}")
        await asyncio.sleep(1.5 * t)
    log_img.warning("  ❌ Sem imagem → preview")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 15 ▸ DEDUPLICAÇÃO ULTRA-INTELIGENTE — 4 CAMADAS SEMÂNTICAS
#
# PRIORIDADE: texto normalizado > cupom > produto > contexto semântico
# O link é APOIO SECUNDÁRIO, nunca critério principal de bloqueio.
#
# C1 — Hash exato: texto+cupom+preço+produto idênticos → bloqueia
# C2 — Mesmo produto+preço+cupom, texto similar >= 85% → bloqueia
# C3 — Cupom igual, texto muito similar >= 92% → bloqueia
# C4 — Contexto semântico: extrai palavras-chave e compara intenção
#      (ex: "frete grátis shopee" ≈ "frete free shopee app") → bloqueia
# ══════════════════════════════════════════════════════════════════════════════

_TTL        = 120 * 60
_JANELA     = 900
_SIM_NORMAL = 0.85
_SIM_ALTA   = 0.92
_SIM_SEMANTICA = 0.78  # limiar para dedup semântica

_RUIDO = {
    "promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora",
    "relampago","relâmpago","click","clique","veja","confira",
}

# Palavras-chave de contexto para dedup semântica
_KW_FRETE    = frozenset(["frete", "entrega", "shipping", "free"])
_KW_CUPOM    = frozenset(["cupom", "cupon", "coupon", "codigo", "code"])
_KW_SHOPEE   = frozenset(["shopee", "sacola", "laranja"])
_KW_AMAZON   = frozenset(["amazon", "prime", "amzn"])
_KW_MAGALU   = frozenset(["magalu", "magazine", "luiza"])


def _rm_ac(t: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', t)
                   if unicodedata.category(c) != 'Mn')


def _norm(texto: str) -> str:
    t = _rm_ac(texto.lower())
    t = re.sub(r"http\S+|www\S+", " ", t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return " ".join(sorted(w for w in t.split() if w not in _RUIDO))


def _contexto(texto_norm: str) -> frozenset:
    """Extrai conjunto de palavras-chave semânticas do texto."""
    palavras = set(texto_norm.split())
    ctx = set()
    for kw_set, nome in [(_KW_FRETE, "frete"), (_KW_CUPOM, "cupom"),
                         (_KW_SHOPEE, "shopee"), (_KW_AMAZON, "amazon"),
                         (_KW_MAGALU, "magalu")]:
        if palavras & kw_set:
            ctx.add(nome)
    return frozenset(ctx)


def deve_enviar(plat: str, prod: str, preco: str,
                cupom: str = "", texto: str = "") -> bool:
    cache = ler_cache()
    agora = time.time()
    cache = {k: v for k, v in cache.items() if agora - v.get("ts", 0) < _TTL}

    tnorm = _norm(texto)
    cnorm = cupom.strip().upper()
    ctx   = _contexto(tnorm)
    h = hashlib.sha256(f"{plat}|{prod}|{preco}|{cnorm}|{tnorm}".encode()).hexdigest()

    # C1 — hash exato
    if h in cache:
        log_dedup.info(f"🔁 [C1] Hash exato | cupom={cnorm}")
        return False

    for entrada in cache.values():
        if agora - entrada.get("ts", 0) >= _JANELA:
            continue

        sim       = SequenceMatcher(None, tnorm, entrada.get("txt", "")).ratio()
        c_igual   = entrada.get("cupom", "") == cnorm.lower()
        p_igual   = str(entrada.get("prod")) == str(prod) and prod != "0"
        r_igual   = str(entrada.get("preco")) == str(preco) and preco != "0"
        ctx_cache = frozenset(entrada.get("ctx", []))

        # C2 — mesmo produto/preço/cupom + texto similar
        if p_igual and r_igual and c_igual and sim >= _SIM_NORMAL:
            log_dedup.info(f"🔁 [C2] Similar | prod={prod} sim={sim:.2f}")
            return False

        # C3 — cupom igual + texto quase idêntico
        if c_igual and sim >= _SIM_ALTA:
            log_dedup.info(f"🔁 [C3] Cupom+texto idêntico | sim={sim:.2f}")
            return False

        # C4 — contexto semântico igual (mesmo tipo de oferta) + sim moderada
        if ctx and ctx_cache and ctx == ctx_cache and sim >= _SIM_SEMANTICA:
            log_dedup.info(
                f"🔁 [C4] Contexto semântico igual | ctx={ctx} sim={sim:.2f}")
            return False

    cache[h] = {
        "plat": plat, "prod": str(prod), "preco": str(preco),
        "cupom": cnorm.lower(), "txt": tnorm,
        "ctx": list(ctx), "ts": agora,
    }
    salvar_cache(cache)
    log_dedup.debug(f"✅ Nova oferta | plat={plat}")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 16 ▸ FINGERPRINT DA OFERTA
# ══════════════════════════════════════════════════════════════════════════════

def _prod_id(mapa: dict) -> str:
    if not mapa:
        return "0"
    p = list(mapa.values())[0]
    for pat in [r'/dp/([A-Z0-9]{10})', r'/gp/product/([A-Z0-9]{10})',
                r'/i\.(\d+\.\d+)', r'/p/(\d+)/', r'/product/(\d+)']:
        m = re.search(pat, p)
        if m:
            return m.group(1)
    return p[-20:]

def _preco(t: str) -> str:
    m = _RE_PRECO.search(t)
    return m.group(0).strip() if m else "0"

def _cupom(t: str) -> str:
    m = re.search(r'\b([A-Z][A-Z0-9]{3,19})\b', t)
    return m.group(1) if m else ""

def _eh_cupom(t: str) -> bool:
    return bool(_RE_CUPOM_KW.search(t))

def _tem_midia(media) -> bool:
    return media is not None and not isinstance(media, MessageMediaWebPage)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 17 ▸ RATE-LIMIT INTERNO
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
# MÓDULO 18 ▸ ANTI-LOOP DE EDIÇÃO
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
# MÓDULO 19 ▸ ENVIO — IMAGEM + TEXTO EM UMA SÓ MENSAGEM
# ══════════════════════════════════════════════════════════════════════════════

async def _enviar(msg: str, imagem_obj, imagem_eh_bytes: bool) -> object:
    """
    Envia SEMPRE em 1 mensagem.
    - imagem_obj + texto <= 1024 → send_file com caption
    - texto > 1024 ou sem imagem → send_message com link_preview
    """
    if imagem_obj and len(msg) <= 1024:
        try:
            sent = await client.send_file(
                GRUPO_DESTINO, imagem_obj,
                caption=msg, parse_mode="md",
            )
            return sent
        except Exception as e:
            log_tg.warning(f"⚠️ send_file falhou ({e}), tentando sem imagem")

    sent = await client.send_message(
        GRUPO_DESTINO, msg,
        parse_mode="md",
        link_preview=True,
    )
    return sent


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 20 ▸ PIPELINE PRINCIPAL — PROCESSAMENTO COMPLETO
# ══════════════════════════════════════════════════════════════════════════════

async def processar(event, is_edit: bool = False):
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
            log_sys.debug(f"⏩ Anti-loop id={msg_id}")
            return
    else:
        loop   = asyncio.get_event_loop()
        mapa_c = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        if str(msg_id) not in mapa_c:
            log_sys.debug(f"⏩ Edit ignorada (preview?): id={msg_id}")
            return

    # ── E3: Filtro de texto ────────────────────────────────────────────────
    if texto_bloqueado(texto):
        log_fil.warning(f"🚫 @{uname}")
        return

    # ── E4: Extrai links ──────────────────────────────────────────────────
    links_raw = [l.rstrip('.,;)>') for l in _RE_URL.findall(texto)]
    log_lnk.info(f"🔗 {len(links_raw)} link(s)")

    if not links_raw and "fadadoscupons" not in uname:
        log_sys.debug("⏩ Sem links")
        return

    # ── E5: Conversão paralela ─────────────────────────────────────────────
    mapa_links, plat_p = {}, "amazon"
    if links_raw:
        mapa_links, plat_p = await converter_links(links_raw)

    if links_raw and not mapa_links:
        log_sys.warning(f"🚫 Zero links válidos | @{uname}")
        return

    # ── E6: Deduplicação semântica ─────────────────────────────────────────
    prod = _prod_id(mapa_links)
    prec = _preco(texto)
    cup  = _cupom(texto)
    log_dedup.debug(f"🔬 prod={prod} preco={prec} cupom={cup} plat={plat_p}")

    if not is_edit:
        if not deve_enviar(plat_p, prod, prec, cup, texto):
            log_dedup.info("🚫 Duplicata bloqueada")
            return

    # ── E7: Formatação automática com emojis dinâmicos ────────────────────
    msg_final = formatar(texto, mapa_links, plat_p)
    log_fmt.debug(f"📝\n{msg_final[:400]}")

    # ── E8: Imagem com verificação de watermark ───────────────────────────
    media_orig     = event.message.media
    tem_img        = _tem_midia(media_orig)
    imagem_obj     = None
    imagem_eh_bytes = False

    if _eh_cupom(texto):
        if tem_img:
            imagem_obj, imagem_eh_bytes = await preparar_imagem(media_orig, True)
        elif plat_p == "shopee":
            if os.path.exists(IMG_SHOPEE):
                imagem_obj, imagem_eh_bytes = IMG_SHOPEE, False
        elif plat_p == "magalu":
            if os.path.exists(IMG_MAGALU):
                imagem_obj, imagem_eh_bytes = IMG_MAGALU, False
        else:
            if os.path.exists(IMG_AMAZON):
                imagem_obj, imagem_eh_bytes = IMG_AMAZON, False
    else:
        if tem_img:
            imagem_obj, imagem_eh_bytes = await preparar_imagem(media_orig, True)
        elif mapa_links:
            img_url = await buscar_imagem(list(mapa_links.values())[0])
            if img_url:
                imagem_obj, imagem_eh_bytes = await preparar_imagem(img_url, False)

    # ── E9: Rate-limit ────────────────────────────────────────────────────
    await _rate_limit()

    # ── E10: Envio/Edição com retry ───────────────────────────────────────
    async with _SEM_ENVIO:
        loop = asyncio.get_event_loop()
        mapa = await loop.run_in_executor(_EXECUTOR, ler_mapa)

        try:
            # MODO EDIÇÃO
            if is_edit:
                id_dest = mapa[str(msg_id)]
                log_tg.info(f"✏️ Editando id={id_dest}")
                for t in range(1, 4):
                    try:
                        await client.edit_message(GRUPO_DESTINO, id_dest, msg_final)
                        log_tg.info("✅ Edição ok")
                        break
                    except MessageNotModifiedError:
                        log_tg.debug("⏩ Idêntico")
                        break
                    except FloodWaitError as e:
                        log_tg.warning(f"⏳ FW edição: {e.seconds}s")
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        log_tg.error(f"❌ Edição t={t}: {e}")
                        if t < 3:
                            await asyncio.sleep(2 ** t)
                return

            # MODO ENVIO
            sent = None
            for t in range(1, 4):
                try:
                    sent = await _enviar(msg_final, imagem_obj, imagem_eh_bytes)
                    break
                except FloodWaitError as e:
                    log_tg.warning(f"⏳ FW envio: {e.seconds}s")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    log_tg.error(f"❌ Envio t={t}: {e}")
                    if t == 1:
                        imagem_obj = None  # retry sem imagem
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
# MÓDULO 21 ▸ HEALTH CHECK (a cada 5 min)
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
# MÓDULO 22 ▸ INICIALIZAÇÃO COM AUTO-RESTART
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
    log_sys.info(f"🏷  Amazon={AMAZON_TAG} | Shopee={SHOPEE_APP_ID} | "
                 f"Magalu promo={MAGALU_PROMOTER_ID} | Slug={MAGALU_SLUG}")
    log_sys.info(f"🖼  Pillow={'instalado' if _PIL_OK else 'NÃO instalado (pip install Pillow)'}")
    log_sys.info("🚀 FOGUETÃO v66.0 ONLINE!")

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
    """Auto-restart em queda. Para só em erro de autenticação."""
    while True:
        try:
            await _run()
        except (AuthKeyUnregisteredError, SessionPasswordNeededError) as e:
            log_sys.error(f"❌ Auth — encerrando: {e}")
            break
        except Exception as e:
            log_sys.error(f"💥 Bot caiu: {e} — restart em 15s...", exc_info=True)
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(15)


if __name__ == "__main__":
    asyncio.run(main())

"""
╔══════════════════════════════════════════════════════════════════════════╗
║  FOGUETÃO v65.0 — AMAZON + SHOPEE + MAGALU — ELITE MÁXIMO              ║
╠══════════════════════════════════════════════════════════════════════════╣
║  PLATAFORMAS ACEITAS:                                                   ║
║   • Amazon  → tag de afiliado, URL limpa e clicável                    ║
║   • Shopee  → API de afiliado oficial                                  ║
║   • Magalu  → parâmetros completos de comissão + Cuttly encurtador     ║
║  TUDO O QUE NÃO FOR DESSAS 3 → DESCARTADO SILENCIOSAMENTE             ║
╠══════════════════════════════════════════════════════════════════════════╣
║  CORREÇÕES v65:                                                         ║
║   1. IMAGEM + TEXTO = 1 mensagem só (nunca duas separadas)             ║
║   2. Amazon URL limpa: apenas /dp/ASIN?tag= (sem lixo de params)       ║
║   3. TAG leo21073-20 NUNCA aparece em links que não são Amazon         ║
║   4. Magalu: desencurta → reconstrói com params de comissão → Cuttly  ║
║   5. Casas Bahia, Americanas e outros: bloqueados no filtro de domínio ║
║   6. Formatação automática bonita linha a linha                        ║
║   7. Anti-loop de edição (preview Telegram não vira nova mensagem)     ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import os, re, time, json, asyncio, aiohttp, hashlib, random
import unicodedata, logging, concurrent.futures
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


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 1 ▸ LOGS PROFISSIONAIS — UM POR MÓDULO/PLATAFORMA
# ══════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 2 ▸ CONFIGURAÇÕES GERAIS
# ══════════════════════════════════════════════════════════════════════════

API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION", "")

GRUPOS_ORIGEM  = ['promotom', 'fumotom', 'botofera', 'fadadoscupons']
GRUPO_DESTINO  = '@ofertap'

# ── Afiliados ─────────────────────────────────────────────────────────────
AMAZON_TAG     = os.environ.get("AMAZON_TAG",    "leo21073-20")   # SÓ para Amazon
SHOPEE_APP_ID  = os.environ.get("SHOPEE_APP_ID", "18348480261")
SHOPEE_SECRET  = os.environ.get("SHOPEE_SECRET", "SGC7FQQQ4R5QCFULPXIBCANATLP272B3")

# Magalu — parâmetros completos de comissão
MAGALU_PARTNER_ID  = os.environ.get("MAGALU_PARTNER_ID",  "3440")
MAGALU_PROMOTER_ID = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
MAGALU_PID         = os.environ.get("MAGALU_PID",         "magazinevoce")

# Cuttly — para encurtar links da Magalu
CUTTLY_API_KEY = os.environ.get("CUTTLY_API_KEY", "8d2afd3c7f72869f42d23cf0d849c72172509")

# ── Imagens fixas de fallback (cupons sem imagem) ─────────────────────────
IMG_AMAZON = "cupom-amazon.jpg"
IMG_SHOPEE = "IMG_20260404_180150.jpg"
IMG_MAGALU = "magalu_promo.jpg"   # coloque essa imagem na pasta se tiver

# ── Persistência ─────────────────────────────────────────────────────────
ARQUIVO_CACHE      = "cache_dedup.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

# ── Concorrência ─────────────────────────────────────────────────────────
_SEM_ENVIO  = asyncio.Semaphore(3)
_SEM_HTTP   = asyncio.Semaphore(20)
_EXECUTOR   = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# ── Rate-limit interno ────────────────────────────────────────────────────
_RATE_LOCK       = asyncio.Lock()
_ULTIMO_ENVIO_TS = 0.0
_INTERVALO_MIN   = 1.5   # segundos mínimos entre envios

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
# MÓDULO 3 ▸ PERSISTÊNCIA — SURVIVES RESTART
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
        log_sys.error(f"❌ Ler {path}: {e}")
        return {}


def _gravar_json(path: str, data: dict, lock: Lock):
    with lock:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log_sys.error(f"❌ Gravar {path}: {e}")


ler_mapa    = lambda: _ler_json(ARQUIVO_MAPEAMENTO)
salvar_mapa = lambda m: _gravar_json(ARQUIVO_MAPEAMENTO, m, _MAP_LOCK)
ler_cache   = lambda: _ler_json(ARQUIVO_CACHE)
salvar_cache= lambda c: _gravar_json(ARQUIVO_CACHE, c, _CACHE_LOCK)


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 4 ▸ FILTRO BLINDADO DE TEXTO
# ══════════════════════════════════════════════════════════════════════════

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
            log_fil.debug(f"🚫 Filtro texto: '{p}'")
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 5 ▸ CLASSIFICAÇÃO DE DOMÍNIO
#
#  REGRA RÍGIDA:
#   • Amazon   → 'amazon'
#   • Shopee   → 'shopee'
#   • Magalu   → 'magalu'
#   • Qualquer outra coisa → None (descartado)
#   • A tag leo21073-20 NUNCA vai para links de outras plataformas
# ══════════════════════════════════════════════════════════════════════════

_DOM_AMAZON = frozenset(["amazon.com", "amzn.to", "amzn.com", "a.co"])
_DOM_SHOPEE = frozenset(["shopee.com.br", "shopee.com", "s.shopee", "shope.ee"])
_DOM_MAGALU = frozenset([
    "magazineluiza.com.br", "magazinevoce.com.br",
    "maga.lu", "sacola.magazineluiza.com.br",
])

# Domínios conhecidos que NUNCA devem ser convertidos
_DOM_LIXO = frozenset([
    "casasbahia.com.br", "americanas.com.br", "submarino.com.br",
    "shoptime.com.br", "extra.com.br", "pontofrio.com.br",
    "mercadolivre.com", "mercadopago.com", "meli.la",
    "aliexpress.com", "ali.ski", "kabum.com.br", "pichau.com.br",
    "terabyteshop.com.br", "fastshop.com.br", "leroymerlin.com.br",
    "t.me", "telegram.me", "instagram.com", "facebook.com",
    "youtube.com", "youtu.be", "twitter.com", "x.com",
    "bit.ly",  # só bloqueia se não expandir para uma das 3 aceitas
])


def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def classificar(url: str) -> str | None:
    """Retorna 'amazon', 'shopee', 'magalu' ou None."""
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
            log_lnk.debug(f"🗑 Lixo: {nl}")
            return None
    return None   # desconhecido → será expandido


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 6 ▸ DESENCURTADOR UNIVERSAL — ATÉ O OSSO (12 níveis)
# ══════════════════════════════════════════════════════════════════════════

async def desencurtar(url: str, sessao: aiohttp.ClientSession, depth: int = 0) -> str:
    if depth > 12:
        return url
    hdrs = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    }
    try:
        # 1ª tentativa: HEAD rápido
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

        # 2ª tentativa: GET completo + meta-refresh + JS
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
                log_lnk.debug(f"  JS d={depth} → {mj.group(1)[:80]}")
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


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 7 ▸ MOTOR AMAZON
#  • Desencurta → verifica que é realmente Amazon → limpa params → injeta tag
#  • URL final: amazon.com.br/dp/ASIN?tag=leo21073-20   (curta e limpa)
# ══════════════════════════════════════════════════════════════════════════

# Parâmetros que NÃO têm valor para o afiliado — remove todos esses
_AMZ_PARAMS_LIXO = frozenset({
    "ascsubtag","btn_ref","ref_","ref","smid","sprefix","sr","spla",
    "dchild","linkcode","linkid","camp","creative","pf_rd_p","pf_rd_r",
    "pd_rd_wg","pd_rd_w","content-id","pd_rd_r","pd_rd_i","ie","qid",
    "_encoding","dib","dib_tag","m","marketplaceid","s","ufe",
    "th","psc",   # th e psc às vezes são necessários mas deixam URL feia
})

# Parâmetros que DEVEM ser mantidos quando presentes
_AMZ_PARAMS_MANTER = frozenset({"tag", "keywords", "node", "k", "i", "rh"})


def _limpar_amazon(url_exp: str) -> str:
    """
    Produz URL Amazon limpa:
      https://www.amazon.com.br/dp/ASIN?tag=leo21073-20
    ou para páginas de busca/eventos:
      https://www.amazon.com.br/s?k=TERMO&tag=leo21073-20
    """
    p    = urlparse(url_exp)
    path = p.path

    # Corta sufixo /ref=... e mantém só até o ASIN
    path = re.sub(r'(/dp/[A-Z0-9]{10})(/.*)?$',         r'\1', path)
    path = re.sub(r'(/gp/product/[A-Z0-9]{10})(/.*)?$', r'\1', path)

    params_orig = parse_qs(p.query, keep_blank_values=False)
    params_new  = {}

    # Mantém apenas os parâmetros "bons"
    for k, v in params_orig.items():
        kl = k.lower()
        if kl in _AMZ_PARAMS_MANTER:
            params_new[k] = v
        elif kl not in _AMZ_PARAMS_LIXO:
            # Parâmetro desconhecido — mantém apenas se for curto (provável necessário)
            if len(v[0]) < 30:
                params_new[k] = v

    # Garante nossa tag (NUNCA outra)
    params_new["tag"] = [AMAZON_TAG]

    query = urlencode(params_new, doseq=True)
    result = urlunparse(p._replace(path=path, query=query))
    return result


async def motor_amazon(url: str, sessao: aiohttp.ClientSession) -> str | None:
    log_amz.debug(f"🔗 Iniciando | {url[:80]}")
    async with _SEM_HTTP:
        exp = await desencurtar(url, sessao)
    log_amz.debug(f"  📦 Expandida | {exp[:80]}")

    if classificar(exp) != "amazon":
        log_amz.warning(f"  ⚠️ Não é Amazon após expansão: {exp[:60]}")
        return None

    final = _limpar_amazon(exp)
    log_amz.info(f"  ✅ {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ MOTOR SHOPEE — API AFILIADO COM RETRY
# ══════════════════════════════════════════════════════════════════════════

async def motor_shopee(url: str, sessao: aiohttp.ClientSession) -> str | None:
    log_shp.debug(f"🔗 Iniciando | {url[:80]}")
    async with _SEM_HTTP:
        exp = await desencurtar(url, sessao)
    log_shp.debug(f"  📦 Expandida | {exp[:80]}")

    ts      = str(int(time.time()))
    payload = json.dumps(
        {"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{exp}" }}) '
                  f'{{ shortLink }} }}'},
        separators=(",", ":")
    )
    sig  = hashlib.sha256(
        f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()
    ).hexdigest()
    hdrs = {
        "Authorization": f"SHA256 Credential={SHOPEE_APP_ID},"
                         f"Timestamp={ts},Signature={sig}",
        "Content-Type": "application/json",
    }

    for t in range(1, 4):
        try:
            async with _SEM_HTTP:
                async with sessao.post(
                    "https://open-api.affiliate.shopee.com.br/graphql",
                    data=payload, headers=hdrs,
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as r:
                    res  = await r.json()
                    link = res["data"]["generateShortLink"]["shortLink"]
                    log_shp.info(f"  ✅ {link}")
                    return link
        except Exception as e:
            log_shp.warning(f"  ⚠️ Tentativa {t}/3: {e}")
            await asyncio.sleep(2 ** t)

    log_shp.error(f"  ❌ API Shopee falhou 3x")
    return None


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 9 ▸ MOTOR MAGALU
#  Fluxo: desencurtar → extrair URL limpa → reconstruir com params de
#  comissão → encurtar via Cuttly
# ══════════════════════════════════════════════════════════════════════════

def _reconstruir_magalu(url_exp: str) -> str:
    """
    Reconstrói o link da Magalu com todos os parâmetros de comissão.
    Garante que promoterId, partnerId, pid e UTMs estejam presentes.
    """
    p = urlparse(url_exp)

    # Pega apenas o path do produto (sem query strings anteriores)
    path = p.path

    params = {
        "utmsource":        "divulgador",
        "utmmedium":        "magalu",
        "partnerid":        MAGALU_PARTNER_ID,
        "promoterid":       MAGALU_PROMOTER_ID,
        "utmcampaign":      MAGALU_PROMOTER_ID,
        "afforcedeeplink":  "true",
        "isretargeting":    "true",
        "pid":              MAGALU_PID,
        "c":                MAGALU_PROMOTER_ID,
    }

    # Constrói a URL base do produto
    base_url = urlunparse(p._replace(query=""))

    # O parâmetro deeplinkvalue aponta para a própria URL do produto
    deeplink = (f"{base_url}?utmsource=divulgador&utmmedium=magalu"
                f"&partnerid={MAGALU_PARTNER_ID}&promoterid={MAGALU_PROMOTER_ID}"
                f"&utmcampaign={MAGALU_PROMOTER_ID}")

    params["deeplinkvalue"] = deeplink
    query = urlencode(params)
    return urlunparse(p._replace(query=query))


async def _cuttly_encurtar(url_longa: str, sessao: aiohttp.ClientSession) -> str:
    """Encurta via API Cuttly. Retorna a URL original em caso de falha."""
    api = (f"https://cutt.ly/api/api.php"
           f"?key={CUTTLY_API_KEY}&short={quote(url_longa, safe='')}")
    try:
        async with _SEM_HTTP:
            async with sessao.get(api, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json(content_type=None)
                status = data.get("url", {}).get("status")
                if status == 7:
                    short = data["url"]["shortLink"]
                    log_mgl.info(f"  ✂️ Cuttly: {short}")
                    return short
                else:
                    log_mgl.warning(f"  ⚠️ Cuttly status={status}")
    except Exception as e:
        log_mgl.warning(f"  ⚠️ Cuttly falhou: {e}")
    return url_longa   # fallback: retorna longa


async def motor_magalu(url: str, sessao: aiohttp.ClientSession) -> str | None:
    log_mgl.debug(f"🔗 Iniciando | {url[:80]}")
    async with _SEM_HTTP:
        exp = await desencurtar(url, sessao)
    log_mgl.debug(f"  📦 Expandida | {exp[:80]}")

    if classificar(exp) != "magalu":
        log_mgl.warning(f"  ⚠️ Não é Magalu após expansão: {exp[:60]}")
        return None

    # Reconstrói com parâmetros de comissão
    url_com_params = _reconstruir_magalu(exp)
    log_mgl.debug(f"  🏷 Com params | {url_com_params[:80]}")

    # Encurta via Cuttly
    final = await _cuttly_encurtar(url_com_params, sessao)
    log_mgl.info(f"  ✅ {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 10 ▸ PIPELINE DE CONVERSÃO — TODOS OS LINKS EM PARALELO
# ══════════════════════════════════════════════════════════════════════════

async def _converter_um(url: str, sessao: aiohttp.ClientSession) -> tuple:
    """
    Converte 1 link.
    Encurtadores genéricos são expandidos antes de classificar.
    Retorna (url_convertida, plataforma) ou (None, None).
    """
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
        # Domínio desconhecido — pode ser encurtador genérico
        nl = _netloc(url)
        # Se for domínio de lixo conhecido → descarta direto
        for d in _DOM_LIXO:
            if d in nl:
                log_lnk.info(f"🗑 Lixo direto: {url[:70]}")
                return None, None

        # Tenta expandir
        log_lnk.debug(f"🔄 Expandindo desconhecido: {url[:70]}")
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
    """
    Converte até 50 links em paralelo.
    Retorna (mapa {original: convertido}, plataforma_principal).
    """
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
            log_lnk.error(f"  ❌ link[{i}] {links[i][:50]}: {res}")
            continue
        novo, plat = res
        if novo and plat:
            mapa[links[i]] = novo
            plats.append(plat)
            log_lnk.debug(f"  [{plat.upper()}] {links[i][:40]} → {novo[:50]}")

    plat_p = max(set(plats), key=plats.count) if plats else "amazon"
    log_lnk.info(f"✅ {len(mapa)}/{len(links)} convertidos | plat={plat_p}")
    return mapa, plat_p


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 11 ▸ FORMATAÇÃO AUTOMÁTICA — MONTA MENSAGEM BONITA
#
#  REGRAS:
#  1. Processa linha a linha, preservando a estrutura original
#  2. Substitui cada link pelo de afiliado
#  3. Remove linhas que são só links inválidos (não-Amazon/Shopee/Magalu)
#  4. Aplica emoji de contexto onde não há emoji próprio:
#     • Primeira linha de texto → 🔥
#     • Linha com R$ (preço)   → ✅
#     • Linha com cupom/off    → 🎟
#     • Links → nunca emoji
#  5. Remove linhas vazias duplicadas
# ══════════════════════════════════════════════════════════════════════════

_RE_URL      = re.compile(r'https?://[^\s\)\]>,"\'<]+')
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


def formatar(texto_original: str, mapa: dict) -> str:
    """
    Formata a mensagem final linha a linha.
    """
    aceitos          = set(mapa.values())
    linhas           = texto_original.split('\n')
    saida            = []
    primeira_texto   = True   # flag: ainda não encontramos a 1ª linha de texto

    for linha in linhas:
        ls = linha.strip()

        # Linha vazia → preserva
        if not ls:
            saida.append("")
            continue

        # ── Verifica se a linha é UM link isolado ────────────────────────
        urls_na_linha = _RE_URL.findall(ls)
        sem_urls = _RE_URL.sub("", ls).strip()

        if urls_na_linha and not sem_urls:
            # Linha só de link(s)
            novos = []
            for u in urls_na_linha:
                uc = u.rstrip('.,;)>')
                if uc in mapa:
                    novos.append(mapa[uc])
                else:
                    log_fmt.debug(f"🗑 Link descartado: {uc[:60]}")
            if novos:
                saida.extend(novos)
            # Se não sobrou nenhum → linha removida
            continue

        # ── Linha com texto (pode ter link inline) ────────────────────────
        nova = ls

        # Substitui links inline
        def _sub(match):
            uc = match.group(0).rstrip('.,;)>')
            if uc in mapa:
                return mapa[uc]
            log_fmt.debug(f"🗑 Link inline descartado: {uc[:60]}")
            return ""

        nova = _RE_URL.sub(_sub, nova).strip()

        # Se ficou vazia após substituição → remove
        if not nova:
            continue

        # ── Aplica emoji de contexto ──────────────────────────────────────
        if not _tem_emoji(nova) and not _RE_URL.match(nova):
            tem_p = bool(_RE_PRECO.search(nova))
            tem_c = bool(_RE_CUPOM_KW.search(nova))

            if primeira_texto and not tem_p and not tem_c:
                nova = "🔥 " + nova
                primeira_texto = False
            elif tem_p and tem_c:
                nova = "🎟 " + nova
                if primeira_texto:
                    primeira_texto = False
            elif tem_p:
                nova = "✅ " + nova
                if primeira_texto:
                    primeira_texto = False
            elif tem_c:
                nova = "🎟 " + nova
                if primeira_texto:
                    primeira_texto = False
            else:
                if primeira_texto:
                    primeira_texto = False
        elif not _RE_URL.match(nova) and _tem_emoji(nova):
            if primeira_texto:
                primeira_texto = False

        saida.append(nova)

    # Remove linhas vazias consecutivas (máx 1)
    final_linhas = []
    prev_vazia   = False
    for l in saida:
        if l.strip() == "":
            if not prev_vazia:
                final_linhas.append("")
            prev_vazia = True
        else:
            prev_vazia = False
            final_linhas.append(l)

    resultado = "\n".join(final_linhas).strip()
    log_fmt.debug(f"✅ {len(resultado)} chars formatados")
    return resultado


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 12 ▸ BUSCA DE IMAGEM — 3 TENTATIVAS + FALLBACK PREVIEW
# ══════════════════════════════════════════════════════════════════════════

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
                        log_img.info(f"  ✅ (t={t}): {tag['content'][:70]}")
                        return tag["content"]
        except asyncio.TimeoutError:
            log_img.warning(f"  ⏱ Timeout t={t}")
        except Exception as e:
            log_img.warning(f"  ⚠️ Erro t={t}: {e}")
        await asyncio.sleep(1.5 * t)
    log_img.warning("  ❌ Sem imagem → preview")
    return None


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 13 ▸ FINGERPRINT DA OFERTA
# ══════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ DEDUPLICAÇÃO ULTRA-FORTE — 3 CAMADAS (TEXTO/CUPOM, ≠ LINK)
# ══════════════════════════════════════════════════════════════════════════

_TTL        = 120 * 60
_JANELA     = 900
_SIM_NORMAL = 0.85
_SIM_ALTA   = 0.92

_RUIDO = {
    "promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora",
    "relampago","relâmpago",
}


def _rm_ac(t: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', t)
                   if unicodedata.category(c) != 'Mn')


def _norm(texto: str) -> str:
    t = _rm_ac(texto.lower())
    t = re.sub(r"http\S+|www\S+", " ", t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return " ".join(sorted(w for w in t.split() if w not in _RUIDO))


def deve_enviar(plat: str, prod: str, preco: str,
                cupom: str = "", texto: str = "") -> bool:
    cache = ler_cache()
    agora = time.time()
    cache = {k: v for k, v in cache.items() if agora - v.get("ts", 0) < _TTL}

    tnorm = _norm(texto)
    cnorm = cupom.strip().upper()
    h = hashlib.sha256(f"{plat}|{prod}|{preco}|{cnorm}|{tnorm}".encode()).hexdigest()

    # C1 — hash exato
    if h in cache:
        log_dedup.info(f"🔁 [C1] Hash exato | prod={prod} cupom={cnorm}")
        return False

    for e in cache.values():
        if agora - e.get("ts", 0) >= _JANELA:
            continue
        sim     = SequenceMatcher(None, tnorm, e.get("txt", "")).ratio()
        c_igual = e.get("cupom", "") == cnorm.lower()
        p_igual = str(e.get("prod")) == str(prod) and prod != "0"
        r_igual = str(e.get("preco")) == str(preco) and preco != "0"

        # C2 — mesmo produto/preço/cupom + texto similar
        if p_igual and r_igual and c_igual and sim >= _SIM_NORMAL:
            log_dedup.info(f"🔁 [C2] sim={sim:.2f} prod={prod}")
            return False

        # C3 — cupom igual + texto quase idêntico
        if c_igual and sim >= _SIM_ALTA:
            log_dedup.info(f"🔁 [C3] sim={sim:.2f} cupom={cnorm}")
            return False

    cache[h] = {"plat": plat, "prod": str(prod), "preco": str(preco),
                "cupom": cnorm.lower(), "txt": tnorm, "ts": agora}
    salvar_cache(cache)
    log_dedup.debug(f"✅ Nova oferta | plat={plat}")
    return True


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 15 ▸ RATE-LIMIT INTERNO
# ══════════════════════════════════════════════════════════════════════════

async def _rate_limit():
    global _ULTIMO_ENVIO_TS
    async with _RATE_LOCK:
        agora  = time.monotonic()
        espera = _INTERVALO_MIN - (agora - _ULTIMO_ENVIO_TS)
        if espera > 0:
            await asyncio.sleep(espera)
        _ULTIMO_ENVIO_TS = time.monotonic()


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 16 ▸ ANTI-LOOP DE EDIÇÃO
#
#  PROBLEMA: o Telegram dispara MessageEdited logo após um envio normal
#  para atualizar o preview de link — isso fazia o bot reenviar a oferta.
#
#  SOLUÇÃO:
#  • NewMessage com id já processado → ignora
#  • MessageEdited com id que não está no mapa → é preview fake → ignora
# ══════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 17 ▸ ENVIO — IMAGEM + TEXTO EM UMA SÓ MENSAGEM
#
#  REGRA: nunca enviar imagem separada e texto como reply.
#  Se texto <= 1024 chars → send_file com caption (1 msg)
#  Se texto  > 1024 chars → send_file com caption truncada + texto completo
#  no mesmo envio usando parse_mode, ou simplesmente envia texto sem foto
#  (Telegram limita caption a 1024 chars).
#  SOLUÇÃO ELEGANTE: se > 1024 chars, envia SEM imagem (link preview ativa).
# ══════════════════════════════════════════════════════════════════════════

async def _enviar(msg: str, imagem) -> object:
    """
    Envia 1 única mensagem.
    Com imagem: send_file com caption (se <= 1024 chars)
    Sem imagem ou texto longo: send_message com link_preview
    """
    if imagem and len(msg) <= 1024:
        try:
            sent = await client.send_file(
                GRUPO_DESTINO, imagem,
                caption=msg,
                parse_mode="md",
            )
            return sent
        except Exception as e:
            log_tg.warning(f"⚠️ send_file falhou ({e}), tentando sem imagem")

    # Sem imagem (ou fallback)
    sent = await client.send_message(
        GRUPO_DESTINO, msg,
        parse_mode="md",
        link_preview=True,
    )
    return sent


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 18 ▸ PIPELINE PRINCIPAL — PROCESSAMENTO COMPLETO
# ══════════════════════════════════════════════════════════════════════════

async def processar(event, is_edit: bool = False):
    msg_id = event.message.id
    texto  = event.message.text or ""
    chat   = await event.get_chat()
    uname  = (chat.username or str(event.chat_id)).lower()

    log_tg.info(f"{'✏️ EDIT' if is_edit else '📩 NEW'} | @{uname} | id={msg_id} | "
                f"{len(texto)} chars")

    # ── E1: Validação básica ──────────────────────────────────────────────
    if not texto.strip():
        return

    # ── E2: Anti-loop ─────────────────────────────────────────────────────
    if not is_edit:
        if await _processado(msg_id):
            log_sys.debug(f"⏩ Anti-loop NewMessage id={msg_id}")
            return
    else:
        loop_exec = asyncio.get_event_loop()
        mapa_chk  = await loop_exec.run_in_executor(_EXECUTOR, ler_mapa)
        if str(msg_id) not in mapa_chk:
            log_sys.debug(f"⏩ Edit ignorada (preview?): id={msg_id}")
            return

    # ── E3: Filtro de texto ────────────────────────────────────────────────
    if texto_bloqueado(texto):
        log_fil.warning(f"🚫 Filtro texto | @{uname}")
        return

    # ── E4: Extrai links brutos ────────────────────────────────────────────
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

    # ── E6: Deduplicação ──────────────────────────────────────────────────
    prod = _prod_id(mapa_links)
    prec = _preco(texto)
    cup  = _cupom(texto)
    log_dedup.debug(f"🔬 prod={prod} preco={prec} cupom={cup} plat={plat_p}")

    if not is_edit:
        if not deve_enviar(plat_p, prod, prec, cup, texto):
            log_dedup.info("🚫 Duplicata")
            return

    # ── E7: Formatação automática ─────────────────────────────────────────
    msg_final = formatar(texto, mapa_links)
    log_fmt.debug(f"📝 Final:\n{msg_final[:300]}")

    # ── E8: Imagem ────────────────────────────────────────────────────────
    media_orig = event.message.media
    imagem     = None

    if _eh_cupom(texto):
        if _tem_midia(media_orig):
            imagem = media_orig
            log_img.debug("🖼 Cupom: imagem real")
        elif plat_p == "shopee":
            imagem = IMG_SHOPEE if os.path.exists(IMG_SHOPEE) else None
            log_img.info(f"🖼 Cupom Shopee → {IMG_SHOPEE}")
        elif plat_p == "magalu":
            imagem = IMG_MAGALU if os.path.exists(IMG_MAGALU) else None
            log_img.info(f"🖼 Cupom Magalu → {IMG_MAGALU}")
        else:
            imagem = IMG_AMAZON if os.path.exists(IMG_AMAZON) else None
            log_img.info(f"🖼 Cupom Amazon → {IMG_AMAZON}")
    else:
        if _tem_midia(media_orig):
            imagem = media_orig
            log_img.debug("🖼 Oferta: imagem real")
        elif mapa_links:
            imagem = await buscar_imagem(list(mapa_links.values())[0])

    # ── E9: Rate-limit ────────────────────────────────────────────────────
    await _rate_limit()

    # ── E10: Envio / Edição com retry ────────────────────────────────────
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
                        log_tg.debug("⏩ Conteúdo idêntico")
                        break
                    except FloodWaitError as e:
                        log_tg.warning(f"⏳ FloodWait edição: {e.seconds}s")
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        log_tg.error(f"❌ Edição t={t}: {e}")
                        if t < 3:
                            await asyncio.sleep(2 ** t)
                return

            # MODO ENVIO — com retry e backoff
            sent = None
            for t in range(1, 4):
                try:
                    sent = await _enviar(msg_final, imagem)
                    break
                except FloodWaitError as e:
                    log_tg.warning(f"⏳ FloodWait envio: {e.seconds}s")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    log_tg.error(f"❌ Envio t={t}: {e}")
                    if t == 1:
                        imagem = None   # retry sem imagem
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


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 19 ▸ HEALTH CHECK PERIÓDICO (a cada 5 min)
# ══════════════════════════════════════════════════════════════════════════

async def _health_check():
    while True:
        await asyncio.sleep(300)
        try:
            cache = ler_cache()
            mapa  = ler_mapa()
            log_hc.info(
                f"💚 Health | cache={len(cache)} | mapa={len(mapa)} | "
                f"anti-loop_set={len(_IDS_PROC)}")
        except Exception as e:
            log_hc.error(f"❌ Health check: {e}")


# ══════════════════════════════════════════════════════════════════════════
# MÓDULO 20 ▸ INICIALIZAÇÃO COM AUTO-RESTART
# ══════════════════════════════════════════════════════════════════════════

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


async def _run():
    log_sys.info("🔌 Conectando ao Telegram...")
    await client.connect()

    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida! Verifique TELEGRAM_SESSION no Railway.")
        return False

    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 Grupos: {GRUPOS_ORIGEM}")
    log_sys.info(f"📣 Destino: {GRUPO_DESTINO}")
    log_sys.info(f"🏷  Amazon={AMAZON_TAG} | Shopee={SHOPEE_APP_ID} | "
                 f"Magalu promo={MAGALU_PROMOTER_ID}")
    log_sys.info("🚀 FOGUETÃO v65.0 ONLINE — AMAZON + SHOPEE + MAGALU!")

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
    """Auto-restart em qualquer queda. Para apenas em erro de autenticação."""
    while True:
        try:
            await _run()
        except (AuthKeyUnregisteredError, SessionPasswordNeededError) as e:
            log_sys.error(f"❌ Autenticação — encerrando: {e}")
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

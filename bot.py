"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   FOGUETÃO v67.0 — AMAZON + SHOPEE + MAGALU — REVISÃO FINAL               ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  MUDANÇAS v67:                                                              ║
║   1. Cupom → crases `CODIGO` (não mais aspas)                              ║
║   2. Shopee → sem desencurtamento (chega pronta, trata direto)             ║
║   3. Amazon → só desencurta se for amzn.to; se amazon.com.br, vai direto  ║
║      Tag leo21073-20 NUNCA vai para links fora da Amazon                   ║
║   4. Magalu → desencurta → se já tiver promoter_id preserva; senão aplica ║
║      Nunca substitui por link genérico fixo                                ║
║   5. Whitelist rígida: Amazon / Shopee / Magalu — resto é deletado         ║
║   6. Limpeza visual: remove ML:, :: ML, - ML e marcadores técnicos         ║
║   7. Emojis contextuais inteligentes (whey💪, tênis👟, álbum📚, etc.)     ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os, re, time, json, asyncio, aiohttp, hashlib, random, io
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

try:
    from PIL import Image
    _PIL_OK = True
except ImportError:
    _PIL_OK = False


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 1 ▸ LOGS PROFISSIONAIS — UM POR PLATAFORMA/MÓDULO
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
# AMAZON_TAG é EXCLUSIVO para domínios amazon.com/amzn.to — nunca outro lugar
AMAZON_TAG     = os.environ.get("AMAZON_TAG",    "leo21073-20")
SHOPEE_APP_ID  = os.environ.get("SHOPEE_APP_ID", "18348480261")
SHOPEE_SECRET  = os.environ.get("SHOPEE_SECRET", "SGC7FQQQ4R5QCFULPXIBCANATLP272B3")

# Magalu
MAGALU_PARTNER_ID  = os.environ.get("MAGALU_PARTNER_ID",  "3440")
MAGALU_PROMOTER_ID = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
MAGALU_PID         = os.environ.get("MAGALU_PID",         "magazinevoce")
MAGALU_SLUG        = os.environ.get("MAGALU_SLUG",        "magazineleo12")
CUTTLY_API_KEY     = os.environ.get("CUTTLY_API_KEY",     "8d2afd3c7f72869f42d23cf0d849c72172509")

IMG_AMAZON = "cupom-amazon.jpg"
IMG_SHOPEE = "IMG_20260404_180150.jpg"
IMG_MAGALU = "magalu_promo.jpg"

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
            log_fil.debug(f"🚫 Filtro: '{p}'")
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 5 ▸ WHITELIST E CLASSIFICAÇÃO — PLATAFORMA ANTES DE TUDO
#
# WHITELIST OBRIGATÓRIA:
#   amazon.com.br / amzn.to → 'amazon'
#   shopee.com.br / s.shopee.com.br → 'shopee'
#   magazineluiza.com.br / sacola.magazineluiza.com.br → 'magalu'
#   Qualquer outro domínio → descartado imediatamente
# ══════════════════════════════════════════════════════════════════════════════

# Whitelist positiva — SOMENTE esses passam
_WHITELIST = {
    "amazon.com.br":               "amazon",
    "amzn.to":                     "amazon",
    "amzn.com":                    "amazon",
    "a.co":                        "amazon",
    "shopee.com.br":               "shopee",
    "s.shopee.com.br":             "shopee",
    "shopee.com":                  "shopee",
    "shope.ee":                    "shopee",
    "magazineluiza.com.br":        "magalu",
    "sacola.magazineluiza.com.br": "magalu",
    "magazinevoce.com.br":         "magalu",
    "maga.lu":                     "magalu",
}

# Encurtadores genéricos que precisam ser expandidos antes de classificar
_ENCURTADORES = frozenset([
    "bit.ly", "cutt.ly", "tinyurl.com", "t.co", "ow.ly",
    "goo.gl", "rb.gy", "is.gd", "tiny.cc", "buff.ly",
])

def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""

def classificar(url: str) -> str | None:
    """
    Retorna 'amazon', 'shopee', 'magalu' ou None.
    Usa whitelist positiva: se não estiver na lista → None → descartado.
    """
    nl = _netloc(url)
    # Verifica whitelist
    for dominio, plat in _WHITELIST.items():
        if dominio in nl:
            return plat
    # Verifica se é encurtador genérico (precisa expandir)
    for enc in _ENCURTADORES:
        if enc in nl:
            return "expandir"
    # Domínio desconhecido → descarta
    log_lnk.debug(f"🗑 Fora da whitelist: {nl}")
    return None

def _magalu_ja_tem_params(url: str) -> bool:
    """
    Retorna True se o link Magalu já tiver os parâmetros de comissão corretos.
    Nesse caso, não sobrescrevemos — apenas preservamos.
    """
    url_lower = url.lower()
    return ("promoterid=" in url_lower or "promoter_id=" in url_lower)

def eh_magalu_vitrine(url: str) -> bool:
    """Retorna True se for URL de vitrine/categoria/lista Magalu."""
    return bool(re.search(
        r'(magazineluiza\.com\.br|magazinevoce\.com\.br)'
        r'.*(vitrine|categoria|lista|promo|stores|loja)',
        url, re.I
    ))


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 6 ▸ DESENCURTADOR — USADO APENAS QUANDO NECESSÁRIO
# (amzn.to, maga.lu, encurtadores genéricos — NÃO usado para Shopee direta)
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
        # HEAD rápido
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
        # GET completo
        async with sessao.get(url, headers=hdrs, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=14)) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            ref  = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
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
#
# Fluxo por tipo de URL:
#   amzn.to  → desencurta → confirma amazon.com.br → limpa → injeta tag
#   amazon.com.br → vai direto → limpa → injeta tag
#   Qualquer outro → NUNCA injeta a tag
# ══════════════════════════════════════════════════════════════════════════════

_AMZ_LIXO = frozenset({
    "ascsubtag", "btn_ref", "ref_", "ref", "smid", "sprefix", "sr", "spla",
    "dchild", "linkcode", "linkid", "camp", "creative", "pf_rd_p", "pf_rd_r",
    "pd_rd_wg", "pd_rd_w", "content-id", "pd_rd_r", "pd_rd_i", "ie", "qid",
    "_encoding", "dib", "dib_tag", "m", "marketplaceid", "ufe",
    "th", "psc", "ingress", "visitid", "lp_context_asin",
})
_AMZ_MANTER = frozenset({"tag", "keywords", "node", "k", "i", "rh"})


def _limpar_url_amazon(url_exp: str) -> str:
    """
    Produz URL Amazon limpa e curta:
    https://www.amazon.com.br/dp/ASIN?tag=leo21073-20
    A tag SOMENTE é injetada aqui — em nenhum outro lugar do código.
    """
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

    params_new["tag"] = [AMAZON_TAG]  # ← ÚNICA INJEÇÃO DA TAG AMAZON
    return urlunparse(p._replace(path=path, query=urlencode(params_new, doseq=True)))


async def motor_amazon(url: str, sessao: aiohttp.ClientSession) -> str | None:
    nl = _netloc(url)

    # Caso 1: amzn.to ou a.co → PRECISA desencurtar
    if "amzn.to" in nl or "a.co" in nl or "amzn.com" in nl:
        log_amz.debug(f"🔗 Expandindo encurtado: {url[:80]}")
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)
        log_amz.debug(f"  📦 {exp[:80]}")
        # Confirma que chegou em amazon.com.br
        if classificar(exp) != "amazon":
            log_amz.warning(f"  ⚠️ Não é Amazon após expansão: {exp[:60]}")
            return None
        final = _limpar_url_amazon(exp)

    # Caso 2: amazon.com.br → vai direto, sem desencurtar
    elif "amazon.com.br" in nl or "amazon.com" in nl:
        log_amz.debug(f"🔗 Amazon direta: {url[:80]}")
        final = _limpar_url_amazon(url)

    else:
        log_amz.warning(f"  ⚠️ URL não reconhecida para motor Amazon: {url[:60]}")
        return None

    log_amz.info(f"  ✅ {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ MOTOR SHOPEE
#
# A Shopee chega PRONTA em shopee.com.br ou s.shopee.com.br.
# Não precisa de desencurtamento.
# Passa direto para a API de afiliado com a URL original.
# ══════════════════════════════════════════════════════════════════════════════

async def motor_shopee(url: str, sessao: aiohttp.ClientSession) -> str | None:
    log_shp.debug(f"🔗 Shopee direta: {url[:80]}")
    # Shopee já vem em domínio correto — não desencurta
    # Apenas gera o link de afiliado via API
    ts      = str(int(time.time()))
    payload = json.dumps(
        {"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{url}" }}) '
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
    log_shp.error("  ❌ API Shopee falhou 3x — retornando original")
    return url  # fallback: URL original da Shopee (sem afiliado mas funciona)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 9 ▸ MOTOR MAGALU
#
# Fluxo:
#   1. Desencurta (maga.lu, encurtadores)
#   2. Confirma que é magazineluiza.com.br
#   3. SE já tiver promoter_id/partner_id corretos → PRESERVA (não sobrescreve)
#   4. SE não tiver → aplica parâmetros de comissão
#   5. SE for vitrine/categoria → substitui slug
#   6. Encurta via Cuttly
# ══════════════════════════════════════════════════════════════════════════════

def _aplicar_params_magalu(url_exp: str) -> str:
    """Aplica parâmetros de comissão Magalu. Substitui slug se for vitrine."""
    p    = urlparse(url_exp)
    path = p.path

    # Substitui slug se for vitrine/categoria
    if eh_magalu_vitrine(url_exp):
        path = re.sub(
            r'(/(?:lojas|magazinevoce)/)[^/]+',
            rf'\1{MAGALU_SLUG}',
            path
        )
        log_mgl.debug(f"  Slug vitrine → {path}")

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
    api = f"https://cutt.ly/api/api.php?key={CUTTLY_API_KEY}&short={quote(url, safe='')}"
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
    return url  # fallback: retorna sem encurtar


async def motor_magalu(url: str, sessao: aiohttp.ClientSession) -> str | None:
    log_mgl.debug(f"🔗 {url[:80]}")

    # Etapa 1: Desencurtar (maga.lu sempre precisa; sacola.magazineluiza pode não precisar)
    nl = _netloc(url)
    precisa_expandir = "maga.lu" in nl or nl in _ENCURTADORES
    if precisa_expandir:
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)
        log_mgl.debug(f"  📦 {exp[:80]}")
    else:
        exp = url

    # Etapa 2: Confirma que é Magalu
    if classificar(exp) != "magalu":
        log_mgl.warning(f"  ⚠️ Não é Magalu após expansão: {exp[:60]}")
        return None

    # Etapa 3: Verifica se já tem parâmetros de comissão corretos
    if _magalu_ja_tem_params(exp):
        log_mgl.info(f"  ✅ Magalu já convertida (params preservados): {exp[:80]}")
        # Só encurta, não sobrescreve
        final = await _cuttly(exp, sessao)
        return final

    # Etapa 4: Aplica parâmetros de comissão
    url_com_params = _aplicar_params_magalu(exp)
    log_mgl.debug(f"  🏷 Params aplicados: {url_com_params[:80]}")

    # Etapa 5: Encurta via Cuttly
    final = await _cuttly(url_com_params, sessao)
    log_mgl.info(f"  ✅ {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 10 ▸ PIPELINE DE CONVERSÃO — PARALELO, COM WHITELIST
# ══════════════════════════════════════════════════════════════════════════════

async def _converter_um(url: str, sessao: aiohttp.ClientSession) -> tuple:
    """
    Converte 1 link. Whitelist aplicada antes de qualquer motor.
    Encurtadores genéricos são expandidos e reclassificados.
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

    if plat == "expandir":
        # Encurtador genérico → expande → reclassifica
        log_lnk.debug(f"🔄 Expandindo encurtador: {url[:70]}")
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
        log_lnk.info(f"🗑 Expandido mas não na whitelist: {exp[:70]}")
        return None, None

    # plat == None → domínio fora da whitelist
    log_lnk.info(f"🗑 Fora da whitelist: {url[:70]}")
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
# MÓDULO 11 ▸ EMOJIS CONTEXTUAIS INTELIGENTES
#
# Emojis específicos por tipo de produto (whey💪, tênis👟, álbum📚, etc.)
# Emojis por contexto de linha (preço, cupom, frete, relâmpago, etc.)
# Variação automática — nunca repete na mesma mensagem
# ══════════════════════════════════════════════════════════════════════════════

# Pools por contexto de linha
_E_TITULO    = ["🔥","💥","🚨","⚡","✨","🎯","💣","🏆","🔝","💎"]
_E_PRECO     = ["✅","💰","💵","🤑","💲","🟢","💸"]
_E_CUPOM     = ["🎟","💸","🏷","🎁","🎪","🎀","🎉"]
_E_FRETE     = ["🚚","🚛","📦","🛻","✈️","🚀"]
_E_RELAMPAGO = ["⚡","⏰","🔥","💥","🚀","🌪️"]
_E_ESTOQUE   = ["📦","🛍","🏪","🏬","🛒"]
_E_ANUNCIO   = ["📢","📣","🔔","📡","💬"]
_E_SHOPEE    = ["🛒","🧡","🛍","🎁"]
_E_AMAZON    = ["📦","🔶","⭐","🎯"]
_E_MAGALU    = ["🔵","🛒","🏬","🎁"]

# Emojis por categoria de produto (detectados no título)
_PROD_EMOJIS = [
    # Saúde e suplementos
    (["whey", "proteína", "proteina", "suplemento", "creatina",
      "bcaa", "colágeno", "vitamina", "omega"], "💪"),
    # Calçados
    (["tênis", "tenis", "sapato", "sandália", "sandalia",
      "sapatênis", "chinelo", "bota", "calçado"], "👟"),
    # Álbum / cards / figurinhas
    (["álbum", "album", "figurinha", "card", "cards",
      "panini", "pokemon", "sticker"], "📚"),
    # Eletrodomésticos
    (["geladeira", "fogão", "fogao", "micro-ondas", "microondas",
      "lavadora", "lava-roupas", "máquina de lavar", "churrasqueira",
      "ar-condicionado", "ventilador"], "🏠"),
    # Celulares e smartphones
    (["celular", "smartphone", "iphone", "samsung galaxy",
      "xiaomi", "motorola", "redmi"], "📱"),
    # Notebooks e computadores
    (["notebook", "computador", "laptop", "pc gamer",
      "desktop", "monitor"], "💻"),
    # TV e entretenimento
    (["smart tv", "televisão", "tv ", "home theater",
      "soundbar", "projetor", "caixa de som", "fone"], "📺"),
    # Cuidados pessoais / beleza
    (["shampoo", "condicionador", "creme", "sérum", "perfume",
      "hidratante", "maquiagem", "batom", "loção", "sabonete"], "💄"),
    # Bebidas
    (["cerveja", "refrigerante", "suco", "energético",
      "whisky", "vinho", "água"], "🥤"),
    # Alimentos
    (["chocolate", "biscoito", "café", "açúcar", "arroz",
      "macarrão", "leite", "queijo", "frango", "carne"], "🍫"),
    # Ferramentas
    (["furadeira", "parafusadeira", "ferramenta", "kit de ferramentas",
      "chave", "alicate"], "🔧"),
    # Games
    (["game", "jogo", "ps5", "xbox", "nintendo",
      "controle", "headset gamer"], "🎮"),
    # Livros
    (["livro", "e-book", "ebook", "literatura",
      "romance", "mangá", "manga"], "📖"),
    # Brinquedos
    (["brinquedo", "boneca", "lego", "pelúcia", "pelucia",
      "carrinho", "quebra-cabeça"], "🧸"),
    # Roupas
    (["camiseta", "camisa", "calça", "vestido", "moletom",
      "jaqueta", "bermuda", "lingerie", "cueca"], "👕"),
    # Bebê
    (["bebê", "bebe", "fraldas", "carrinho de bebê",
      "mamadeira", "chupeta"], "👶"),
    # Pets
    (["pet", "cachorro", "gato", "ração", "aquário",
      "coleira", "arranhador"], "🐾"),
]

_emoji_sessao: set = set()

def _pick(pool: list) -> str:
    disp = [e for e in pool if e not in _emoji_sessao]
    if not disp:
        disp = pool
    e = random.choice(disp)
    _emoji_sessao.add(e)
    return e

def _emoji_produto(titulo: str) -> str | None:
    """Detecta o tipo de produto pelo título e retorna emoji específico."""
    tl = titulo.lower()
    for palavras, emoji in _PROD_EMOJIS:
        if any(p in tl for p in palavras):
            return emoji
    return None

def _emoji_linha(linha: str, plat: str, eh_titulo: bool) -> str | None:
    """Escolhe o emoji certo para a linha com base no contexto."""
    ll = linha.lower()

    if eh_titulo:
        # Tenta emoji de produto específico primeiro
        ep = _emoji_produto(linha)
        if ep:
            return ep
        # Fallback por plataforma
        if plat == "shopee":
            return _pick(_E_SHOPEE)
        if plat == "magalu":
            return _pick(_E_MAGALU)
        return _pick(_E_TITULO)

    # Contexto por conteúdo da linha
    if any(x in ll for x in ["frete grátis","frete gratis","entrega grátis",
                               "entrega gratis","frete free","sem frete"]):
        return _pick(_E_FRETE)
    if any(x in ll for x in ["relâmpago","relampago","flash","acaba hoje",
                               "só hoje","termina em","últimas unidades"]):
        return _pick(_E_RELAMPAGO)
    if any(x in ll for x in ["cupom","cupon","código","codigo","off",
                               "resgate","desconto","coupon"]):
        return _pick(_E_CUPOM)
    if re.search(r'R\$\s?[\d.,]+', linha):
        return _pick(_E_PRECO)
    if any(x in ll for x in ["anúncio","anuncio","publicidade","patrocinado"]):
        return _pick(_E_ANUNCIO)
    if any(x in ll for x in ["estoque","unidades","disponível","disponivel"]):
        return _pick(_E_ESTOQUE)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 12 ▸ FORMATAÇÃO AUTOMÁTICA INTELIGENTE
#
# Regras definitivas:
#  1. Processa linha a linha preservando estrutura original
#  2. Substitui links pelos de afiliado
#  3. Remove linhas de links inválidos (fora da whitelist)
#  4. Remove marcadores técnicos: ML:, :: ML, - ML, etc.
#  5. Cupom: coloca crases `CODIGO` (não aspas)
#  6. Aplica emoji contextual variável
#  7. Remove linhas vazias duplicadas
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
# Código de cupom: sequência de letras maiúsculas/números de 4 a 20 chars
_RE_COD_CUPOM = re.compile(r'(?<![`"\'])\b([A-Z][A-Z0-9_-]{3,19})\b(?![`"\'"])')

# Marcadores técnicos para remover (limpeza visual)
_RE_MARCADORES = re.compile(
    r'^\s*(?:[-:•|]\s*)?(?:ML|MG|AMZ|AMZ:|ML:|MG:)\s*[-:•]?\s*',
    re.I
)
# Remove prefixos ":: ML", "- ML", "ML:", no início de linha
_RE_PREFIXO_LIXO = re.compile(
    r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:)\s*',
    re.I
)


def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI.search(s))


def _aplicar_crases_cupom(linha: str) -> str:
    """
    Coloca crases em torno do código do cupom se ainda não tiver.
    CUPOM: RELAMPAGO10  →  CUPOM: `RELAMPAGO10`
    CUPOM: `RELAMPAGO10`  →  (não altera)
    """
    def _sub(match):
        cod   = match.group(1)
        inicio = match.start(1)
        fim    = match.end(1)
        texto  = match.string
        # Verifica se já há crase antes ou depois
        antes = texto[inicio - 1] if inicio > 0 else ""
        depois = texto[fim] if fim < len(texto) else ""
        if antes == "`" or depois == "`":
            return cod  # já formatado
        return f"`{cod}`"
    return _RE_COD_CUPOM.sub(_sub, linha)


def _limpar_marcadores(linha: str) -> str:
    """Remove marcadores técnicos desnecessários (ML:, :: ML, etc.)"""
    linha = _RE_PREFIXO_LIXO.sub("", linha)
    linha = _RE_MARCADORES.sub("", linha)
    return linha.strip()


def formatar(texto_original: str, mapa: dict, plat: str) -> str:
    """Formata a mensagem linha a linha com emojis dinâmicos e crases no cupom."""
    global _emoji_sessao
    _emoji_sessao = set()  # reseta para cada mensagem

    linhas         = texto_original.split('\n')
    saida          = []
    primeira_texto = True  # próxima linha de texto é o título

    for linha in linhas:
        ls = linha.strip()
        if not ls:
            saida.append("")
            continue

        # ── Linha que é SOMENTE link(s) ───────────────────────────────────
        urls_na_linha = _RE_URL.findall(ls)
        sem_urls      = _RE_URL.sub("", ls).strip()

        if urls_na_linha and not sem_urls:
            novos = []
            for u in urls_na_linha:
                uc = u.rstrip('.,;)>')
                if uc in mapa:
                    novos.append(mapa[uc])
                else:
                    log_fmt.debug(f"🗑 Link fora da whitelist: {uc[:60]}")
                    # link não convertido = fora da whitelist = removido
            if novos:
                saida.extend(novos)
            # Se não sobrou nada → linha removida (link inválido)
            continue

        # ── Linha com texto (pode ter link inline) ────────────────────────
        nova = ls

        def _sub_link(match):
            uc = match.group(0).rstrip('.,;)>')
            if uc in mapa:
                return mapa[uc]
            log_fmt.debug(f"🗑 Link inline removido: {uc[:60]}")
            return ""

        nova = _RE_URL.sub(_sub_link, nova).strip()
        if not nova:
            continue

        # ── Limpeza de marcadores técnicos ────────────────────────────────
        nova = _limpar_marcadores(nova)
        if not nova:
            continue

        # ── Crases no código do cupom (não aspas) ─────────────────────────
        if _RE_CUPOM_KW.search(nova) or _RE_COD_CUPOM.search(nova):
            nova = _aplicar_crases_cupom(nova)

        # ── Emoji contextual (só onde não há emoji próprio) ───────────────
        if not _tem_emoji(nova) and not _RE_URL.match(nova):
            emoji = _emoji_linha(nova, plat, eh_titulo=primeira_texto)
            if emoji:
                nova = f"{emoji} {nova}"
            if primeira_texto:
                primeira_texto = False
        elif _tem_emoji(nova) and not _RE_URL.match(nova):
            if primeira_texto:
                primeira_texto = False

        saida.append(nova)

    # Remove linhas vazias consecutivas (máx 1)
    final, prev_v = [], False
    for l in saida:
        if l.strip() == "":
            if not prev_v:
                final.append("")
            prev_v = True
        else:
            prev_v = False
            final.append(l)

    resultado = "\n".join(final).strip()
    log_fmt.debug(f"✅ {len(resultado)} chars formatados")
    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 13 ▸ IMAGEM — DETECÇÃO DE MARCA D'ÁGUA + FALLBACK
# ══════════════════════════════════════════════════════════════════════════════

def _processar_imagem_bytes(dados: bytes) -> bytes:
    """Crop central 90% para remover marcas d'água nas bordas."""
    if not _PIL_OK:
        return dados
    try:
        img = Image.open(io.BytesIO(dados)).convert("RGB")
        w, h = img.size
        mx, my = int(w * 0.05), int(h * 0.05)
        cropped = img.crop((mx, my, w - mx, h - my))
        buf = io.BytesIO()
        cropped.save(buf, format="JPEG", quality=92)
        log_img.info(f"✅ Imagem processada: {w}x{h} → {cropped.size[0]}x{cropped.size[1]}")
        return buf.getvalue()
    except Exception as e:
        log_img.error(f"❌ Processar imagem: {e}")
        return dados

def _tem_marca_dagua(dados: bytes) -> bool:
    if not _PIL_OK:
        return False
    try:
        img    = Image.open(io.BytesIO(dados)).convert("RGBA")
        w, h   = img.size
        pixels = img.load()
        count  = sum(
            1 for x in range(max(0, w - 80), w)
            for y in range(max(0, h - 40), h)
            if pixels[x, y][3] < 200 and sum(pixels[x, y][:3]) > 400
        )
        if count > 50:
            log_img.info(f"🔍 Marca d'água detectada ({count} px)")
            return True
        return False
    except Exception:
        return False

async def _baixar_bytes(url: str) -> bytes | None:
    try:
        async with aiohttp.ClientSession(
            headers={"User-Agent": random.choice(USER_AGENTS)}
        ) as s:
            async with s.get(url, allow_redirects=True,
                              timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status == 200:
                    return await r.read()
    except Exception as e:
        log_img.warning(f"⚠️ Download imagem: {e}")
    return None

async def preparar_imagem(media_or_url, tem_real: bool):
    """Retorna (objeto_para_envio, eh_bytes)."""
    if not media_or_url:
        return None, False
    if tem_real:
        return media_or_url, False  # objeto Telethon → envia direto
    if isinstance(media_or_url, str):
        dados = await _baixar_bytes(media_or_url)
        if not dados:
            return None, False
        if _tem_marca_dagua(dados):
            dados = _processar_imagem_bytes(dados)
        return io.BytesIO(dados), True
    return media_or_url, False

async def buscar_imagem(url: str) -> str | None:
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
                        log_img.info(f"✅ Imagem t={t}: {tag['content'][:70]}")
                        return tag["content"]
        except asyncio.TimeoutError:
            log_img.warning(f"⏱ t={t}")
        except Exception as e:
            log_img.warning(f"⚠️ t={t}: {e}")
        await asyncio.sleep(1.5 * t)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ DEDUPLICAÇÃO — 4 CAMADAS SEMÂNTICAS
# (texto é prioridade; link nunca é critério principal)
# ══════════════════════════════════════════════════════════════════════════════

_TTL        = 120 * 60
_JANELA     = 900
_SIM_2      = 0.85
_SIM_3      = 0.92
_SIM_4      = 0.78

_RUIDO = {
    "promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora",
    "relampago","relâmpago","click","clique","veja","confira",
}
_KW_FRETE  = frozenset(["frete","entrega","shipping","free"])
_KW_CUPOM  = frozenset(["cupom","cupon","coupon","codigo","code"])
_KW_SHOPEE = frozenset(["shopee","sacola","laranja"])
_KW_AMAZON = frozenset(["amazon","prime","amzn"])
_KW_MAGALU = frozenset(["magalu","magazine","luiza"])

def _rm_ac(t: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', t)
                   if unicodedata.category(c) != 'Mn')

def _norm(texto: str) -> str:
    t = _rm_ac(texto.lower())
    t = re.sub(r"http\S+|www\S+", " ", t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return " ".join(sorted(w for w in t.split() if w not in _RUIDO))

def _ctx(tnorm: str) -> frozenset:
    p = set(tnorm.split())
    c = set()
    for kw, nome in [(_KW_FRETE,"frete"),(_KW_CUPOM,"cupom"),
                     (_KW_SHOPEE,"shopee"),(_KW_AMAZON,"amazon"),
                     (_KW_MAGALU,"magalu")]:
        if p & kw:
            c.add(nome)
    return frozenset(c)

def deve_enviar(plat: str, prod: str, preco: str,
                cupom: str = "", texto: str = "") -> bool:
    cache = ler_cache()
    agora = time.time()
    cache = {k: v for k, v in cache.items() if agora - v.get("ts", 0) < _TTL}

    tnorm = _norm(texto)
    cnorm = cupom.strip().upper()
    ctx   = _ctx(tnorm)
    h     = hashlib.sha256(f"{plat}|{prod}|{preco}|{cnorm}|{tnorm}".encode()).hexdigest()

    # C1 — hash exato
    if h in cache:
        log_dedup.info(f"🔁 [C1] cupom={cnorm}")
        return False

    for e in cache.values():
        if agora - e.get("ts", 0) >= _JANELA:
            continue
        sim     = SequenceMatcher(None, tnorm, e.get("txt", "")).ratio()
        c_igual = e.get("cupom", "") == cnorm.lower()
        p_igual = str(e.get("prod")) == str(prod) and prod != "0"
        r_igual = str(e.get("preco")) == str(preco) and preco != "0"
        ctx_e   = frozenset(e.get("ctx", []))

        # C2 — mesmo produto/preço/cupom + texto similar
        if p_igual and r_igual and c_igual and sim >= _SIM_2:
            log_dedup.info(f"🔁 [C2] sim={sim:.2f}")
            return False
        # C3 — cupom igual + texto quase idêntico
        if c_igual and sim >= _SIM_3:
            log_dedup.info(f"🔁 [C3] sim={sim:.2f}")
            return False
        # C4 — contexto semântico idêntico + texto moderadamente similar
        if ctx and ctx_e and ctx == ctx_e and sim >= _SIM_4:
            log_dedup.info(f"🔁 [C4] ctx={ctx} sim={sim:.2f}")
            return False

    cache[h] = {
        "plat": plat, "prod": str(prod), "preco": str(preco),
        "cupom": cnorm.lower(), "txt": tnorm,
        "ctx": list(ctx), "ts": agora,
    }
    salvar_cache(cache)
    log_dedup.debug(f"✅ Nova | plat={plat}")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 15 ▸ FINGERPRINT
# ══════════════════════════════════════════════════════════════════════════════

def _prod_id(mapa: dict) -> str:
    if not mapa:
        return "0"
    p = list(mapa.values())[0]
    for pat in [r'/dp/([A-Z0-9]{10})',r'/gp/product/([A-Z0-9]{10})',
                r'/i\.(\d+\.\d+)',r'/p/(\d+)/',r'/product/(\d+)']:
        m = re.search(pat, p)
        if m:
            return m.group(1)
    return p[-20:]

def _preco(t: str) -> str:
    m = _RE_PRECO.search(t)
    return m.group(0).strip() if m else "0"

def _cupom(t: str) -> str:
    m = re.search(r'\b([A-Z][A-Z0-9_-]{3,19})\b', t)
    return m.group(1) if m else ""

def _eh_cupom(t: str) -> bool:
    return bool(_RE_CUPOM_KW.search(t))

def _tem_midia(media) -> bool:
    return media is not None and not isinstance(media, MessageMediaWebPage)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 16 ▸ RATE-LIMIT INTERNO
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
# MÓDULO 17 ▸ ANTI-LOOP DE EDIÇÃO
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
# MÓDULO 18 ▸ ENVIO — SEMPRE 1 MENSAGEM (IMAGEM + TEXTO JUNTOS)
# ══════════════════════════════════════════════════════════════════════════════

async def _enviar(msg: str, img_obj) -> object:
    """
    1 mensagem só, sempre.
    - Com imagem E texto <= 1024 chars → send_file + caption
    - Texto > 1024 ou sem imagem → send_message + link_preview
    """
    if img_obj and len(msg) <= 1024:
        try:
            return await client.send_file(
                GRUPO_DESTINO, img_obj,
                caption=msg, parse_mode="md",
            )
        except Exception as e:
            log_tg.warning(f"⚠️ send_file falhou ({e}), tentando sem imagem")
    return await client.send_message(
        GRUPO_DESTINO, msg,
        parse_mode="md", link_preview=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 19 ▸ PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

async def processar(event, is_edit: bool = False):
    msg_id = event.message.id
    texto  = event.message.text or ""
    chat   = await event.get_chat()
    uname  = (chat.username or str(event.chat_id)).lower()

    log_tg.info(f"{'✏️ EDIT' if is_edit else '📩 NEW'} | @{uname} | id={msg_id} | {len(texto)}c")

    if not texto.strip():
        return

    # E2: Anti-loop
    if not is_edit:
        if await _processado(msg_id):
            log_sys.debug(f"⏩ Anti-loop id={msg_id}")
            return
    else:
        loop  = asyncio.get_event_loop()
        mapa_c = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        if str(msg_id) not in mapa_c:
            log_sys.debug(f"⏩ Edit ignorada (preview?): id={msg_id}")
            return

    # E3: Filtro de texto
    if texto_bloqueado(texto):
        log_fil.warning(f"🚫 @{uname}")
        return

    # E4: Extrai links
    links_raw = [l.rstrip('.,;)>') for l in _RE_URL.findall(texto)]
    log_lnk.info(f"🔗 {len(links_raw)} link(s)")

    if not links_raw and "fadadoscupons" not in uname:
        log_sys.debug("⏩ Sem links")
        return

    # E5: Conversão paralela com whitelist
    mapa_links, plat_p = {}, "amazon"
    if links_raw:
        mapa_links, plat_p = await converter_links(links_raw)

    if links_raw and not mapa_links:
        log_sys.warning(f"🚫 Zero links na whitelist | @{uname}")
        return

    # E6: Deduplicação semântica
    prod = _prod_id(mapa_links)
    prec = _preco(texto)
    cup  = _cupom(texto)
    log_dedup.debug(f"🔬 prod={prod} preco={prec} cupom={cup} plat={plat_p}")

    if not is_edit:
        if not deve_enviar(plat_p, prod, prec, cup, texto):
            log_dedup.info("🚫 Duplicata")
            return

    # E7: Formatação com emojis dinâmicos e crases no cupom
    msg_final = formatar(texto, mapa_links, plat_p)
    log_fmt.debug(f"📝\n{msg_final[:400]}")

    # E8: Imagem
    media_orig = event.message.media
    tem_img    = _tem_midia(media_orig)
    img_obj    = None

    if _eh_cupom(texto):
        if tem_img:
            img_obj, _ = await preparar_imagem(media_orig, True)
        elif plat_p == "shopee":
            img_obj = IMG_SHOPEE if os.path.exists(IMG_SHOPEE) else None
        elif plat_p == "magalu":
            img_obj = IMG_MAGALU if os.path.exists(IMG_MAGALU) else None
        else:
            img_obj = IMG_AMAZON if os.path.exists(IMG_AMAZON) else None
    else:
        if tem_img:
            img_obj, _ = await preparar_imagem(media_orig, True)
        elif mapa_links:
            img_url = await buscar_imagem(list(mapa_links.values())[0])
            if img_url:
                img_obj, _ = await preparar_imagem(img_url, False)

    # E9: Rate-limit
    await _rate_limit()

    # E10: Envio / Edição com retry
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
                        log_tg.debug("⏩ Idêntico")
                        break
                    except FloodWaitError as e:
                        log_tg.warning(f"⏳ FW: {e.seconds}s")
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        log_tg.error(f"❌ t={t}: {e}")
                        if t < 3: await asyncio.sleep(2 ** t)
                return

            sent = None
            for t in range(1, 4):
                try:
                    sent = await _enviar(msg_final, img_obj)
                    break
                except FloodWaitError as e:
                    log_tg.warning(f"⏳ FW: {e.seconds}s")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    log_tg.error(f"❌ Envio t={t}: {e}")
                    if t == 1: img_obj = None
                    elif t < 3: await asyncio.sleep(2 ** t)

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
# MÓDULO 20 ▸ HEALTH CHECK (a cada 5 min)
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
# MÓDULO 21 ▸ INICIALIZAÇÃO COM AUTO-RESTART
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
    log_sys.info(f"🏷  Amazon={AMAZON_TAG} | Shopee={SHOPEE_APP_ID}")
    log_sys.info(f"🏪  Magalu promo={MAGALU_PROMOTER_ID} | Slug={MAGALU_SLUG}")
    log_sys.info(f"🖼  Pillow={'OK' if _PIL_OK else 'NÃO (pip install Pillow)'}")
    log_sys.info("🚀 FOGUETÃO v67.0 ONLINE!")

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
            log_sys.error(f"💥 Bot caiu: {e} — restart em 15s...", exc_info=True)
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())

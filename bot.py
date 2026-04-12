"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   FOGUETÃO v71.0 — MÓDULO 14 REESCRITO: RENDERIZADOR LINEAR               ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  MUDANÇA CIRÚRGICA v71 — MÓDULO 14 APENAS:                                 ║
║                                                                             ║
║  ANTES (v70): reconstrução total — separava título/preço/links em buckets  ║
║    e remontava → QUEBRAVA LISTAS LONGAS (10 produtos JBL misturados)        ║
║                                                                             ║
║  AGORA (v71): processamento LINEAR linha por linha — espelho do original   ║
║    • Linha de link     → substitui pelo convertido NA MESMA POSIÇÃO        ║
║    • Linha de ruído    → remove sem mover nada                             ║
║    • Linha de anúncio  → remove e coloca 1 único '#anúncio' no final       ║
║    • Linha de texto    → mantém posição + emoji se não tiver               ║
║    • 🛒 Shopee/Magalu → só adiciona se 'carrinho' não estiver na linha     ║
║                                                                             ║
║  RESULTADO: lista de 10 produtos Amazon fica com cada link abaixo do seu  ║
║  título, exatamente como no canal de origem.                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os, re, time, json, asyncio, aiohttp, hashlib, random, io
import unicodedata, logging, concurrent.futures
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

# ─── Credenciais ISOLADAS por plataforma — NUNCA misturar ────────────────────
_AMZ_TAG    = os.environ.get("AMAZON_TAG",    "leo21073-20")   # só para amazon.com.br
_SHP_APP_ID = os.environ.get("SHOPEE_APP_ID", "18348480261")
_SHP_SECRET = os.environ.get("SHOPEE_SECRET", "SGC7FQQQ4R5QCFULPXIBCANATLP272B3")
_MGL_PARTNER  = os.environ.get("MAGALU_PARTNER_ID",  "3440")
_MGL_PROMOTER = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
_MGL_PID      = os.environ.get("MAGALU_PID",         "magazinevoce")
_MGL_SLUG     = os.environ.get("MAGALU_SLUG",        "magazineleo12")
_CUTTLY_KEY   = os.environ.get("CUTTLY_API_KEY",     "8d2afd3c7f72869f42d23cf0d849c72172509")

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
# MÓDULO 5 ▸ WHITELIST + CLASSIFICAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

_WHITELIST: dict[str, str] = {
    "amazon.com.br": "amazon", "amzn.to": "amazon",
    "amzn.com": "amazon",      "a.co": "amazon",
    "shopee.com.br": "shopee", "s.shopee.com.br": "shopee",
    "shopee.com": "shopee",    "shope.ee": "shopee",
    "magazineluiza.com.br": "magalu", "sacola.magazineluiza.com.br": "magalu",
    "magazinevoce.com.br": "magalu",  "maga.lu": "magalu",
}

# Qualquer encurtador desconhecido → expande e reclassifica
_ENCURTADORES = frozenset([
    "bit.ly", "cutt.ly", "tinyurl.com", "t.co", "ow.ly",
    "goo.gl", "rb.gy", "is.gd", "tiny.cc", "buff.ly",
    "short.io", "bl.ink", "rebrand.ly", "shorturl.at",
])

# Links preservados sem processamento (WhatsApp, etc.)
_PRESERVE = frozenset(["wa.me", "chat.whatsapp.com", "api.whatsapp.com"])

def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""

def classificar(url: str) -> Optional[str]:
    """
    Retorna: 'amazon' | 'shopee' | 'magalu' | 'preservar' | 'expandir' | None

    REGRA DE ISOLAMENTO ESTANQUE:
    - Verificação é feita por SUFIXO de domínio (netloc termina com o domínio ou é igual).
    - Magalu é verificado ANTES de Amazon e Shopee para evitar qualquer ambiguidade.
    - Um link magazineluiza.com.br NUNCA passa pelo motor Amazon.
    - Um link amazon.com.br NUNCA passa pelo motor Magalu.
    """
    nl = _netloc(url)
    if not nl:
        return None

    # Preservar primeiro (WhatsApp / wa.me)
    for d in _PRESERVE:
        if nl == d or nl.endswith("." + d):
            return "preservar"

    # MAGALU verificado ANTES de amazon/shopee — evita cross-linking
    for dom in ("magazineluiza.com.br", "sacola.magazineluiza.com.br",
                "magazinevoce.com.br", "maga.lu"):
        if nl == dom or nl.endswith("." + dom):
            log_lnk.debug(f"🔵 MAGALU: {nl}")
            return "magalu"

    # AMAZON — somente domínios exatos da Amazon
    for dom in ("amazon.com.br", "amzn.to", "amzn.com", "a.co"):
        if nl == dom or nl.endswith("." + dom):
            log_lnk.debug(f"🟠 AMAZON: {nl}")
            return "amazon"

    # SHOPEE
    for dom in ("shopee.com.br", "s.shopee.com.br", "shopee.com", "shope.ee"):
        if nl == dom or nl.endswith("." + dom):
            log_lnk.debug(f"🟣 SHOPEE: {nl}")
            return "shopee"

    # Encurtadores genéricos → expandir e reclassificar
    for enc in _ENCURTADORES:
        if nl == enc or nl.endswith("." + enc):
            return "expandir"

    log_lnk.debug(f"🗑 Fora whitelist: {nl}")
    return None

def _eh_vitrine_magalu(url: str) -> bool:
    return bool(re.search(
        r'(magazineluiza\.com\.br|magazinevoce\.com\.br)'
        r'.*(vitrine|categoria|lista|promo|stores|loja)',
        url, re.I))


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 6 ▸ DESENCURTADOR ULTRA-FORTE
# Segue 301/302/307/308, meta-refresh, JS location, e cadeia completa.
# Suporta qualquer encurtador: cutt.ly → bit.ly → magalu → produto final.
# ══════════════════════════════════════════════════════════════════════════════

async def desencurtar_ultra(url: str, sessao: aiohttp.ClientSession,
                             depth: int = 0) -> str:
    """
    Desencurta até o destino final. Segue qualquer tipo de redirect.
    Chamado por: motor_amazon (amzn.to), motor_magalu (maga.lu/qualquer),
                 pipeline (encurtadores genéricos).
    Motor Shopee NUNCA chama esta função.
    """
    if depth > 15:
        log_lnk.warning(f"⚠️ Prof. max ({depth}): {url[:60]}")
        return url

    hdrs = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate",
    }

    try:
        # Tentativa 1: HEAD para seguir redirects HTTP rapidamente
        try:
            async with sessao.head(
                url, headers=hdrs, allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=10),
                max_redirects=20
            ) as r:
                final = str(r.url)
                if final != url:
                    log_lnk.debug(f"  HEAD d={depth} → {final[:70]}")
                    return await desencurtar_ultra(final, sessao, depth + 1)
                return final
        except Exception:
            pass  # Alguns servidores bloqueiam HEAD → cai no GET

        # Tentativa 2: GET completo — lê HTML para meta-refresh e JS
        async with sessao.get(
            url, headers=hdrs, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=15),
            max_redirects=20
        ) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")
            soup = BeautifulSoup(html, "html.parser")

            # Meta refresh: <meta http-equiv="refresh" content="0;url=...">
            ref = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)", ref["content"], re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    log_lnk.debug(f"  META d={depth} → {novo[:70]}")
                    return await desencurtar_ultra(novo, sessao, depth + 1)

            # JS: window.location = "..." ou window.location.href = "..."
            for pat in [
                r'window\.location(?:\.href)?\s*=\s*["\']([^"\']+)["\']',
                r'location\.replace\(["\']([^"\']+)["\']\)',
                r'location\.assign\(["\']([^"\']+)["\']\)',
            ]:
                mj = re.search(pat, html)
                if mj:
                    log_lnk.debug(f"  JS d={depth} → {mj.group(1)[:70]}")
                    return await desencurtar_ultra(mj.group(1), sessao, depth + 1)

            # Link canônico como fallback
            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href") and canon["href"] != url:
                log_lnk.debug(f"  CANON d={depth} → {canon['href'][:70]}")
                return await desencurtar_ultra(canon["href"], sessao, depth + 1)

            if pos != url:
                return await desencurtar_ultra(pos, sessao, depth + 1)
            return pos

    except asyncio.TimeoutError:
        log_lnk.warning(f"⏱ Timeout: {url[:70]}")
        return url
    except Exception as e:
        log_lnk.error(f"❌ Desencurtar {url[:60]}: {e}")
        return url


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 7 ▸ MOTOR AMAZON (ISOLADO)
#
# Limpeza inteligente por tipo de URL:
#  /dp/ASIN              → amazon.com.br/dp/ASIN?tag=
#  /gp/product/ASIN      → amazon.com.br/dp/ASIN?tag=
#  /promotion/psp/ID     → amazon.com.br/promotion/psp/ID?tag=
#  /events/... ou /b?    → mantém path + tag (sem params lixo)
#  Qualquer outro path   → remove params lixo, injeta tag
#
# A tag _AMZ_TAG é injetada SOMENTE aqui — em nenhuma outra função.
# ══════════════════════════════════════════════════════════════════════════════

# Parâmetros que SEMPRE são removidos de URLs Amazon
_AMZ_LIXO = frozenset({
    "ascsubtag","btn_ref","ref_","ref","smid","sprefix","sr","spla",
    "dchild","linkcode","linkid","camp","creative","pf_rd_p","pf_rd_r",
    "pd_rd_wg","pd_rd_w","content-id","pd_rd_r","pd_rd_i","ie","qid",
    "_encoding","dib","dib_tag","m","marketplaceid","ufe","th","psc",
    "ingress","visitid","lp_context_asin","s",
    # Específicos de campanhas e promoções
    "redirectasin","redirectmerchantid","redirectasincustomeraction",
})

# Parâmetros mantidos quando presentes
_AMZ_MANTER = frozenset({"tag", "keywords", "node", "k", "i", "rh"})


def _limpar_url_amazon(url_bruta: str) -> str:
    """
    Limpeza inteligente da URL Amazon por tipo de conteúdo.
    *** ÚNICA função que usa _AMZ_TAG ***

    Tipos tratados:
      - Produto (/dp/ ou /gp/product/): mantém só ASIN + tag
      - Campanha (/promotion/psp/): mantém path + tag (sem redirectAsin, etc.)
      - Evento/busca (/events/, /b?, /s?): mantém path + params uteis + tag
      - Genérico: remove lixo + tag
    """
    p    = urlparse(url_bruta)
    path = p.path

    # ── Tipo 1: Produto ──────────────────────────────────────────────────────
    m_dp = re.match(r'(/dp/[A-Z0-9]{10})', path)
    m_gp = re.match(r'(/gp/product/[A-Z0-9]{10})', path)
    if m_dp:
        # /dp/ASIN → limpa tudo, só tag
        return urlunparse(p._replace(
            path=m_dp.group(1),
            query=f"tag={_AMZ_TAG}", fragment=""
        ))
    if m_gp:
        # /gp/product/ASIN → converte para /dp/ASIN
        asin = m_gp.group(1).split("/")[-1]
        return urlunparse(p._replace(
            path=f"/dp/{asin}",
            query=f"tag={_AMZ_TAG}", fragment=""
        ))

    # ── Tipo 2: Campanha/promoção (/promotion/psp/) ──────────────────────────
    if "/promotion/psp/" in path:
        # Mantém path exato da campanha, somente tag — remove todo o resto
        return urlunparse(p._replace(
            query=f"tag={_AMZ_TAG}", fragment=""
        ))

    # ── Tipo 3: Eventos / buscas / landing pages ─────────────────────────────
    # (/events/, /b?, /s?, /deals, /stores, /gp/goldbox, etc.)
    # Remove params de rastreamento mas mantém params funcionais
    params = {}
    for k, v in parse_qs(p.query, keep_blank_values=False).items():
        kl = k.lower()
        if kl in _AMZ_MANTER:
            params[k] = v
        elif kl not in _AMZ_LIXO and len(v[0]) < 50:
            params[k] = v
    params["tag"] = [_AMZ_TAG]
    return urlunparse(p._replace(query=urlencode(params, doseq=True), fragment=""))


async def motor_amazon(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """Motor exclusivo Amazon. Nenhuma var de Shopee ou Magalu é usada aqui."""
    nl = _netloc(url)

    if any(d in nl for d in ("amzn.to", "a.co", "amzn.com")):
        # Encurtado → DESENCURTA primeiro
        log_amz.debug(f"🔗 Expandindo: {url[:80]}")
        async with _SEM_HTTP:
            exp = await desencurtar_ultra(url, sessao)
        log_amz.debug(f"  📦 {exp[:80]}")
        if classificar(exp) != "amazon":
            log_amz.warning(f"  ⚠️ Não é Amazon: {exp[:60]}")
            return None
        final = _limpar_url_amazon(exp)
    elif any(d in nl for d in ("amazon.com.br", "amazon.com")):
        # URL direta → limpa direto sem desencurtar
        log_amz.debug(f"🔗 Direta: {url[:80]}")
        final = _limpar_url_amazon(url)
    else:
        log_amz.warning(f"  ⚠️ Domínio inválido: {nl}")
        return None

    log_amz.info(f"  ✅ {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ MOTOR SHOPEE (ISOLADO)
# Shopee chega pronta. NUNCA chama desencurtar_ultra().
# API direta com retry 3x. Somente _SHP_APP_ID / _SHP_SECRET.
# ══════════════════════════════════════════════════════════════════════════════

async def motor_shopee(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """Motor exclusivo Shopee. Não usa nenhuma var de Amazon nem Magalu."""
    log_shp.debug(f"🔗 {url[:80]}")
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
    log_shp.error("  ❌ Shopee API falhou 3x")
    return url  # URL original Shopee ainda é válida


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 9 ▸ MOTOR MAGALU (ISOLADO)
# Desencurta qualquer link Magalu (maga.lu, cutt.ly, bit.ly apontando p/ MGL).
# SEMPRE sobrescreve parâmetros com os do sistema.
# Somente _MGL_* e _CUTTLY_KEY são usados aqui.
# ══════════════════════════════════════════════════════════════════════════════

def _construir_url_magalu(url_exp: str) -> str:
    """
    Constrói URL Magalu com parâmetros do sistema.
    PASSO 1: limpa TODOS os parâmetros da URL original (remove restos de outros afiliados).
    PASSO 2: reconstrói com parâmetros de comissão 100% do sistema.
    NUNCA herda parâmetros externos — URL de saída é sempre limpa.
    """
    p    = urlparse(url_exp)
    path = p.path

    # Substitui slug se for vitrine/categoria
    if _eh_vitrine_magalu(url_exp):
        path = re.sub(r'(/(?:lojas|magazinevoce)/)[^/]+', rf'\1{_MGL_SLUG}', path)
        log_mgl.debug(f"  Slug vitrine → {path}")

    # URL base absolutamente limpa (zero params do original)
    base = urlunparse(p._replace(path=path, query="", fragment=""))

    # deeplink também usa a URL base limpa
    deeplink = (f"{base}?utm_source=divulgador&utm_medium=magalu"
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
    """
    Encurta via Cuttly com encoding correto e tratamento de erro detalhado.
    A URL é double-encoded para evitar falha em URLs com caracteres especiais.
    Retorna a URL original se Cuttly falhar (fail-safe).
    """
    url_encoded = quote(url, safe="")
    api = f"https://cutt.ly/api/api.php?key={_CUTTLY_KEY}&short={url_encoded}"
    try:
        async with _SEM_HTTP:
            async with sessao.get(api, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    log_mgl.warning(f"  ⚠️ Cuttly HTTP {r.status}")
                    return url
                try:
                    data = await r.json(content_type=None)
                except Exception:
                    raw = await r.text()
                    log_mgl.warning(f"  ⚠️ Cuttly JSON inválido: {raw[:80]}")
                    return url

                status = data.get("url", {}).get("status")
                # Status 7 = sucesso
                if status == 7:
                    short = data["url"]["shortLink"]
                    log_mgl.info(f"  ✂️ Cuttly: {short}")
                    return short
                # Status 2 = URL já encurtada (retorna o link existente)
                if status == 2:
                    existing = data["url"].get("shortLink", url)
                    log_mgl.info(f"  ✂️ Cuttly (existente): {existing}")
                    return existing
                log_mgl.warning(f"  ⚠️ Cuttly status={status} | url={url[:60]}")
    except asyncio.TimeoutError:
        log_mgl.warning(f"  ⏱ Cuttly timeout | url={url[:60]}")
    except Exception as e:
        log_mgl.error(f"  ❌ Cuttly erro: {e} | url={url[:60]}")
    return url  # fail-safe: retorna URL sem encurtar


async def motor_magalu(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """Motor exclusivo Magalu. Desencurta qualquer variante."""
    log_mgl.debug(f"🔗 {url[:80]}")

    # SEMPRE desencurta — garante chegar no domínio final
    nl = _netloc(url)
    precisa = "maga.lu" in nl or nl in _ENCURTADORES
    if precisa or classificar(url) == "expandir":
        async with _SEM_HTTP:
            exp = await desencurtar_ultra(url, sessao)
        log_mgl.debug(f"  📦 {exp[:80]}")
    else:
        exp = url

    if classificar(exp) != "magalu":
        log_mgl.warning(f"  ⚠️ Não-Magalu: {exp[:60]}")
        return None

    url_c = _construir_url_magalu(exp)
    final = await _cuttly(url_c, sessao)
    log_mgl.info(f"  ✅ {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 10 ▸ EXTRAÇÃO INTELIGENTE DE LINKS
#
# Extrai TODOS os links de uma mensagem, separando:
#  - links a preservar (wa.me, WhatsApp)
#  - links a converter (Amazon / Shopee / Magalu)
#  - links a descartar (fora da whitelist)
# ══════════════════════════════════════════════════════════════════════════════

_RE_URL = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c]+')

def extrair_links(texto: str) -> tuple[list, list]:
    """
    Retorna (links_converter, links_preservar).
    links_converter: precisam ser processados pelos motores
    links_preservar: wa.me / WhatsApp → passam sem alteração
    """
    brutos    = [l.rstrip('.,;)>') for l in _RE_URL.findall(texto)]
    converter = []
    preservar = []

    for url in brutos:
        plat = classificar(url)
        if plat == "preservar":
            preservar.append(url)
        elif plat is not None:   # amazon, shopee, magalu, expandir
            converter.append(url)
        # None → fora da whitelist → ignora

    log_lnk.debug(f"🔗 {len(converter)} converter | {len(preservar)} preservar")
    return converter, preservar


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 11 ▸ PIPELINE DE CONVERSÃO PARALELA
# ══════════════════════════════════════════════════════════════════════════════

async def _converter_um(url: str, sessao: aiohttp.ClientSession) -> tuple:
    """
    Converte 1 link. Roteamento ESTANQUE por plataforma.
    GUARD ANTI-CROSS-LINKING: cada URL só pode entrar no motor da sua plataforma.
    """
    plat = classificar(url)

    # ── Rotas diretas ─────────────────────────────────────────────────────────
    if plat == "amazon":
        # Só passa pelo motor Amazon. NUNCA pelo motor Magalu ou Shopee.
        r = await motor_amazon(url, sessao)
        return (r, "amazon") if r else (None, None)

    if plat == "shopee":
        # Só passa pelo motor Shopee. NUNCA pelo motor Amazon ou Magalu.
        r = await motor_shopee(url, sessao)
        return (r, "shopee") if r else (None, None)

    if plat == "magalu":
        # Só passa pelo motor Magalu. NUNCA pelo motor Amazon ou Shopee.
        r = await motor_magalu(url, sessao)
        return (r, "magalu") if r else (None, None)

    # ── Encurtador genérico → expande → reclassifica → roteamento estanque ───
    if plat == "expandir":
        log_lnk.debug(f"🔄 Expandindo: {url[:70]}")
        async with _SEM_HTTP:
            exp = await desencurtar_ultra(url, sessao)
        p2 = classificar(exp)
        log_lnk.debug(f"  Reclassificado como: {p2} | {exp[:60]}")

        if p2 == "amazon":
            r = await motor_amazon(exp, sessao)
            return (r, "amazon") if r else (None, None)
        if p2 == "shopee":
            r = await motor_shopee(exp, sessao)
            return (r, "shopee") if r else (None, None)
        if p2 == "magalu":
            r = await motor_magalu(exp, sessao)
            return (r, "magalu") if r else (None, None)

        log_lnk.info(f"🗑 Não na whitelist após expansão: {exp[:70]}")
        return None, None

    # Fora da whitelist → descarta
    log_lnk.debug(f"🗑 Descartado: {url[:70]}")
    return None, None


async def converter_links(links: list) -> tuple:
    """Converte até 50 links em paralelo. Retorna (mapa, plataforma_principal)."""
    if not links:
        return {}, "amazon"
    log_lnk.info(f"🚀 {len(links)} link(s)")
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

    mapa, plats = {}, []
    for i, res in enumerate(resultados):
        if isinstance(res, Exception):
            log_lnk.error(f"  ❌ [{i}]: {res}")
            continue
        novo, plat = res
        if novo and plat:
            mapa[links[i]] = novo
            plats.append(plat)
            log_lnk.debug(f"  [{plat.upper()}] → {novo[:50]}")

    plat_p = max(set(plats), key=plats.count) if plats else "amazon"
    log_lnk.info(f"✅ {len(mapa)}/{len(links)} | plat={plat_p}")
    return mapa, plat_p


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 12 ▸ LIMPEZA DE RUÍDO TEXTUAL
# Remove anúncio, CTA quebrado, linhas repetidas, espaços desnecessários.
# Chamada ANTES da formatação final.
# ══════════════════════════════════════════════════════════════════════════════

_RE_PRECO    = re.compile(r'R\$\s?[\d.,]+')
_RE_CUPOM_KW = re.compile(
    r'\b(?:cupom|cupon|off|resgate|codigo|coupon|desconto|assine)\b', re.I)
_RE_EMOJI    = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF"
    r"\U0001F900-\U0001F9FF\u2B50\u2B55\u231A\u231B\u25A0-\u25FF]",
    flags=re.UNICODE,
)
_RE_COD_CUPOM = re.compile(r'(?<![`"\'])\b([A-Z][A-Z0-9_-]{3,19})\b(?![`"\'"])')

# Padrões de ruído técnico/visual para remover
_RE_RUIDO_LINHA = re.compile(
    r'^\s*(?:'
    r':::?\s*ML|[-–]\s*ML|ML\s*:|'
    r'[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?|'
    r'-Anúncio$|^Anúncio$|^anuncio$|^-anuncio$|^- Anúncio$'
    r')\s*$',
    re.I
)

def limpar_ruido_textual(texto: str) -> str:
    """
    Limpeza em 3 camadas:

    Camada 1 — Expressões de afiliado inline (re.sub):
      Remove frases como '-Link produto:', 'Resgate aqui:', 'Confira no app',
      marcadores como ':::', '---', 'ML:', ':: ML', etc.
      Aplicado no texto inteiro ANTES de dividir em linhas.

    Camada 2 — Linhas de ruído puro:
      Linhas que ficaram vazias ou só com ruído após camada 1 são removidas.

    Camada 3 — Normalização de espaços:
      Remove caracteres Unicode invisíveis e linhas vazias consecutivas.
    """
    # ── Camada 0: Unicode invisíveis ─────────────────────────────────────
    texto = re.sub(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]', ' ', texto)

    # ── Camada 1: Expressões de afiliado / CTA / marcadores inline ───────
    # Expressões que aparecem no INÍCIO de linha (com ou sem hífen/dois-pontos)
    _exprs_inicio = [
        r'^\s*-?\s*Link\s+produto\s*:?\s*',
        r'^\s*-?\s*Link\s+da\s+oferta\s*:?\s*',
        r'^\s*-?\s*Resgate\s+aqui\s*:?\s*',
        r'^\s*-?\s*Resgate\s+no\s+app\s*:?\s*',
        r'^\s*-?\s*Confira\s+no\s+app\s*:?\s*',
        r'^\s*-?\s*Link\s+no\s+comentário\s*:?\s*',
        r'^\s*-?\s*Compre\s+aqui\s*:?\s*',
        r'^\s*-?\s*Acesse\s+aqui\s*:?\s*',
        r'^\s*-?\s*Clique\s+aqui\s*:?\s*',
        r'^\s*-?\s*Veja\s+aqui\s*:?\s*',
        r'^\s*-?\s*Assine\s+aqui\s*:?\s*',
        r'^\s*-?\s*Saiba\s+mais\s*:?\s*',
        r'^\s*-?\s*Anúncio\s*:?\s*',
        r'^\s*-?\s*anuncio\s*:?\s*',
        r'^\s*-?\s*Publicidade\s*:?\s*',
    ]
    # Marcadores técnicos inline
    _marcadores = [
        r':::\s*(?:ML|MG|AMZ|loja)?\s*',
        r'---+\s*',
        r'===+\s*',
        r'^\s*[-–—]+\s*(?:ML|MG|AMZ)\s*[-–—]*\s*',
        r'^\s*(?:ML|MG|AMZ)\s*:\s*',
        r'^\s*::\s*(?:ML|MG|AMZ)\s*::\s*',
    ]

    # Aplica substituições linha por linha para não quebrar URLs
    linhas = texto.split('\n')
    limpas = []
    for linha in linhas:
        l = linha
        for expr in _exprs_inicio:
            l = re.sub(expr, '', l, flags=re.I | re.M)
        for marc in _marcadores:
            l = re.sub(marc, '', l, flags=re.I | re.M)
        limpas.append(l)

    # ── Camada 2: Remove linhas que viraram ruído puro ────────────────────
    _RE_LINHA_LIXO = re.compile(
        r'^\s*(?:'
        r'-\s*Anúncio|Anúncio|anuncio|-anuncio|'
        r'- Anúncio|-Publicidade|Publicidade|'
        r':::\s*|---+|===+|'
        r'[-–]\s*(?:ML|MG|AMZ)|(?:ML|MG|AMZ)\s*:'
        r')\s*$',
        re.I
    )
    filtradas = [l for l in limpas if not _RE_LINHA_LIXO.match(l.strip())]

    # ── Camada 3: Linhas vazias consecutivas (máx 1) ──────────────────────
    final, pv = [], False
    for l in filtradas:
        if l.strip() == "":
            if not pv:
                final.append("")
            pv = True
        else:
            pv = False
            final.append(l)

    return "\n".join(final).strip()


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 13 ▸ EMOJIS SEMÂNTICOS POR CATEGORIA E PLATAFORMA
# Emojis fixos e determinísticos — não aleatórios.
# Cada plataforma tem seu conjunto. Produto detectado no título ganha emoji.
# ══════════════════════════════════════════════════════════════════════════════

_EMOJI_PLAT = {
    "amazon": {"titulo": "🔥", "preco": "✅", "cupom": "🎟️",
               "frete": "🚚", "anuncio": "📢", "estoque": "📦"},
    "shopee": {"titulo": "🛒", "preco": "💰", "cupom": "🎁",
               "frete": "🚚", "anuncio": "📢", "estoque": "📦"},
    "magalu": {"titulo": "🔵", "preco": "✅", "cupom": "🏷️",
               "frete": "🚚", "anuncio": "📢", "estoque": "🛍️"},
}

# ── Hierarquia de categorias com PESO (maior = maior prioridade) ─────────────
# Cada entrada: (peso, palavras_chave, emoji)
# Quando há conflito (ex: "frango" em ração de pet), a categoria de maior peso vence.
# PET (peso 100) > BEBÊ (95) > ELETRÔNICOS (90) > ALIMENTOS HUMANOS (20)

_CATS_HIERARQUIA = [
    # ── Peso 100 — Alta especificidade (nunca confundir) ─────────────────────
    (100, ["ração","ração gato","ração cachorro","coleira","antipulgas",
           "arranhador","aquário","comedouro","bebedouro pet"], "🐾"),
    (100, ["fraldas","mamadeira","chupeta","berço","carrinho de bebê",
           "enxoval bebê","body bebê"], "👶"),

    # ── Peso 90 — Eletrônicos ─────────────────────────────────────────────────
    (90,  ["celular","smartphone","iphone","galaxy","xiaomi","motorola",
           "redmi","android"], "📱"),
    (90,  ["notebook","laptop","pc gamer","computador","desktop"], "💻"),
    (90,  ["smart tv","televisão","tv 4k","tv 55","soundbar","projetor",
           "home theater"], "📺"),
    (90,  ["monitor gamer","tela gamer","display gamer"], "🎮"),
    (90,  ["ps5","xbox","nintendo","playstation","controle gamer",
           "headset gamer"], "🎮"),

    # ── Peso 80 — Categoria bem definida ─────────────────────────────────────
    (80,  ["whey","proteína","proteina","creatina","bcaa","suplemento",
           "pre-treino","pré-treino","termogênico"], "💪"),
    (80,  ["tênis","tenis","sapato","sandália","sandalia","sapatênis",
           "chinelo","bota calçado"], "👟"),
    (80,  ["meia","meias","soquete"], "🧦"),
    (80,  ["álbum","album","figurinha","panini","pokemon","sticker card"], "📚"),
    (80,  ["shampoo","condicionador","creme facial","sérum","perfume",
           "hidratante corporal","maquiagem","batom"], "💄"),
    (80,  ["furadeira","parafusadeira","ferramenta","alicate","chave inglesa",
           "kit ferramentas"], "🔧"),
    (80,  ["livro","e-book","mangá","literatura","romance"], "📖"),
    (80,  ["brinquedo","boneca","lego","pelúcia","quebra-cabeça"], "🧸"),
    (80,  ["camiseta","camisa","calça","vestido","moletom","jaqueta",
           "blusa","bermuda","cueca","roupa"], "👕"),

    # ── Peso 70 — Eletrodomésticos ───────────────────────────────────────────
    (70,  ["geladeira","fogão","micro-ondas","microondas","lavadora",
           "lava-roupas","máquina de lavar","ar-condicionado"], "🏠"),
    (70,  ["aspirador","purificador","fritadeira","airfryer","cafeteira",
           "liquidificador","batedeira"], "🏠"),

    # ── Peso 50 — Genérico ───────────────────────────────────────────────────
    (50,  ["colágeno","vitamina","omega","suplemento alimentar"], "💊"),

    # ── Peso 20 — Alimentos humanos (baixo peso — perde para pet/bebê) ───────
    (20,  ["cerveja","refrigerante","suco","energético","whisky","vinho",
           "água mineral"], "🥤"),
    (20,  ["chocolate","biscoito","café","açúcar","arroz","leite",
           "queijo","frango","carne","feijão","macarrão"], "🍫"),
]

# Palavras-chave que ANULAM categorias de peso menor
# Ex: se "ração" estiver no título, ignora "frango" como alimento humano
_DOMINADORES = {
    "🐾": ["ração","petshop","pet shop","veterinário","antipulgas"],
    "👶": ["fraldas","mamadeira","enxoval bebê","berço"],
}


def _emoji_produto(titulo: str) -> Optional[str]:
    """
    Detecção de emoji com hierarquia de prioridades.
    Verifica dominadores primeiro para evitar falso-positivo (ração→🍫).
    Depois percorre categorias em ordem decrescente de peso.
    """
    tl = titulo.lower()

    # Verifica dominadores — se detectado, retorna diretamente
    for emoji_dom, kws in _DOMINADORES.items():
        if any(kw in tl for kw in kws):
            log_fmt.debug(f"  Dominador ativo: {emoji_dom} via {[k for k in kws if k in tl]}")
            return emoji_dom

    # Percorre por peso decrescente
    candidatos = []
    for peso, palavras, emoji in _CATS_HIERARQUIA:
        if any(p in tl for p in palavras):
            candidatos.append((peso, emoji))

    if not candidatos:
        return None

    # Retorna o de maior peso
    candidatos.sort(key=lambda x: x[0], reverse=True)
    escolhido = candidatos[0][1]
    log_fmt.debug(f"  Emoji produto: {escolhido} (peso={candidatos[0][0]})")
    return escolhido


def _emoji_de_linha(linha: str, plat: str, eh_titulo: bool) -> Optional[str]:
    """Retorna emoji correto por contexto. Determinístico."""
    ep = _EMOJI_PLAT.get(plat, _EMOJI_PLAT["amazon"])
    ll = linha.lower()

    if eh_titulo:
        ep_prod = _emoji_produto(linha)
        return ep_prod if ep_prod else ep["titulo"]

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
    return None


def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI.search(s))


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ RENDERIZADOR POR PLATAFORMA
# Estado local por chamada — sem estado global compartilhado.
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ RENDERIZADOR LINEAR — ESPELHO DO CANAL DE ORIGEM
#
# PRINCÍPIO: Substituição de conteúdo, NÃO reconstrução de layout.
#
# O bot percorre a mensagem LINHA POR LINHA na ordem original:
#   • Linha de link       → substitui pelo link convertido (posição preservada)
#   • Linha de ruído      → remove
#   • Linha de anúncio    → remove (coloca 1 único no final)
#   • Linha de texto      → mantém posição, enfeita com emoji se não tiver
#
# Resultado: espelho perfeito do canal de origem com links trocados.
# Listas longas (10 produtos da Amazon) ficam com cada link abaixo do título.
# ══════════════════════════════════════════════════════════════════════════════

_RE_LIXO_PREFIXO = re.compile(
    r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:|[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?)\s*',
    re.I)

# Regex para detectar linha de "anúncio" em qualquer variação
_RE_ANUNCIO_LINHA = re.compile(
    r'^\s*[-#]?\s*(?:anúncio|anuncio|publicidade|patrocinado|sponsored)\s*$',
    re.I)

# Regex para detectar se uma linha de texto é APENAS ruído (sem valor comercial)
_RE_LINHA_APENAS_LIXO = re.compile(
    r'^\s*(?:'
    r'[-–—]{2,}|'                          # separadores como --- ou ===
    r'===+|'
    r':::\s*$|'
    r'(?:ML|MG|AMZ)\s*:\s*$|'             # marcadores técnicos sozinhos
    r'::\s*(?:ML|MG|AMZ)\s*::\s*$'
    r')\s*$',
    re.I)


def _aplicar_crases(linha: str) -> str:
    """Coloca crases no código do cupom se ainda não tiver."""
    def _sub(m):
        cod, ini, fim = m.group(1), m.start(1), m.end(1)
        s = m.string
        antes  = s[ini - 1] if ini > 0 else ""
        depois = s[fim]     if fim < len(s) else ""
        if antes == "`" or depois == "`":
            return cod
        return f"`{cod}`"
    return _RE_COD_CUPOM.sub(_sub, linha)


def _enfeitar_texto(linha: str, plat: str, eh_primeira: bool) -> str:
    """
    Adiciona emoji contextual se a linha não tiver nenhum.
    Usa o emoji de produto para a primeira linha, contexto para as demais.
    Linhas que são só URL nunca recebem emoji.
    """
    ls = linha.strip()
    if not ls or _tem_emoji(ls) or _RE_URL.match(ls):
        return linha  # já tem emoji ou é link → não mexe

    ep = _EMOJI_PLAT.get(plat, _EMOJI_PLAT["amazon"])
    ll = ls.lower()

    if eh_primeira:
        # Primeira linha de texto = título do produto
        emoji = _emoji_produto(ls) or ep["titulo"]
        return f"{emoji} {ls}"

    # Demais linhas — emoji por contexto
    if any(x in ll for x in ["frete grátis","frete gratis","entrega grátis",
                               "entrega gratis","sem frete","frete free"]):
        return f"{ep['frete']} {ls}"
    if any(x in ll for x in ["cupom","cupon","código","codigo","off",
                               "resgate","desconto","coupon"]):
        return f"{ep['cupom']} {ls}"
    if _RE_PRECO.search(ls):
        return f"{ep['preco']} {ls}"

    # Linha de texto simples sem contexto claro → não adiciona emoji
    return linha


def _emoji_carrinho_shopee_magalu(url: str, linha_texto: str) -> str:
    """
    Se o link for Shopee ou Magalu e a linha associada não tiver 🛒,
    adiciona 🛒 no início da linha (somente se 'carrinho' não estiver já lá).
    """
    if "carrinho" in linha_texto.lower() or "🛒" in linha_texto:
        return linha_texto  # já tem → não duplica
    plat_link = classificar(url) if url else None
    if plat_link in ("shopee", "magalu"):
        ls = linha_texto.strip()
        if ls and not _RE_URL.match(ls):
            return f"🛒 {ls}"
    return linha_texto


def renderizar(texto: str, mapa_links: dict,
               links_preservar: list, plat: str) -> str:
    """
    Renderizador linear — espelho do canal de origem.

    Percorre LINHA POR LINHA na ordem original:
      1. Ruído puro        → remove
      2. Anúncio           → remove (vai para o final como único '#anúncio')
      3. Linha só de link  → substitui pelo convertido na mesma posição
                             Link fora da whitelist → remove a linha
      4. Texto com link    → substitui links inline, remove links inválidos
      5. Texto puro        → mantém posição + aplica emoji se não tiver

    Estado 100% local — sem variáveis globais modificadas.
    """
    # Mapa completo: links convertidos + links preservados (wa.me, etc.)
    mapa_total = dict(mapa_links)
    for url in links_preservar:
        mapa_total[url] = url

    linhas     = texto.split('\n')
    saida      = []         # linhas de saída na ordem original
    tem_anuncio = False     # rastreia se havia anúncio no original
    eh_primeira_texto = True  # controla emoji do título

    for linha in linhas:
        ls = linha.strip()

        # ── Linha vazia → preserva (máx 1 consecutiva — tratado no final) ─
        if not ls:
            saida.append("")
            continue

        # ── Ruído puro (---, :::, ML: etc.) → remove ─────────────────────
        if _RE_LINHA_APENAS_LIXO.match(ls):
            log_fmt.debug(f"🗑 Ruído puro: {ls[:50]}")
            continue

        # ── Anúncio → registra e remove (voltará no final como único) ─────
        if _RE_ANUNCIO_LINHA.match(ls):
            tem_anuncio = True
            log_fmt.debug(f"📢 Anúncio registrado, removido da posição")
            continue

        # ── Limpa prefixos técnicos da linha ─────────────────────────────
        ls = _RE_LIXO_PREFIXO.sub("", ls).strip()
        if not ls:
            continue

        # ── Extrai URLs da linha ──────────────────────────────────────────
        urls_na_linha = _RE_URL.findall(ls)
        sem_urls      = _RE_URL.sub("", ls).strip()

        # ── Caso A: linha é SOMENTE link(s) ──────────────────────────────
        if urls_na_linha and not sem_urls:
            for u in urls_na_linha:
                uc = u.rstrip('.,;)>')
                if uc in mapa_total:
                    saida.append(mapa_total[uc])
                    log_fmt.debug(f"🔗 Link substituído: {uc[:40]} → {mapa_total[uc][:40]}")
                else:
                    # Link fora da whitelist → linha removida (não polui o post)
                    log_fmt.debug(f"🗑 Link inválido removido: {uc[:60]}")
            continue

        # ── Caso B: linha tem texto + possível link inline ────────────────
        def _sub_inline(m):
            uc = m.group(0).rstrip('.,;)>')
            if uc in mapa_total:
                return mapa_total[uc]
            return ""  # link inválido inline → remove apenas o link

        nova = _RE_URL.sub(_sub_inline, ls).strip()
        if not nova:
            continue

        # Aplica crases em códigos de cupom
        if _RE_CUPOM_KW.search(nova) or _RE_COD_CUPOM.search(nova):
            nova = _aplicar_crases(nova)

        # Enfeita com emoji (mantém posição, só decora)
        nova = _enfeitar_texto(nova, plat, eh_primeira=eh_primeira_texto)

        if eh_primeira_texto and not _RE_URL.match(nova.strip()):
            eh_primeira_texto = False

        saida.append(nova)

    # ── Normaliza linhas vazias consecutivas (máx 1) ─────────────────────
    final, pv = [], False
    for l in saida:
        if l.strip() == "":
            if not pv:
                final.append("")
            pv = True
        else:
            pv = False
            final.append(l)

    # Remove vazia no início/fim
    while final and final[0].strip() == "":
        final.pop(0)
    while final and final[-1].strip() == "":
        final.pop()

    # ── Adiciona 1 único "#anúncio" no final se havia no original ────────
    if tem_anuncio:
        final.append("")
        final.append("#anúncio")

    resultado = "\n".join(final).strip()
    log_fmt.debug(f"✅ Renderizado [{plat.upper()}] {len(resultado)} chars")
    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 15 ▸ DEDUPLICAÇÃO SEMÂNTICA — POR MUDANÇA REAL
#
# CHAVE: plataforma + cupom + campanha detectada
# PREÇO e LINK não são critérios de bloqueio
#
# BLOQUEIA quando:
#  • hash exato (cupom + campanha + texto = idêntico)
#  • cupom igual + texto >= 92% similar (mesma janela de 30 min)
#  • campanha igual + texto >= 78% similar (mesma janela de 30 min)
#
# PERMITE quando:
#  • cupom mudou
#  • campanha mudou
#  • texto mudou significativamente
# ══════════════════════════════════════════════════════════════════════════════

_TTL_CACHE       = 120 * 60   # 120 min de memória geral
_TTL_EVENTO      = 24 * 60 * 60  # 24h para eventos recorrentes (Quiz, Roleta)
_SIM_CUP         = 0.92       # cupom igual + texto similar → bloqueia
_SIM_CAMP        = 0.78       # campanha igual + texto similar → bloqueia
_SIM_EVENTO      = 0.95       # Quiz/Roleta/Missão → sensibilidade máxima
_JANELA          = 30 * 60    # janela padrão de 30 min
_JANELA_EVENTO   = 24 * 60 * 60  # janela de 24h para eventos

_RUIDO_NORM = {
    "promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora",
    "relampago","relâmpago","click","clique","veja","confira","app",
}

# Palavras-chave de eventos recorrentes — sensibilidade máxima (SIM_EVENTO)
_KW_EVENTO = frozenset([
    "quiz","roleta","missão","missao","arena","girar","gire",
    "roda","jogar","jogue","desafio","desafio diário","daily",
])

def _rm_ac(t: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', t)
                   if unicodedata.category(c) != 'Mn')

def _normalizar(texto: str) -> str:
    t = _rm_ac(texto.lower())
    t = re.sub(r"http\S+|www\S+", " ", t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return " ".join(sorted(w for w in t.split() if w not in _RUIDO_NORM))

def _eh_evento(texto: str) -> bool:
    """Retorna True se o texto descreve um evento recorrente (Quiz, Roleta, etc.)"""
    tl = texto.lower()
    return any(kw in tl for kw in _KW_EVENTO)

def _detectar_campanha(texto: str) -> str:
    tl = texto.lower()
    tokens = []
    if "amazon app" in tl or "app amazon" in tl:
        tokens.append("amazon_app")
    if "mastercard" in tl:
        tokens.append("mastercard")
    if "frete" in tl and ("grátis" in tl or "gratis" in tl):
        tokens.append("frete_gratis")
    if "prime" in tl:
        tokens.append("prime")
    if "black friday" in tl or "blackfriday" in tl:
        tokens.append("blackfriday")
    if "shopee" in tl and "frete" in tl:
        tokens.append("shopee_frete")
    # Tokens de evento
    for kw in _KW_EVENTO:
        if kw in tl:
            tokens.append(f"evento_{kw}")
            break
    return "|".join(sorted(tokens)) if tokens else "geral"

def _extrair_cupom(texto: str) -> str:
    m = re.search(r'\b([A-Z][A-Z0-9_-]{3,19})\b', texto)
    return m.group(1) if m else ""

def _extrair_prod_id(mapa: dict) -> str:
    if not mapa:
        return "0"
    p = list(mapa.values())[0]
    for pat in [r'/dp/([A-Z0-9]{10})', r'/gp/product/([A-Z0-9]{10})',
                r'/i\.(\d+\.\d+)', r'/p/(\d+)/']:
        m = re.search(pat, p)
        if m:
            return m.group(1)
    return p[-20:]

def deve_enviar(plat: str, prod: str, cupom: str, texto: str) -> bool:
    """
    Deduplicação semântica inteligente.

    REGRA ESPECIAL PARA EVENTOS (Quiz, Roleta, Missão):
    - Sensibilidade sobe para 0.95 (SIM_EVENTO)
    - Janela de bloqueio aumenta para 24h
    - Qualquer variação pequena no texto é bloqueada

    REGRA GERAL:
    - C1: hash exato → bloqueia
    - C2: cupom igual + texto >= 92% → bloqueia
    - C3: campanha igual + texto >= 78% → bloqueia
    - Plataformas diferentes nunca se bloqueiam
    """
    cache = ler_cache()
    agora = time.time()
    cache = {k: v for k, v in cache.items() if agora - v.get("ts", 0) < max(_TTL_CACHE, _TTL_EVENTO)}

    tnorm    = _normalizar(texto)
    cnorm    = cupom.strip().upper()
    campanha = _detectar_campanha(texto)
    eh_evt   = _eh_evento(texto)

    # Ajusta parâmetros se for evento recorrente
    sim_camp_usar  = _SIM_EVENTO if eh_evt else _SIM_CAMP
    janela_usar    = _JANELA_EVENTO if eh_evt else _JANELA
    ttl_usar       = _TTL_EVENTO if eh_evt else _TTL_CACHE

    if eh_evt:
        log_dedup.debug(f"⚠️ EVENTO detectado — sensibilidade={sim_camp_usar} janela=24h")

    h = hashlib.sha256(f"{plat}|{cnorm}|{campanha}|{tnorm}".encode()).hexdigest()

    # C1: hash exato
    if h in cache:
        log_dedup.info(f"🔁 [C1] | plat={plat} cupom={cnorm} camp={campanha}")
        return False

    for entrada in cache.values():
        if agora - entrada.get("ts", 0) >= janela_usar:
            continue
        if entrada.get("plat") != plat:
            continue
        sim        = SequenceMatcher(None, tnorm, entrada.get("txt", "")).ratio()
        c_igual    = entrada.get("cupom", "") == cnorm.lower()
        camp_igual = entrada.get("camp", "") == campanha

        # C2: cupom igual + texto muito parecido
        if c_igual and sim >= _SIM_CUP:
            log_dedup.info(f"🔁 [C2] cupom={cnorm} sim={sim:.2f}")
            return False

        # C3: campanha igual + texto parecido (sensibilidade varia por tipo)
        if camp_igual and campanha != "geral" and sim >= sim_camp_usar:
            log_dedup.info(
                f"🔁 [C3{'_EVENTO' if eh_evt else ''}] "
                f"camp={campanha} sim={sim:.2f} limiar={sim_camp_usar}")
            return False

    cache[h] = {
        "plat": plat, "prod": str(prod),
        "cupom": cnorm.lower(), "camp": campanha,
        "txt": tnorm, "ts": agora,
        "evento": eh_evt,
    }
    salvar_cache(cache)
    log_dedup.debug(f"✅ Nova | plat={plat} cupom={cnorm} camp={campanha} evento={eh_evt}")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 16 ▸ IMAGEM — WATERMARK + BUSCA + FALLBACK
# ══════════════════════════════════════════════════════════════════════════════

def _processar_bytes(dados: bytes) -> bytes:
    if not _PIL_OK:
        return dados
    try:
        img = Image.open(io.BytesIO(dados)).convert("RGB")
        w, h = img.size
        mx, my = int(w * 0.05), int(h * 0.05)
        c = img.crop((mx, my, w - mx, h - my))
        buf = io.BytesIO()
        c.save(buf, format="JPEG", quality=92)
        return buf.getvalue()
    except Exception as e:
        log_img.error(f"❌ Processar: {e}")
        return dados

def _tem_watermark(dados: bytes) -> bool:
    if not _PIL_OK:
        return False
    try:
        img = Image.open(io.BytesIO(dados)).convert("RGBA")
        w, h = img.size
        px   = img.load()
        n    = sum(1 for x in range(max(0, w-80), w)
                   for y in range(max(0, h-40), h)
                   if px[x, y][3] < 200 and sum(px[x, y][:3]) > 400)
        return n > 50
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

def _tem_midia(media) -> bool:
    return media is not None and not isinstance(media, MessageMediaWebPage)

def _eh_cupom(t: str) -> bool:
    return bool(_RE_CUPOM_KW.search(t))


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
#
# Problema: o Telegram dispara MessageEdited após send_file (atualiza preview).
# Solução:
#   NewMessage com id já processado → ignora (anti-loop)
#   MessageEdited com id no mapeamento → EDITA (comportamento correto)
#   MessageEdited com id FORA do mapeamento → ignora (preview fake)
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


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 19 ▸ ENVIO — 1 MENSAGEM SEMPRE
# ══════════════════════════════════════════════════════════════════════════════

async def _enviar(msg: str, img_obj) -> object:
    """Imagem + texto = 1 mensagem. Texto longo → sem imagem + preview."""
    if img_obj and len(msg) <= 1024:
        try:
            return await client.send_file(
                GRUPO_DESTINO, img_obj, caption=msg, parse_mode="md")
        except Exception as e:
            log_tg.warning(f"⚠️ send_file: {e}")
    return await client.send_message(
        GRUPO_DESTINO, msg, parse_mode="md", link_preview=True)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 20 ▸ PIPELINE PRINCIPAL — NOVO/EDITADO UNIFICADO
# ══════════════════════════════════════════════════════════════════════════════

async def processar(event, is_edit: bool = False):
    """
    Pipeline completo. Estado limpo a cada chamada.
    Suporta mensagens novas E editadas.
    """
    msg_id = event.message.id
    texto  = event.message.text or ""
    chat   = await event.get_chat()
    uname  = (chat.username or str(event.chat_id)).lower()

    log_tg.info(f"{'✏️ EDIT' if is_edit else '📩 NEW'} | @{uname} | id={msg_id} | {len(texto)}c")

    # E1: Vazio
    if not texto.strip():
        return

    # E2: Anti-loop
    if not is_edit:
        if await _foi_processado(msg_id):
            log_sys.debug(f"⏩ Anti-loop: {msg_id}")
            return
    else:
        loop  = asyncio.get_event_loop()
        mapa_c = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        if str(msg_id) not in mapa_c:
            log_sys.debug(f"⏩ Edit ignorada (preview?): {msg_id}")
            return

    # E3: Filtro de texto
    if texto_bloqueado(texto):
        return

    # E4: Limpeza de ruído textual
    texto_limpo = limpar_ruido_textual(texto)

    # E5: Extração inteligente de links (converter + preservar)
    links_conv, links_pres = extrair_links(texto_limpo)
    log_lnk.info(f"🔗 {len(links_conv)} converter | {len(links_pres)} preservar")

    if not links_conv and not links_pres and "fadadoscupons" not in uname:
        log_sys.debug("⏩ Sem links")
        return

    # E6: Conversão paralela (whitelist + motores isolados)
    mapa_links, plat_p = await converter_links(links_conv)

    if links_conv and not mapa_links and not links_pres:
        log_sys.warning(f"🚫 Zero links válidos | @{uname}")
        return

    # E7: Deduplicação semântica
    prod = _extrair_prod_id(mapa_links)
    cup  = _extrair_cupom(texto_limpo)

    if not is_edit:
        if not deve_enviar(plat_p, prod, cup, texto_limpo):
            return

    # E8: Renderização isolada por plataforma
    msg_final = renderizar(texto_limpo, mapa_links, links_pres, plat_p)
    log_fmt.debug(f"📝 [{plat_p.upper()}]\n{msg_final[:400]}")

    # E9: Imagem
    media_orig = event.message.media
    tem_img    = _tem_midia(media_orig)
    img_obj    = None

    if _eh_cupom(texto_limpo):
        if tem_img:
            img_obj, _ = await preparar_imagem(media_orig, True)
        else:
            fb = {"amazon": _IMG_AMZ, "shopee": _IMG_SHP, "magalu": _IMG_MGL}
            f  = fb.get(plat_p, _IMG_AMZ)
            img_obj = f if os.path.exists(f) else None
    else:
        if tem_img:
            img_obj, _ = await preparar_imagem(media_orig, True)
        elif mapa_links:
            img_url = await buscar_imagem(list(mapa_links.values())[0])
            if img_url:
                img_obj, _ = await preparar_imagem(img_url, False)

    # E10: Rate-limit
    await _rate_limit()

    # E11: Envio / Edição com retry
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
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        log_tg.error(f"❌ Edit t={t}: {e}")
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
# MÓDULO 21 ▸ HEALTH CHECK
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
        log_sys.error("❌ Sessão inválida! Verifique TELEGRAM_SESSION.")
        return False

    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 Grupos: {GRUPOS_ORIGEM}")
    log_sys.info(f"📣 Destino: {GRUPO_DESTINO}")
    log_sys.info(f"🟠 Amazon: {_AMZ_TAG}")
    log_sys.info(f"🟣 Shopee: {_SHP_APP_ID}")
    log_sys.info(f"🔵 Magalu: promo={_MGL_PROMOTER} slug={_MGL_SLUG}")
    log_sys.info(f"🖼  Pillow: {'OK' if _PIL_OK else 'pip install Pillow'}")
    log_sys.info("🚀 FOGUETÃO v69.0 — IA DE OFERTAS — ONLINE!")

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

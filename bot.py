"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   FOGUETÃO v73.0 — VERSÃO ELITE CONSOLIDADA                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  CIRURGIAS v73 (sem alterar outros módulos):                                ║
║                                                                             ║
║  MÓDULO 5  — Barreira de concorrência:                                     ║
║    • t.me/joinchat, t.me/canal, chat.whatsapp.com → deletados em silêncio  ║
║    • wa.me preservado (suporte direto)                                     ║
║    • _eh_vitrine_magalu agora detecta /l/ e /selecao/                      ║
║                                                                             ║
║  MÓDULO 9  — DNA Magalu + Cuttly resiliente:                               ║
║    • Path NUNCA zerado (/p/ID/, /l/lista/, /selecao/ preservados)          ║
║    • Parâmetros oficiais: partner_id, promoter_id, af_force_deeplink,      ║
║      deep_link_value (underline — formato API Magazine Você)               ║
║    • Cuttly: 15s timeout, 3 tentativas com backoff, trata status 2         ║
║                                                                             ║
║  MÓDULO 12 — Limpeza agressiva em 4 camadas:                               ║
║    • Camada 2 nova: remove linhas com t.me/chat.whatsapp.com               ║
║    • CTAs de outros bots: 'Entre no grupo', 'Grupo VIP', etc.              ║
║                                                                             ║
║  MÓDULO 16 — Buscador obstinado de imagem:                                 ║
║    • 3 tentativas com 1s de intervalo (antes era 1.5*t)                    ║
║                                                                             ║
║  MÓDULO 19 — Prioridade absoluta de envio com imagem:                      ║
║    • send_file sempre preferido                                             ║
║    • Texto longo: imagem + texto em sequência (não descarta imagem)        ║
║    • Só cai para texto puro se send_file falhar irrecuperavelmente         ║
║                                                                             ║
║  NÃO ALTERADO: módulos 6, 7, 8, 10, 11, 13, 14, 15, 17, 18, 20, 21, 22  ║
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
#
# REGRA ESPECIAL — BYPASS AUTOMÁTICO PARA MULTI-OFERTAS:
# Se o texto contiver "ofertas" + plataforma (Amazon/Shopee/Magalu),
# NÃO bloqueia mesmo que tenha palavras da lista — são posts de lista completa.
# Exemplo: "Ofertas Shopee", "Ofertas na Amazon", "Ofertas Magalu" → passa.
#
# Bloqueia normalmente apenas quando:
#   - É oferta de 1 produto (sem indicativo de lista)
#   - E contém palavras técnicas bloqueadas (hardware, periférico, etc.)
# ══════════════════════════════════════════════════════════════════════════════

_FILTRO_TEXTO = [
    "Monitor Samsung", "Fonte Mancer", "Placa de video", "Monitor LG",
    "PC home Essential", "Suporte articulado", "Gabinetes em oferta",
    "VHAGAR", "Superframe", "AM5", "AM4", "GTX", "DDR5", "DDR4",
    "Dram", "Monitor Safe", "Monitor Redragon", "CL18", "CL16", "CL32",
    "MT/s", "MHz", "RX 580", "Ryzen", "Placa Mãe", "Gabinete Gamer",
    "Water Cooler", "Air Cooler",
]

# Indicativos de lista multi-produto — bypass do filtro
_RE_MULTI_OFERTA = re.compile(
    r'\b(?:ofertas?|promoções?)\s+(?:na\s+|no\s+|da\s+)?'
    r'(?:shopee|amazon|magalu|magazine\s*luiza)\b',
    re.I
)

def texto_bloqueado(texto: str) -> bool:
    """
    Retorna True se o texto deve ser bloqueado.
    BYPASS: posts de lista multi-produto ("Ofertas Shopee", "Ofertas Amazon")
    nunca são bloqueados, mesmo com palavras da lista de filtro.
    """
    # Bypass: post de lista completa → nunca bloqueia
    if _RE_MULTI_OFERTA.search(texto):
        log_fil.debug(f"✅ Bypass multi-oferta: {texto[:60]}")
        return False

    tl = texto.lower()
    for p in _FILTRO_TEXTO:
        if p.lower() in tl:
            log_fil.debug(f"🚫 Filtro: '{p}'")
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

# Links que NUNCA devem ser processados nem aparecer no canal destino:
#   t.me/joinchat, t.me/outros_grupos → deletados silenciosamente
#   chat.whatsapp.com → deletado (convite para grupo de concorrente)
# Links que são PRESERVADOS sem alteração:
#   wa.me → suporte direto ao usuário (sempre mantido)
_PRESERVE  = frozenset(["wa.me", "api.whatsapp.com"])  # só esses passam
_DELETAR   = frozenset([                                # esses somem em silêncio
    "t.me", "telegram.me", "telegram.org",
    "chat.whatsapp.com",                               # convite grupo WhatsApp
])

def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""

def _eh_link_grupo_externo(url: str) -> bool:
    """
    Retorna True se o link for convite para grupo externo (concorrente).
    Detecta: t.me/joinchat, t.me/+hash, t.me/canal_qualquer,
             chat.whatsapp.com/invite/...
    """
    nl = _netloc(url)
    for d in _DELETAR:
        if nl == d or nl.endswith("." + d):
            return True
    return False

def classificar(url: str) -> Optional[str]:
    """
    Retorna: 'amazon' | 'shopee' | 'magalu' | 'preservar' | 'expandir' | None

    REGRA DE ISOLAMENTO ESTANQUE:
    - Links de grupos externos (t.me, chat.whatsapp.com) → None (deletados)
    - wa.me → 'preservar' (link de suporte, não substituído)
    - Magalu verificado ANTES de Amazon/Shopee → zero cross-linking
    - Correspondência por SUFIXO de domínio (não substring)
    """
    nl = _netloc(url)
    if not nl:
        return None

    # Links de grupos externos → deletar silenciosamente
    if _eh_link_grupo_externo(url):
        log_lnk.debug(f"🚫 Grupo externo bloqueado: {nl}")
        return None

    # wa.me → preservar (suporte direto)
    for d in _PRESERVE:
        if nl == d or nl.endswith("." + d):
            return "preservar"

    # MAGALU primeiro — evita cross-linking com Amazon
    for dom in ("magazineluiza.com.br", "sacola.magazineluiza.com.br",
                "magazinevoce.com.br", "maga.lu"):
        if nl == dom or nl.endswith("." + dom):
            log_lnk.debug(f"🔵 MAGALU: {nl}")
            return "magalu"

    # AMAZON
    for dom in ("amazon.com.br", "amzn.to", "amzn.com", "a.co"):
        if nl == dom or nl.endswith("." + dom):
            log_lnk.debug(f"🟠 AMAZON: {nl}")
            return "amazon"

    # SHOPEE
    for dom in ("shopee.com.br", "s.shopee.com.br", "shopee.com", "shope.ee"):
        if nl == dom or nl.endswith("." + dom):
            log_lnk.debug(f"🟣 SHOPEE: {nl}")
            return "shopee"

    # Encurtadores genéricos → expande e reclassifica
    for enc in _ENCURTADORES:
        if nl == enc or nl.endswith("." + enc):
            return "expandir"

    log_lnk.debug(f"🗑 Fora whitelist: {nl}")
    return None

def _eh_vitrine_magalu(url: str) -> bool:
    """Retorna True para URLs de vitrine/categoria/lista Magalu."""
    return bool(re.search(
        r'(magazineluiza\.com\.br|magazinevoce\.com\.br)'
        r'.*(vitrine|categoria|lista|/l/|/selecao/|promo|stores|loja)',
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
#
# PRINCÍPIO: substituição pura de IDs — path NUNCA alterado.
# Tipos de path preservados:
#   /produto-xyz/p/jg52b75k2d/pf/macc/  → produto
#   /l/lista-nome/                        → lista curada
#   /selecao/nome/                        → seleção
#   /magazine{slug}/lojista/{loja}/       → lojista (vitrine magazinevoce)
#   /                                     → homepage (sem produto = erro!)
#
# PROTEÇÃO ANTI-SACOLA/HOME:
# Se após desencurtar o path for "/" ou vazio → link inválido → None
# (evita enviar sacola.magazineluiza.com.br/?f=1 ou homepage sem produto)
# ══════════════════════════════════════════════════════════════════════════════

def _eh_link_valido_magalu(url_exp: str) -> bool:
    """
    Retorna False se o link expandido for inválido:
    - homepage sem path de produto (path == "/" ou vazio)
    - sacola sem contexto de produto (?f=1 sem path real)
    - path com menos de 2 segmentos úteis
    """
    p    = urlparse(url_exp)
    host = p.netloc.lower()
    path = p.path.rstrip("/")

    # sacola.magazineluiza.com.br com path vazio ou só /?f=1 → inválido
    if "sacola" in host and (not path or path in ("/", "")):
        log_mgl.warning(f"  ⚠️ Sacola sem produto: {url_exp[:60]}")
        return False

    # Homepage sem path de produto → inválido
    if path in ("", "/", "") or len(path.split("/")) < 2:
        log_mgl.warning(f"  ⚠️ Homepage sem produto: {url_exp[:60]}")
        return False

    return True


def _substituir_ids_magalu(url_exp: str) -> str:
    """
    Substitui SOMENTE os IDs de afiliado. Preserva 100% do path original.

    Casos tratados:
      magazineluiza.com.br/produto/p/ID/pf/cat/  → preserva tudo até ?
      magazinevoce.com.br/magazine{slug}/lojista/ → preserva path + slug do lojista
      m.magazineluiza.com.br/...                  → preserva (mobile também)
    """
    p    = urlparse(url_exp)
    path = p.path  # ← NUNCA zerado

    # Vitrine magazinevoce: substitui slug do afiliado antigo pelo meu
    # APENAS o segmento /magazine{slug}/, não toca em /lojista/ nem demais
    if "magazinevoce.com.br" in p.netloc.lower():
        path = re.sub(
            r'^(/magazine)[^/]+',   # /magazineantigo → /magazinemeu
            rf'\1{_MGL_SLUG}',
            path
        )
        log_mgl.debug(f"  Slug magazinevoce → {path}")

    # Lê params originais (preserva tudo que não for de afiliado)
    params_orig = parse_qs(p.query, keep_blank_values=True)
    params_new  = {k: v[0] for k, v in params_orig.items()}

    # Remove tag Amazon e params de afiliados externos
    for k_remover in ["tag", "partnerid", "promoterid", "deeplinkvalue",
                       "afforcedeeplink", "isretargeting", "partner_id_old"]:
        params_new.pop(k_remover, None)

    # Injeta/substitui SOMENTE os IDs de comissão
    params_new["partner_id"]         = _MGL_PARTNER
    params_new["promoter_id"]        = _MGL_PROMOTER
    params_new["utm_source"]         = "divulgador"
    params_new["utm_medium"]         = "magalu"
    params_new["utm_campaign"]       = _MGL_PROMOTER
    params_new["pid"]                = _MGL_PID
    params_new["c"]                  = _MGL_PROMOTER
    params_new["af_force_deeplink"]  = "true"

    # deep_link_value aponta para URL base do produto (path preservado)
    base = urlunparse(p._replace(path=path, query="", fragment=""))
    params_new["deep_link_value"] = (
        f"{base}?utm_source=divulgador&utm_medium=magalu"
        f"&partner_id={_MGL_PARTNER}&promoter_id={_MGL_PROMOTER}"
        f"&utm_campaign={_MGL_PROMOTER}"
    )

    url_final = urlunparse(p._replace(
        path=path, query=urlencode(params_new), fragment=""))
    log_mgl.debug(f"  🏷 {url_final[:100]}")
    return url_final


async def _cuttly(url: str, sessao: aiohttp.ClientSession) -> str:
    """Encurta via Cuttly. 3 tentativas, 15s timeout, status 2 aceito."""
    url_encoded = quote(url, safe="")
    api = f"https://cutt.ly/api/api.php?key={_CUTTLY_KEY}&short={url_encoded}"

    for t in range(1, 4):
        try:
            async with _SEM_HTTP:
                async with sessao.get(
                    api, timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    if r.status != 200:
                        log_mgl.warning(f"  ⚠️ HTTP {r.status} t={t}/3")
                        await asyncio.sleep(2 ** t)
                        continue
                    try:
                        data = await r.json(content_type=None)
                    except Exception:
                        raw = await r.text()
                        log_mgl.warning(f"  ⚠️ JSON inválido t={t}: {raw[:60]}")
                        await asyncio.sleep(2 ** t)
                        continue

                    status = data.get("url", {}).get("status")
                    if status == 7:
                        s = data["url"]["shortLink"]
                        log_mgl.info(f"  ✂️ t={t}: {s}")
                        return s
                    if status == 2:
                        s = data["url"].get("shortLink", url)
                        log_mgl.info(f"  ✂️ existente t={t}: {s}")
                        return s
                    log_mgl.warning(f"  ⚠️ status={status} t={t}/3")
                    await asyncio.sleep(2 ** t)

        except asyncio.TimeoutError:
            log_mgl.warning(f"  ⏱ timeout t={t}/3")
            await asyncio.sleep(2 ** t)
        except Exception as e:
            log_mgl.error(f"  ❌ t={t}: {e}")
            await asyncio.sleep(2 ** t)

    log_mgl.error(f"  ❌ Cuttly 3x falhou | {url[:80]}")
    return url


async def motor_magalu(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """
    Motor exclusivo Magalu.
    1. Desencurta até URL real
    2. Valida que tem path de produto (não sacola/home)
    3. Substitui apenas IDs de afiliado
    4. Encurta via Cuttly
    """
    log_mgl.debug(f"🔗 {url[:80]}")

    # Etapa 1: desencurta
    nl = _netloc(url)
    if "maga.lu" in nl or nl in _ENCURTADORES or classificar(url) == "expandir":
        async with _SEM_HTTP:
            exp = await desencurtar_ultra(url, sessao)
        log_mgl.debug(f"  📦 {exp[:80]}")
    else:
        exp = url

    # Etapa 2: confirma Magalu
    if classificar(exp) != "magalu":
        log_mgl.warning(f"  ⚠️ Não-Magalu: {exp[:60]}")
        return None

    # Etapa 3: proteção anti-sacola/homepage sem produto
    if not _eh_link_valido_magalu(exp):
        log_mgl.warning(f"  ⚠️ Link inválido (sacola/home sem produto): {exp[:80]}")
        return None  # descarta — não envia link quebrado

    # Etapa 4: substitui IDs
    url_com_ids = _substituir_ids_magalu(exp)

    # Etapa 5: encurta
    final = await _cuttly(url_com_ids, sessao)
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
    Limpeza em 4 camadas:

    Camada 0 — Unicode invisíveis removidos.
    Camada 1 — Expressões de afiliado / CTA de outros bots (re.sub inline).
    Camada 2 — Linhas que contêm links para grupos externos (t.me, chat.whatsapp.com).
    Camada 3 — Linhas que viraram ruído puro após a limpeza.
    Camada 4 — Linhas vazias consecutivas normalizadas (máx 1).
    """
    # ── Camada 0: Unicode invisíveis ─────────────────────────────────────
    texto = re.sub(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]', ' ', texto)

    # ── Camada 1: CTA / expressões de outros bots (inline por linha) ─────
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
        r'^\s*-?\s*Entre\s+no\s+grupo\s*:?\s*',    # "Entre no grupo"
        r'^\s*-?\s*Acesse\s+o\s+grupo\s*:?\s*',    # "Acesse o grupo"
        r'^\s*-?\s*Grupo\s+VIP\s*:?\s*',            # "Grupo VIP"
        r'^\s*-?\s*Anúncio\s*:?\s*',
        r'^\s*-?\s*anuncio\s*:?\s*',
        r'^\s*-?\s*Publicidade\s*:?\s*',
    ]
    _marcadores = [
        r':::\s*(?:ML|MG|AMZ|loja)?\s*',
        r'---+\s*',
        r'===+\s*',
        r'^\s*[-–—]+\s*(?:ML|MG|AMZ)\s*[-–—]*\s*',
        r'^\s*(?:ML|MG|AMZ)\s*:\s*',
        r'^\s*::\s*(?:ML|MG|AMZ)\s*::\s*',
    ]

    linhas = texto.split('\n')
    limpas = []
    for linha in linhas:
        l = linha
        for expr in _exprs_inicio:
            l = re.sub(expr, '', l, flags=re.I | re.M)
        for marc in _marcadores:
            l = re.sub(marc, '', l, flags=re.I | re.M)
        limpas.append(l)

    # ── Camada 2: Remove linhas com links para grupos externos ────────────
    # (t.me/joinchat, t.me/canal, chat.whatsapp.com)
    _RE_GRUPO_EXTERNO = re.compile(
        r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)'
        r'[^\s]*',
        re.I
    )
    sem_grupos = []
    for linha in limpas:
        if _RE_GRUPO_EXTERNO.search(linha):
            # Se a linha INTEIRA é o link do grupo → remove a linha
            resto = _RE_GRUPO_EXTERNO.sub('', linha).strip()
            if not resto:
                log_fmt.debug(f"🚫 Linha grupo externo removida: {linha[:60]}")
                continue
            # Linha com texto + link do grupo → remove só o link
            linha = _RE_GRUPO_EXTERNO.sub('', linha).strip()
        sem_grupos.append(linha)

    # ── Camada 3: Remove linhas que viraram ruído puro ────────────────────
    _RE_LINHA_LIXO = re.compile(
        r'^\s*(?:'
        r'-\s*Anúncio|Anúncio|anuncio|-anuncio|'
        r'- Anúncio|-Publicidade|Publicidade|'
        r':::\s*|---+|===+|'
        r'[-–]\s*(?:ML|MG|AMZ)|(?:ML|MG|AMZ)\s*:'
        r')\s*$',
        re.I
    )
    filtradas = [l for l in sem_grupos if not _RE_LINHA_LIXO.match(l.strip())]

    # ── Camada 4: Linhas vazias consecutivas (máx 1) ──────────────────────
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
# MÓDULO 13 ▸ EMOJIS DINÂMICOS (VARIEDADE PROFISSIONAL)
# ══════════════════════════════════════════════════════════════════════════════

# Sua tabela com listas de opções (O bot vai escolher uma de cada lista)
_EMJ = {
    "titulo_oferta":    ["🔥", "💥", "⚡️", "🚀"],
    "titulo_cupom":     ["🚨", "🔔", "📢"],
    "titulo_evento":    ["⚠️", "🎯", "🎰"],
    "preco":            ["💵", "💰", "🤑", "💸"],
    "cupom_cod":        ["🎟", "🎫", "🏷"],
    "resgate":          ["✅", "🎯", "🔗"],
    "carrinho":         ["🛒", "🛍"],
    "frete":            ["🚚", "📦", "✈️"],
}

def _classificar_mensagem(texto: str) -> str:
    """Decide a intenção da mensagem."""
    if _KW_EVENTO.search(texto): return "evento"
    if _KW_CUPOM.search(texto): return "cupom_puro"
    return "oferta_produto"

def _emoji_topo(tipo: str) -> str:
    """Escolhe um emoji aleatório para o topo conforme o tipo."""
    if tipo == "evento": return random.choice(_EMJ["titulo_evento"])
    if tipo == "cupom_puro": return random.choice(_EMJ["titulo_cupom"])
    return random.choice(_EMJ["titulo_oferta"])

def _emoji_de_linha(linha: str, plat: str, eh_titulo: bool) -> Optional[str]:
    """Retorna um emoji aleatório dentro da categoria da linha."""
    ls = linha.lower()
    
    if eh_titulo:
        tipo = _classificar_mensagem(linha)
        return _emoji_topo(tipo)

    # O bot escolhe aleatoriamente um dos emojis da categoria
    if any(x in ls for x in ["frete grátis", "entrega grátis", "sem frete"]):
        return random.choice(_EMJ["frete"])
    
    if any(x in ls for x in ["cupom", "código", "off"]):
        return random.choice(_EMJ["cupom_cod"])
    
    if "r$" in ls:
        return random.choice(_EMJ["preco"])
    
    if any(x in ls for x in ["resgate", "clique", "acesse"]):
        return random.choice(_EMJ["resgate"])
    
    if "carrinho" in ls:
        return random.choice(_EMJ["carrinho"])
        
    return None

def _tem_emoji(s: str) -> bool:
    return bool(re.search(r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF]", s))

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ RENDERIZADOR LINEAR (COM CLIQUE E COPIE)
# ══════════════════════════════════════════════════════════════════════════════

# ─── FERRAMENTAS DE BUSCA DO MÓDULO 14 (OBRIGATÓRIO) ───────────────────────
_RE_LIXO_PREFIXO = re.compile(r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:|[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?)\s*', re.I)
_RE_ANUNCIO_LINHA = re.compile(r'^\s*[-#]?\s*(?:anúncio|anuncio|publicidade|patrocinado|sponsored)\s*$', re.I)
_RE_URL = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c]+')

def _aplicar_crases_no_codigo(linha: str) -> str:
    """Procura um código de cupom na linha e coloca crases se não houver."""
    # Regex que procura palavras em maiúsculo com números (Ex: HOJEPODE, GANHE10)
    # Ignora palavras que já tenham crases ou que sejam links
    if "http" in linha: return linha
    
    def _substituir(match):
        codigo = match.group(0)
        # Se o código for muito curto (tipo 'R$') ou já tiver crase, não mexe
        if len(codigo) < 4 or "`" in linha:
            return codigo
        return f"`{codigo}`"

    # Procura padrões de 4 a 20 caracteres maiúsculos/números
    return re.sub(r'\b[A-Z0-9_-]{4,20}\b', _substituir, linha)

def renderizar(texto: str, mapa_links: dict, links_preservar: list, plat: str) -> str:
    """Processa linha por linha e garante o clique-e-copie no cupom."""
    mapa_total = dict(mapa_links)
    for url in links_preservar: mapa_total[url] = url

    linhas = texto.split('\n')
    saida = []
    eh_primeira_texto = True 

    for linha in linhas:
        ls = linha.strip()
        if not ls:
            saida.append("")
            continue

        if _RE_ANUNCIO_LINHA.match(ls):
            saida.append(ls)
            continue

        ls = _RE_LIXO_PREFIXO.sub("", ls).strip()
        if not ls: continue

        # --- PROCESSAMENTO DE LINKS ---
        urls_na_linha = _RE_URL.findall(ls)
        sem_urls = _RE_URL.sub("", ls).strip()

        if urls_na_linha and not sem_urls:
            for u in urls_na_linha:
                uc = u.rstrip('.,;)>')
                if uc in mapa_total: saida.append(mapa_total[uc])
            continue

        def _sub_link(m):
            uc = m.group(0).rstrip('.,;)>')
            return mapa_total.get(uc, "")
        
        nova_linha = _RE_URL.sub(_sub_link, ls).strip()
        if not nova_linha: continue

        # --- REGRA DA CRASE (CLIQUE E COPIE) ---
        # Se a linha fala de cupom ou código, a gente tenta aplicar a crase
        if any(x in nova_linha.lower() for x in ["cupom", "código", "use", "cod"]):
            nova_linha = _aplicar_crases_no_codigo(nova_linha)

        # --- EMOJIS (Módulo 13) ---
        if not _tem_emoji(nova_linha):
            emoji = _emoji_de_linha(nova_linha, plat, eh_titulo=eh_primeira_texto)
            if emoji: nova_linha = f"{emoji} {nova_linha}"
        
        # Regra do Carrinho
        if plat in ["shopee", "magalu"] and "carrinho" in nova_linha.lower():
            if "🛒" not in nova_linha: nova_linha = "🛒 " + nova_linha

        if eh_primeira_texto: eh_primeira_texto = False
        saida.append(nova_linha)

    return "\n".join(saida).strip()


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 15 ▸ DEDUPLICAÇÃO SEMÂNTICA — OLHA PARA A ALMA, IGNORA O LINK
#
# PRINCÍPIO FUNDAMENTAL: o link nunca é critério. Apenas texto e cupom importam.
#
# CHAVE DE IDENTIDADE DE UMA OFERTA:
#   plataforma + conjunto de cupons + alma do texto normalizado
#
# CAMADAS DE BLOQUEIO:
#   C1 — Hash exato da alma → bloqueia (mesmo texto, mesmos cupons)
#   C2 — Cupom idêntico + alma similar >= 0.72 nos últimos 10 min → bloqueia
#         (mesmo cupom chegando de grupos diferentes com texto ligeiramente diferente)
#   C3 — Campanha igual + alma similar >= 0.80 nos últimos 10 min → bloqueia
#   C4 — Alma muito similar >= 0.90 nos últimos 10 min → bloqueia
#         (mesmo post sem cupom, só texto parecido)
#   C5 — Evento (Quiz/Roleta/Missão): cupom igual → bloqueia por 24h
#
# EXCEÇÕES (SEMPRE PASSA):
#   • Texto contém "Voltando", "Normalizou", "Estoque Renovado" → reenvio permitido
#   • Cupom diferente → sempre passa (nova oferta real)
#   • Plataformas diferentes nunca se bloqueiam entre si
# ══════════════════════════════════════════════════════════════════════════════

_TTL_CACHE     = 120 * 60        # memória geral: 120 min
_TTL_EVENTO    = 24 * 60 * 60   # eventos: 24h
_JANELA_CURTA  = 10 * 60        # janela principal: 10 min (grupos simultâneos)
_JANELA_EVENTO = 24 * 60 * 60   # janela evento: 24h

# Limiares de similaridade da alma do texto
_SIM_CUPOM     = 0.72   # C2: cupom igual → baixo limiar (difícil escapar)
_SIM_CAMPANHA  = 0.80   # C3: mesma campanha
_SIM_TEXTO     = 0.90   # C4: texto quase idêntico (sem cupom)
_SIM_EVENTO    = 0.60   # C5: evento → qualquer parecença bloqueia por 24h

_RUIDO_NORM = {
    "promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora",
    "imperdivel","imperdível","exclusivo","limitado","corra","ative",
    "use","saiu","vazou","resgate","acesse","confira","link","clique",
    "app","relampago","relâmpago","click","veja","agora","novo","nova",
}

# Transformamos as listas em Radares (Regex) para o comando .search() funcionar
_KW_EVENTO = re.compile(r'(?i)\b(quiz|roleta|missão|missao|arena|girar|gire|roda|jogar|jogue|desafio)\b')
_KW_STATUS = re.compile(r'(?i)\b(voltando|voltou|normalizou|renovado|estoque\s+renovado|regularizou)\b')
_KW_CAMPANHA = {
    "amazon_app":   ["amazon app","app amazon","aplicativo amazon"],
    "mastercard":   ["mastercard","master card"],
    "frete_gratis": ["frete gratis","frete grátis","entrega gratis"],
    "prime":        ["prime","amazon prime"],
    "shopee_frete": ["frete shopee","shopee frete"],
    "magalu_app":   ["magalu app","app magalu"],
}

_RE_EMOJI_STRIP = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF]+",
    flags=re.UNICODE)


def _rm_ac(t: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', t)
                   if unicodedata.category(c) != 'Mn')


def _extrair_todos_cupons(texto: str) -> frozenset:
    """Extrai TODOS os códigos de cupom do texto (não só o primeiro)."""
    return frozenset(re.findall(r'\b([A-Z][A-Z0-9_-]{3,19})\b', texto))


def _normalizar_alma(texto: str) -> str:
    """
    Extrai a 'alma' do texto removendo todo ruído:
    links, emojis, símbolos, palavras de reclame, pontuação.
    Normaliza valores: R$10 → PRECO, 15% → PCT
    """
    t = _rm_ac(texto.lower())
    t = re.sub(r'https?://\S+', ' ', t)           # remove URLs
    t = _RE_EMOJI_STRIP.sub(' ', t)               # remove emojis
    t = re.sub(r'r\$\s*[\d.,]+', ' PRECO ', t)   # normaliza preços
    t = re.sub(r'\b\d+\s*%', ' PCT ', t)          # normaliza %
    t = re.sub(r'\b\d+\b', ' NUM ', t)            # normaliza números
    t = re.sub(r'[^\w\s]', ' ', t)               # remove pontuação
    t = re.sub(r'\s+', ' ', t).strip()
    palavras = [w for w in t.split() if w not in _RUIDO_NORM and len(w) > 1]
    return ' '.join(sorted(palavras))


def _detectar_campanha(texto: str) -> str:
    """Detecta a alma da oferta para o bot não repetir posts."""
    tl = texto.lower()
    tokens = []
    
    # Procura por palavras de evento usando o Radar
    match_evento = _KW_EVENTO.search(texto)
    if match_evento:
        tokens.append(f"evento_{match_evento.group(0).lower().strip()}")
    
    # Outras detecções simples
    if "amazon app" in tl: tokens.append("amazon_app")
    if "mastercard" in tl: tokens.append("mastercard")
    if "prime" in tl:      tokens.append("prime")

    return "|".join(sorted(tokens)) if tokens else "geral"


def _eh_evento(texto: str) -> bool:
    """Retorna True se o texto descreve um evento recorrente (Quiz, Roleta...)."""
    # Mesma lógica: usamos o radar .search()
    return bool(_KW_EVENTO.search(texto))


def _eh_mudanca_status(texto: str) -> bool:
    """Retorna True se o texto indica mudança de status real (Voltando, Normalizou...)."""
    # Usamos o radar .search() em vez de tentar percorrer como se fosse uma lista
    return bool(_KW_STATUS.search(texto))


def _extrair_cupom(texto: str) -> str:
    """Compatibilidade: retorna o primeiro cupom (usado pelo pipeline)."""
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
    Decide se a oferta deve ser enviada.
    Olha para a ALMA (texto normalizado + cupons) — NUNCA para o link.
    """
    # ── Exceção imediata: mudança de status sempre passa ─────────────────
    if _eh_mudanca_status(texto):
        log_dedup.info(f"✅ Mudança de status detectada — passa direto")
        # Registra mas não bloqueia
        _registrar_cache(plat, prod, cupom, texto)
        return True

    cache    = ler_cache()
    agora    = time.time()
    ttl_max  = max(_TTL_CACHE, _TTL_EVENTO)
    cache    = {k: v for k, v in cache.items()
                if agora - v.get("ts", 0) < ttl_max}

    alma     = _normalizar_alma(texto)
    cupons   = _extrair_todos_cupons(texto)     # TODOS os cupons do texto
    campanha = _detectar_campanha(texto)
    eh_evt   = _eh_evento(texto)

    # Hash da identidade da oferta
    cupons_str = "|".join(sorted(cupons))
    h = hashlib.sha256(f"{plat}|{cupons_str}|{alma}".encode()).hexdigest()

    # C1: hash exato da alma → bloqueia
    if h in cache:
        log_dedup.info(f"🔁 [C1] Idêntico | plat={plat} cupons={cupons_str}")
        return False

    janela = _JANELA_EVENTO if eh_evt else _JANELA_CURTA

    for entrada in cache.values():
        if agora - entrada.get("ts", 0) >= janela:
            continue
        if entrada.get("plat") != plat:
            continue  # plataformas nunca bloqueiam entre si

        alma_cache   = entrada.get("alma", "")
        cupons_cache = frozenset(entrada.get("cupons", []))
        camp_cache   = entrada.get("camp", "")
        sim          = SequenceMatcher(None, alma, alma_cache).ratio()
        cupom_igual  = bool(cupons & cupons_cache) if cupons else False

        # C2: mesmo cupom + alma parecida (grupos simultâneos)
        if cupom_igual and sim >= _SIM_CUPOM:
            log_dedup.info(
                f"🔁 [C2] Cupom igual + similar | "
                f"cupons={cupons & cupons_cache} sim={sim:.2f}")
            return False

        # C3: mesma campanha + alma similar
        if campanha == camp_cache and campanha != "geral" and sim >= _SIM_CAMPANHA:
            log_dedup.info(
                f"🔁 [C3] Campanha igual + similar | "
                f"camp={campanha} sim={sim:.2f}")
            return False

        # C4: texto muito similar mesmo sem cupom
        if sim >= _SIM_TEXTO:
            log_dedup.info(f"🔁 [C4] Alma muito similar | sim={sim:.2f}")
            return False

        # C5: evento recorrente — bloqueio por 24h mesmo com sim baixo
        if eh_evt and cupom_igual and sim >= _SIM_EVENTO:
            log_dedup.info(
                f"🔁 [C5-EVENTO] | cupons={cupons & cupons_cache} sim={sim:.2f}")
            return False

    # Nova oferta — registra
    _registrar_cache(plat, prod, cupom, texto)
    log_dedup.debug(f"✅ Nova | plat={plat} cupons={cupons_str} camp={campanha}")
    return True


def _registrar_cache(plat: str, prod: str, cupom: str, texto: str):
    """Registra a oferta no cache. Chamado tanto por deve_enviar quanto pela exceção de status."""
    cache    = ler_cache()
    agora    = time.time()
    alma     = _normalizar_alma(texto)
    cupons   = _extrair_todos_cupons(texto)
    campanha = _detectar_campanha(texto)
    cupons_str = "|".join(sorted(cupons))
    h = hashlib.sha256(f"{plat}|{cupons_str}|{alma}".encode()).hexdigest()
    cache[h] = {
        "plat":   plat,
        "prod":   str(prod),
        "cupom":  cupom.upper(),
        "cupons": list(cupons),
        "camp":   campanha,
        "alma":   alma,
        "ts":     agora,
    }
    salvar_cache(cache)



async def buscar_imagem(url: str) -> Optional[str]:
    """
    Buscador obstinado de imagem com 4 estratégias em ordem:
    1. og:image ou twitter:image na página do produto
    2. Primeira <img> com src https e dimensão >= 200px
    3. JSON-LD schema.org (image) no HTML
    4. Tentativa direto na URL (se já for imagem)
    """
    headers = {"User-Agent": random.choice(USER_AGENTS),
               "Accept-Language": "pt-BR,pt;q=0.9"}

    for t in range(1, 4):
        log_img.debug(f"🖼 t={t}/3 | {url[:60]}")
        try:
            async with aiohttp.ClientSession(headers=headers) as s:
                async with s.get(url, allow_redirects=True,
                                  timeout=aiohttp.ClientTimeout(total=12)) as r:

                    # Estratégia 4: URL já é imagem direta
                    ct = r.headers.get("content-type", "")
                    if "image" in ct:
                        log_img.info(f"✅ URL é imagem direta t={t}")
                        return str(r.url)

                    html = await r.text(errors="ignore")
                    soup = BeautifulSoup(html, "html.parser")

                    # Estratégia 1: meta og:image / twitter:image
                    for attr in [{"property": "og:image"},
                                  {"name": "twitter:image"},
                                  {"property": "og:image:secure_url"}]:
                        tag = soup.find("meta", attrs=attr)
                        if tag and tag.get("content", "").startswith("http"):
                            log_img.info(f"✅ og/twitter t={t}: {tag['content'][:70]}")
                            return tag["content"]

                    # Estratégia 2: JSON-LD schema.org
                    for script in soup.find_all("script",
                                                 type="application/ld+json"):
                        try:
                            data = json.loads(script.string or "")
                            # Pode ser lista ou dict
                            items = data if isinstance(data, list) else [data]
                            for item in items:
                                img = item.get("image")
                                if isinstance(img, str) and img.startswith("http"):
                                    log_img.info(f"✅ JSON-LD t={t}: {img[:70]}")
                                    return img
                                if isinstance(img, list) and img:
                                    candidate = img[0]
                                    if isinstance(candidate, str):
                                        log_img.info(f"✅ JSON-LD[] t={t}: {candidate[:70]}")
                                        return candidate
                                    if isinstance(candidate, dict):
                                        u = candidate.get("url", "")
                                        if u.startswith("http"):
                                            log_img.info(f"✅ JSON-LD t={t}: {u[:70]}")
                                            return u
                        except Exception:
                            pass

                    # Estratégia 3: primeira <img> grande na página
                    for img_tag in soup.find_all("img", src=True):
                        src = img_tag.get("src", "")
                        if not src.startswith("http"):
                            continue
                        w = img_tag.get("width", "0")
                        h = img_tag.get("height", "0")
                        try:
                            if int(w) >= 200 or int(h) >= 200:
                                log_img.info(f"✅ <img> t={t}: {src[:70]}")
                                return src
                        except (ValueError, TypeError):
                            # sem dimensão explícita — pega se tiver product/image no src
                            if any(x in src.lower() for x in
                                   ["product", "produto", "item", "image", "foto"]):
                                log_img.info(f"✅ <img> keyword t={t}: {src[:70]}")
                                return src

        except asyncio.TimeoutError:
            log_img.warning(f"⏱ Timeout t={t}/3")
        except Exception as e:
            log_img.warning(f"⚠️ t={t}/3: {e}")

        if t < 3:
            await asyncio.sleep(1.0)

    log_img.warning(f"❌ Sem imagem após 3 tentativas | {url[:60]}")
    return None



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
    """
    Prioridade absoluta: envia SEMPRE como Imagem + Texto quando há imagem.

    Se img_obj for fornecido:
      • Tenta send_file + caption (caption máx 1024 chars)
      • Se caption > 1024: envia imagem sem caption + mensagem de texto separada
        (ainda são 2 mensagens, mas a imagem não é perdida)
      • Só cai para send_message puro se send_file lançar exceção irrecuperável

    Se img_obj for None: envia texto com link_preview.
    """
    if img_obj:
        if len(msg) <= 1024:
            try:
                return await client.send_file(
                    GRUPO_DESTINO, img_obj,
                    caption=msg, parse_mode="md")
            except Exception as e:
                log_tg.warning(f"⚠️ send_file+caption falhou: {e} — tentando sem caption")
                try:
                    # Tenta enviar imagem sem caption
                    await client.send_file(GRUPO_DESTINO, img_obj)
                    # Envia o texto separado logo em seguida
                    return await client.send_message(
                        GRUPO_DESTINO, msg, parse_mode="md", link_preview=True)
                except Exception as e2:
                    log_tg.warning(f"⚠️ send_file sem caption falhou: {e2} — só texto")
        else:
            # Texto longo: envia imagem primeiro, depois o texto completo
            try:
                await client.send_file(GRUPO_DESTINO, img_obj)
                return await client.send_message(
                    GRUPO_DESTINO, msg, parse_mode="md", link_preview=False)
            except Exception as e:
                log_tg.warning(f"⚠️ send_file texto longo falhou: {e} — só texto")

    # Fallback final: só texto (nunca descarta a oferta por causa da imagem)
    return await client.send_message(
        GRUPO_DESTINO, msg, parse_mode="md", link_preview=True)

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 20 ▸ PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

# Fila de espera para cupons secos (aguarda versão completa)
_FILA_ESPERA: dict = {}   # {hash_simplificado: (texto, ts)}
_FILA_LOCK = asyncio.Lock()

_RE_TITULO_GENERICO = re.compile(
    r'^\s*(?:cupons?\s+(?:shopee|amazon|magalu)|links?\s+de\s+cupom|'
    r'cupons?\s+disponíveis?|novos?\s+cupons?)\s*$',
    re.I | re.M
)


def _eh_cupom_seco(texto: str) -> bool:
    """
    Retorna True se o post for um cupom seco:
    - Título genérico ("Cupons Shopee", "Novos Cupons")
    - Só links, sem valor OFF, sem mínimo, sem código explícito
    """
    tem_titulo_generico = bool(_RE_TITULO_GENERICO.search(texto))
    tem_valor           = bool(re.search(r'R\$\s?[\d.,]+|\d+\s*%', texto))
    tem_codigo          = bool(re.search(r'\b([A-Z][A-Z0-9_-]{3,19})\b', texto))
    tem_descricao       = len([l for l in texto.splitlines()
                                if l.strip() and not re.match(r'https?://', l.strip())
                                and len(l.strip()) > 5]) >= 2

    return tem_titulo_generico and not tem_valor and not tem_codigo and not tem_descricao


def _tem_contexto_real(texto: str) -> bool:
    """
    Retorna True se a mensagem tem contexto textual real de oferta.
    Qualquer texto descritivo com plataforma, campanha ou benefício conta.
    Mesmo sem preço ou cupom.
    """
    linhas_texto = [l.strip() for l in texto.splitlines()
                    if l.strip() and not re.match(r'https?://', l.strip())]
    if not linhas_texto:
        return False

    texto_total = " ".join(linhas_texto)
    tl = texto_total.lower()

    # Tem qualquer indicativo de campanha / benefício / contexto
    indicadores = [
        r'off', r'desconto', r'%', r'r\$', r'promoção', r'oferta',
        r'cupom', r'live', r'evento', r'até\s+\d', r'grátis',
        r'exclusivo', r'parceiro', r'móveis', r'eletro', r'tech',
        r'apartir', r'a\s+partir', r'meia\s+noite', r'começou',
        r'volta', r'normalizou', r'relâmpago', r'flash',
    ]
    for ind in indicadores:
        if re.search(ind, tl):
            return True

    # Texto com mais de 20 chars já é contexto suficiente
    if len(texto_total) > 20:
        return True

    return False


async def processar(event, is_edit: bool = False):
    """
    Pipeline completo com:
    - Bypass para multi-ofertas (não bloqueia "Ofertas Shopee")
    - Sem imagem fallback para cupons Shopee (só Amazon mantém)
    - Cupom seco: aguarda 50s por versão completa antes de enviar
    - Dedup entre grupos (janela curta 2 min para mesmo texto)
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
        loop   = asyncio.get_event_loop()
        mapa_c = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        if str(msg_id) not in mapa_c:
            log_sys.debug(f"⏩ Edit ignorada (preview?): {msg_id}")
            return

    # E3: Filtro de texto (com bypass para multi-ofertas)
    if texto_bloqueado(texto):
        return

    # E4: Limpeza de ruído
    texto_limpo = limpar_ruido_textual(texto)

    # ── E4b: Cupom seco — aguarda versão completa ─────────────────────────
    if not is_edit and _eh_cupom_seco(texto_limpo):
        # Calcula hash do texto para identificar se versão completa chegou
        h_seco = hashlib.sha256(texto_limpo[:100].encode()).hexdigest()[:12]
        async with _FILA_LOCK:
            _FILA_ESPERA.pop(h_seco, None)
        # Após 50s, verifica se já foi enviado por versão completa
        # Se chegou versão completa, o cache dedup vai bloquear este
        log_fil.info(f"⏳ Processando cupom seco após espera | @{uname}")

    # ── E4c: Descarta se não tiver contexto real (só links crus) ─────────
    if not _tem_contexto_real(texto_limpo):
        log_fil.info(f"🗑 Sem contexto real — descartado | @{uname}")
        return

    # E5: Extração de links
    links_conv, links_pres = extrair_links(texto_limpo)
    log_lnk.info(f"🔗 {len(links_conv)} converter | {len(links_pres)} preservar")

    if not links_conv and not links_pres and "fadadoscupons" not in uname:
        log_sys.debug("⏩ Sem links")
        return

    # E6: Conversão paralela
    mapa_links, plat_p = await converter_links(links_conv)

    if links_conv and not mapa_links and not links_pres:
        log_sys.warning(f"🚫 Zero links válidos | @{uname}")
        return

    # E7: Deduplicação semântica (entre grupos, janela curta)
    prod = _extrair_prod_id(mapa_links)
    cup  = _extrair_cupom(texto_limpo)

    if not is_edit:
        if not deve_enviar(plat_p, prod, cup, texto_limpo):
            return

    # E8: Renderização profissional
    msg_final = renderizar(texto_limpo, mapa_links, links_pres, plat_p)
    log_fmt.debug(f"📝 [{plat_p.upper()}]\n{msg_final[:400]}")

    # E9: Imagem
    # ── REGRA: Shopee NÃO usa imagem fallback de arquivo (passa como vier)
    #           Amazon mantém imagem fallback
    #           Magalu mantém imagem fallback
    media_orig = event.message.media
    tem_img    = _tem_midia(media_orig)
    img_obj    = None

    if _eh_cupom(texto_limpo):
        if tem_img:
            img_obj, _ = await preparar_imagem(media_orig, True)
        elif plat_p == "amazon" and os.path.exists(_IMG_AMZ):
            img_obj = _IMG_AMZ   # Amazon: mantém fallback
        elif plat_p == "magalu" and os.path.exists(_IMG_MGL):
            img_obj = _IMG_MGL   # Magalu: mantém fallback
        # Shopee: sem fallback — passa cupom como veio (texto puro)
    else:
        if tem_img:
            img_obj, _ = await preparar_imagem(media_orig, True)
        elif mapa_links:
            # Tenta buscar imagem do produto (4 estratégias, 3 tentativas)
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

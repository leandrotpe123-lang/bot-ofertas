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
_SHP_SECRET = os.environ.get("SHOPEE_SECRET", "")
_MGL_PARTNER  = os.environ.get("MAGALU_PARTNER_ID",  "3440")
_MGL_PROMOTER = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
_MGL_PID      = os.environ.get("MAGALU_PID",         "magazinevoce")
_MGL_SLUG     = os.environ.get("MAGALU_SLUG",        "magazineleo12")
_CUTTLY_KEY = os.environ.get("CUTTLY_API_KEY", "")

_IMG_AMZ = "cupom-amazon.jpg"
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
    return bool(re.search(
        r'(magazineluiza\.com\.br|magazinevoce\.com\.br)'
        r'.*(vitrine|categoria|lista|/l/|/selecao/|/p/|'
        r'promo|stores|loja|busca|ofertas|cupom|produto|departamento)',
        url,
        re.I
    ))


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

    if nl in ("amzn.to", "a.co", "amzn.com"):
    log_amz.debug(f"🔗 Expandindo: {url[:80]}")
    async with _SEM_HTTP:
        exp = await desencurtar_ultra(url, sessao)

elif nl in ("amazon.com.br", "www.amazon.com.br", "amazon.com", "www.amazon.com"):
    log_amz.debug(f"🔗 Direta: {url[:80]}")
    exp = url
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
# MÓDULO 8 ▸ MOTOR SHOPEE (ISOLADO - VERSÃO PROFISSIONAL)
# ══════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════
# MOTOR SHOPEE (API DIRETA)
# ═════════════════════════════════════════════════════════════
async def motor_shopee(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """Motor exclusivo Shopee com retry interno seguro."""
    log_shp.debug(f"🔗 {url[:80]}")

    for tentativa in range(1, 4):
        try:
            ts = str(int(time.time()))

            payload = json.dumps(
                {
                    "query": (
                        f'mutation {{ generateShortLink(input: '
                        f'{{ originUrl: "{url}" }}) {{ shortLink }} }}'
                    )
                },
                separators=(",", ":")
            )

            sig = hashlib.sha256(
                f"{_SHP_APP_ID}{ts}{payload}{_SHP_SECRET}".encode()
            ).hexdigest()

            headers = {
                "Authorization": (
                    f"SHA256 Credential={_SHP_APP_ID},"
                    f"Timestamp={ts},Signature={sig}"
                ),
                "Content-Type": "application/json",
            }

            async with _SEM_HTTP:
                async with sessao.post(
                    "https://open-api.affiliate.shopee.com.br/graphql",
                    data=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=12)
                ) as r:

                    res = await r.json()

                    link = (
                        res.get("data", {})
                        .get("generateShortLink", {})
                        .get("shortLink")
                    )

                    if link:
                        log_shp.info(f"✅ Shopee OK: {link}")
                        return link

                    raise Exception("shortLink vazio")

        except Exception as e:
            log_shp.warning(f"⚠️ tentativa {tentativa}/3: {e}")
            await asyncio.sleep(2 ** tentativa)

    log_shp.error("❌ Shopee API falhou após 3 tentativas")
    return None


# ═════════════════════════════════════════════════════════════
# CAMADA DE GARANTIA (NUNCA ENVIA LINK SEM COMISSÃO)
# ═════════════════════════════════════════════════════════════
async def processar_link_shopee(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """Camada superior de garantia de conversão."""

    link_convertido = await motor_shopee(url, sessao)

    if not link_convertido:
        log_shp.warning("⚠️ Shopee falhou, nova tentativa após delay...")
        await asyncio.sleep(3)

        link_convertido = await motor_shopee(url, sessao)

    if not link_convertido:
        log_shp.error("❌ BLOQUEIO: link sem comissão NÃO será enviado")
        return None

    return link_convertido

# ══════════════════════════════════════════════════════════════════════════════
MÓDULO 9 ▸ MOTOR MAGALU (ELITE VERSION)
#
# PIPELINE CONTROLADO:
#   IN → EXPAND → VALIDATE → NORMALIZE → AFFILIATE → SHORTEN → VERIFY → OUT
#
# ✔ Preserva 100% do path
# ✔ Remove apenas IDs de afiliado
# ✔ Bloqueia sacola/home sem produto
# ✔ Retorno sempre seguro (None se falhar)
# ══════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════
# 1. VALIDAÇÃO ROBUSTA (ANTI SACOLA / HOME / INVALIDO)
# ═════════════════════════════════════════════════════════════
def _eh_link_valido_magalu(url: str) -> bool:
    p = urlparse(url)
    host = p.netloc.lower()
    path = p.path.rstrip("/")

    # sem path útil
    if not path or path == "/":
        return False

    # sacola sem produto
    if "sacola" in host:
        return False

    # path fraco (sem estrutura de produto)
    if len(path.split("/")) < 2:
        return False

    return True


# ═════════════════════════════════════════════════════════════
# 2. NORMALIZAÇÃO DE URL (SEM PERDER PATH)
# ═════════════════════════════════════════════════════════════
def _substituir_ids_magalu(url: str) -> str:
    p = urlparse(url)
    path = p.path

    # magazinevoce fix
    if "magazinevoce.com.br" in p.netloc.lower():
        path = re.sub(r'^(/magazine)[^/]+', rf'\1{_MGL_SLUG}', path)

    params = parse_qs(p.query, keep_blank_values=True)
    clean = {k: v[0] for k, v in params.items()}

    # remove lixo antigo
    for k in [
        "tag", "partnerid", "promoterid",
        "afforcedeeplink", "deeplinkvalue"
    ]:
        clean.pop(k, None)

    # injeta IDs corretos
    clean.update({
        "partner_id": _MGL_PARTNER,
        "promoter_id": _MGL_PROMOTER,
        "utm_source": "divulgador",
        "utm_medium": "magalu",
        "utm_campaign": _MGL_PROMOTER,
        "pid": _MGL_PID,
        "af_force_deeplink": "true",
    })

    base = urlunparse(p._replace(path=path, query="", fragment=""))

    clean["deep_link_value"] = base

    return urlunparse(p._replace(
        path=path,
        query=urlencode(clean),
        fragment=""
    ))


# ═════════════════════════════════════════════════════════════
# 3. ENCURTADOR (RETRY INTELIGENTE)
# ═════════════════════════════════════════════════════════════
async def _cuttly(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    api = f"https://cutt.ly/api/api.php?key={_CUTTLY_KEY}&short={quote(url, safe='')}"

    for t in range(1, 4):
        try:
            async with _SEM_HTTP:
                async with sessao.get(api, timeout=aiohttp.ClientTimeout(total=15)) as r:

                    if r.status != 200:
                        await asyncio.sleep(2 ** t)
                        continue

                    data = await r.json(content_type=None)
                    status = data.get("url", {}).get("status")

                    if status in (7, 2):
                        return data["url"].get("shortLink", url)

        except Exception:
            await asyncio.sleep(2 ** t)

    return None


# ═════════════════════════════════════════════════════════════
# 4. MOTOR PRINCIPAL (PIPELINE CONTROLADO)
# ═════════════════════════════════════════════════════════════
async def motor_magalu(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    log_mgl.debug(f"🔗 IN: {url[:80]}")

    # STEP 1 → EXPANDIR (se necessário)
    try:
        if "maga.lu" in url or classificar(url) == "expandir":
            async with _SEM_HTTP:
                url = await desencurtar_ultra(url, sessao)
    except Exception:
        return None

    log_mgl.debug(f"EXP: {url[:80]}")

    # STEP 2 → VALIDAR ORIGEM
    if classificar(url) != "magalu":
        return None

    # STEP 3 → VALIDAR ESTRUTURA PRODUTO
    if not _eh_link_valido_magalu(url):
        return None

    # STEP 4 → NORMALIZAR / AFILIAR
    afiliado = _substituir_ids_magalu(url)

    # STEP 5 → ENCURTAR FINAL
    final = await _cuttly(afiliado, sessao)

    # STEP 6 → VERIFICAÇÃO FINAL (GARANTIA)
    if not final:
        return None

    log_mgl.info(f"✅ OUT: {final}")
    return final


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 10 ▸ EXTRAÇÃO INTELIGENTE DE LINKS
#
# Extrai TODOS os links de uma mensagem, separando:
#  - links a preservar (wa.me, WhatsApp)
#  - links a converter (Amazon / Shopee / Magalu)
#  - links a descartar (fora da whitelist)
# ══════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════
# REGEX ROBUSTA (ANTI COPY/PASTE LIXO)
# ═════════════════════════════════════════════════════════════
_RE_URL = re.compile(
    r'https?://[^\s\)\]>,"\'<\u200b\u200c\u200d\u2060]+'
)


# ═════════════════════════════════════════════════════════════
# NORMALIZAÇÃO BASE (CLEAN INPUT)
# ═════════════════════════════════════════════════════════════
def _normalizar_url(url: str) -> str:
    return url.strip().rstrip('.,;)>]}\n\r\t ')


# ═════════════════════════════════════════════════════════════
# EXTRAÇÃO BRUTA SEGURA
# ═════════════════════════════════════════════════════════════
def _extrair_bruto(texto: str) -> List[str]:
    return [_normalizar_url(u) for u in _RE_URL.findall(texto)]


# ═════════════════════════════════════════════════════════════
# DETECTOR DE LINK CANÔNICO (CAMPANHAS FIXAS)
# ═════════════════════════════════════════════════════════════
def _eh_link_canonico(url: str) -> bool:
    return any(k in url.lower() for k in (
        "carrinho",
        "cart",
        "checkout",
        "cupom",
        "resgate",
        "promo"
    ))


# ═════════════════════════════════════════════════════════════
# ROUTER PRINCIPAL (CORE DE ENTRADA)
# ═════════════════════════════════════════════════════════════
def extrair_links(texto: str) -> Tuple[List[str], List[str]]:
    """
    Retorna:
        converter → links para motor de afiliado
        preservar → links neutros (WhatsApp etc)

    REGRAS:
        ✔ dedup global
        ✔ ignora lixo externo
        ✔ detecta links de campanha fixa
    """

    brutos = _extrair_bruto(texto)

    converter: List[str] = []
    preservar: List[str] = []

    vistos = set()

    for url in brutos:

        # ─────────────────────────────
        # DEDUP GLOBAL
        # ─────────────────────────────
        if url in vistos:
            continue
        vistos.add(url)

        # ─────────────────────────────
        # CLASSIFICAÇÃO CENTRAL
        # ─────────────────────────────
        try:
            tipo = classificar(url)
        except Exception:
            continue

        # ─────────────────────────────
        # PRESERVAÇÃO
        # ─────────────────────────────
        if tipo == "preservar":
            preservar.append(url)
            continue

        # ─────────────────────────────
        # CAMPANHAS FIXAS (IMPORTANTE)
        # ─────────────────────────────
        if _eh_link_canonico(url):
            # ainda pode converter, mas evita duplicar processamento pesado
            converter.append(url)
            continue

        # ─────────────────────────────
        # LINKS AFILIÁVEIS
        # ─────────────────────────────
        if tipo in ("amazon", "shopee", "magalu", "expandir"):
            converter.append(url)

    # ─────────────────────────────
    # TELEMETRIA (SEM POLUIR LÓGICA)
    # ─────────────────────────────
    log_lnk.debug(
        f"ROUTER | total={len(vistos)} | "
        f"converter={len(converter)} | preservar={len(preservar)}"
    )

    return converter, preservar


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 11 ▸ PIPELINE DE CONVERSÃO PARALELA
# ══════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════
# CONVERSOR UNITÁRIO (ROTEAMENTO ESTANQUE)
# ═════════════════════════════════════════════════════════════
async def _converter_um(url: str, sessao: aiohttp.ClientSession) -> tuple:
    plat = classificar(url)

    async def _rota(u: str, motor, nome: str):
        r = await motor(u, sessao)
        return (r, nome) if r else (None, None)

    # ── Rotas diretas ─────────────────────────────────────────
    if plat == "amazon":
        return await _rota(url, motor_amazon, "amazon")

    if plat == "shopee":
        return await _rota(url, motor_shopee, "shopee")

    if plat == "magalu":
        return await _rota(url, motor_magalu, "magalu")

    # ── Expansão segura ───────────────────────────────────────
    if plat == "expandir":
        log_lnk.debug(f"🔄 expand: {url[:60]}")

        try:
            async with _SEM_HTTP:
                exp = await desencurtar_ultra(url, sessao)
        except Exception:
            return None, None

        p2 = classificar(exp)

        if p2 in ("amazon", "shopee", "magalu"):
            return await _converter_um(exp, sessao)

        log_lnk.debug(f"🗑 descartado pós-expand: {exp[:60]}")
        return None, None

    return None, None


# ═════════════════════════════════════════════════════════════
# PIPELINE PARALELO
# ═════════════════════════════════════════════════════════════
async def converter_links(links: list) -> Tuple[Dict[str, str], str]:

    if not links:
        return {}, "amazon"

    log_lnk.info(f"🚀 convertendo {len(links)} links")

    conn = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False)

    async with aiohttp.ClientSession(
        connector=conn,
        timeout=aiohttp.ClientTimeout(total=40, connect=8),
        headers={"User-Agent": random.choice(USER_AGENTS)},
    ) as sessao:

        tarefas = [
            _converter_um(l, sessao)
            for l in links[:50]
        ]

        resultados = await asyncio.gather(*tarefas, return_exceptions=True)

    mapa: Dict[str, str] = {}
    plats = []

    for i, res in enumerate(resultados):

        if isinstance(res, Exception):
            log_lnk.error(f"❌ erro idx={i}: {res}")
            continue

        novo, plat = res

        if novo and plat:
            mapa[links[i]] = novo
            plats.append(plat)

            log_lnk.debug(f"[{plat.upper()}] {novo[:60]}")

    plataforma_principal = (
        max(set(plats), key=plats.count)
        if plats else "amazon"
    )

    log_lnk.info(f"✅ {len(mapa)}/{len(links)} | main={plataforma_principal}")

    return mapa, plataforma_principal


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 12 ▸ LIMPEZA DE RUÍDO TEXTUAL
# Remove anúncio, CTA quebrado, linhas repetidas, espaços desnecessários.
# Chamada ANTES da formatação final.
# ══════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════
# PRÉ-COMPILAÇÃO (performance + consistência)
# ═════════════════════════════════════════════════════════════
_RE_INVISIVEIS = re.compile(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]')
_RE_GRUPO_EXTERNO = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)[^\s]*',
    re.I
)

_RE_LINHA_RUIDO = re.compile(
    r'^\s*(?:'
    r'-\s*Anúncio|Anúncio|anuncio|'
    r'-\s*Publicidade|Publicidade|'
    r':::+|---+|===+|'
    r'[-–—]\s*(?:ML|MG|AMZ)|'
    r'(?:ML|MG|AMZ)\s*:'
    r')\s*$',
    re.I
)

_RE_CTA_RUIDO = re.compile(
    r'^\s*(?:'
    r'link\s+(?:do\s+)?produto|'
    r'link\s+da\s+oferta|'
    r'resgate\s+aqui|'
    r'clique\s+aqui|'
    r'acesse\s+aqui|'
    r'compre\s+aqui|'
    r'veja\s+aqui|'
    r'assine\s+aqui|'
    r'grupo\s+vip|'
    r'entrar\s+no\s+grupo|'
    r'acessar\s+grupo'
    r')\s*:?\s*',
    re.I
)


# ═════════════════════════════════════════════════════════════
# NORMALIZAÇÃO BASE
# ═════════════════════════════════════════════════════════════
def _normalizar(texto: str) -> str:
    # remove invisíveis
    texto = _RE_INVISIVEIS.sub(" ", texto)

    # normaliza quebras
    texto = texto.replace("\r\n", "\n").replace("\r", "\n")

    return texto


# ═════════════════════════════════════════════════════════════
# LIMPEZA PRINCIPAL (PIPELINE)
# ═════════════════════════════════════════════════════════════
def limpar_ruido_textual(texto: str) -> str:
    """
    Pipeline de limpeza:

    1. normaliza unicode
    2. remove CTAs ruins
    3. remove links de grupo externo
    4. remove linhas lixo estruturais
    5. normaliza linhas vazias
    """

    texto = _normalizar(texto)

    linhas = texto.split("\n")

    resultado = []
    vazio = False

    for linha in linhas:
        l = linha.strip()

        # ─────────────────────────────
        # mantém estrutura de quebra
        # ─────────────────────────────
        if not l:
            if not vazio:
                resultado.append("")
            vazio = True
            continue

        vazio = False

        # ─────────────────────────────
        # remove CTAs de baixo valor
        # ─────────────────────────────
        if _RE_CTA_RUIDO.match(l):
            continue

        # ─────────────────────────────
        # remove links de grupos externos
        # ─────────────────────────────
        if _RE_GRUPO_EXTERNO.search(l):
            l = _RE_GRUPO_EXTERNO.sub("", l).strip()
            if not l:
                continue

        # ─────────────────────────────
        # remove linhas puramente estruturais
        # ─────────────────────────────
        if _RE_LINHA_RUIDO.match(l):
            continue

        resultado.append(l)

    return "\n".join(resultado).strip()
    
        

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 13 ▸ EMOJIS DINÂMICOS E RADARES DE BUSCA
# ══════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════
# RADARES (INTENÇÃO)
# ═════════════════════════════════════════════════════════════
_KW_PRECO = re.compile(r'R\$\s?[\d.,]+', re.I)

_KW_CUPOM = re.compile(
    r'\b(cupom|cupon|código|codigo|coupon|off|resgate|cod)\b',
    re.I
)

_KW_FRETE = re.compile(
    r'\b(frete\s+gráti?s|entrega\s+gráti?s|sem\s+frete)\b',
    re.I
)

_KW_EVENTO = re.compile(
    r'\b(quiz|roleta|miss[aã]o|arena|girar|jogar|desafio)\b',
    re.I
)


# ═════════════════════════════════════════════════════════════
# EMOJIS FIXOS (RODAM DENTRO DO GRUPO)
# ═════════════════════════════════════════════════════════════
_EMJ = {
    "oferta":   ["🔥", "💥", "⚡️", "🚀"],
    "cupom":    ["🚨", "📢", "🔔"],
    "evento":   ["⚠️", "🎯", "🎰"],
    "preco":    ["💰", "💵", "🤑"],
    "codigo":   ["🎟", "🏷", "🎫"],
    "resgate":  ["🔗", "🎯", "✅"],
    "carrinho": ["🛒", "🛍"],
    "frete":    ["🚚", "📦", "✈️"],
}


# ═════════════════════════════════════════════════════════════
# CLASSIFICAÇÃO DE TEXTO
# ═════════════════════════════════════════════════════════════
def _classificar_mensagem(texto: str) -> str:
    if _KW_EVENTO.search(texto):
        return "evento"
    if _KW_CUPOM.search(texto):
        return "cupom"
    return "oferta"


# ═════════════════════════════════════════════════════════════
# DETECTA SE JÁ EXISTE EMOJI
# ═════════════════════════════════════════════════════════════
def _tem_emoji(s: str) -> bool:
    return bool(
        re.search(r'[\U0001F300-\U0001FAFF\U00002600-\U000027BF]', s)
    )


# ═════════════════════════════════════════════════════════════
# EMOJI DO TOPO (ROTAÇÃO CONTROLADA)
# ═════════════════════════════════════════════════════════════
def _emoji_topo(tipo: str) -> str:
    return random.choice(_EMJ.get(tipo, _EMJ["oferta"]))


# ═════════════════════════════════════════════════════════════
# EMOJI DE LINHA (SEM SOBRESCREVER EXISTENTE)
# ═════════════════════════════════════════════════════════════
def _emoji_de_linha(linha: str, plat: str, eh_titulo: bool) -> Optional[str]:

    l = linha.lower()

    if eh_titulo:
        return _emoji_topo(_classificar_mensagem(linha))

    if _KW_FRETE.search(l):
        return random.choice(_EMJ["frete"])

    if _KW_CUPOM.search(l):
        return random.choice(_EMJ["codigo"])

    if _KW_PRECO.search(l):
        return random.choice(_EMJ["preco"])

    if "resgate" in l:
        return random.choice(_EMJ["resgate"])

    if "carrinho" in l:
        return random.choice(_EMJ["carrinho"])

    return None

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ RENDERIZADOR LINEAR (COM CLIQUE E COPIE)
# ══════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════
# REGEX DE LIMPEZA DE PREFIXO
# ═════════════════════════════════════════════════════════════
_RE_LIXO_PREFIXO = re.compile(
    r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:|[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?)\s*',
    re.I
)

_RE_ANUNCIO = re.compile(
    r'^\s*(anúncio|anuncio|publicidade|patrocinado|sponsored)\s*$',
    re.I
)

_RE_URL = re.compile(
    r'https?://[^\s\)\]>,"\'<\u200b\u200c]+'
)


# ═════════════════════════════════════════════════════════════
# CRASE INTELIGENTE (SEM QUEBRAR TEXTO)
# ═════════════════════════════════════════════════════════════
def _aplicar_crase_codigo(linha: str) -> str:

    if "http" in linha or "`" in linha:
        return linha

    def repl(m):
        code = m.group(0)
        if len(code) < 4:
            return code
        return f"`{code}`"

    return re.sub(r'\b[A-Z0-9_-]{4,20}\b', repl, linha)


# ═════════════════════════════════════════════════════════════
# RENDER PRINCIPAL
# ═════════════════════════════════════════════════════════════
def renderizar(texto: str, mapa_links: dict, links_preservar: list, plat: str) -> str:

    mapa = {**mapa_links}
    for u in links_preservar:
        mapa[u] = u

    linhas = texto.split("\n")

    saida = []
    primeiro_texto = True

    for linha in linhas:

        l = linha.strip()

        if not l:
            saida.append("")
            continue

        if _RE_ANUNCIO.match(l):
            saida.append(l)
            continue

        l = _RE_LIXO_PREFIXO.sub("", l).strip()
        if not l:
            continue

        # ─────────────────────────────
        # LINKS
        # ─────────────────────────────
        urls = _RE_URL.findall(l)
        sem_url = _RE_URL.sub("", l).strip()

        if urls and not sem_url:
            for u in urls:
                u = u.rstrip('.,;)>')
                if u in mapa:
                    saida.append(mapa[u])
            continue

        def sub(m):
            u = m.group(0).rstrip('.,;)>')
            return mapa.get(u, "")

        l = _RE_URL.sub(sub, l).strip()
        if not l:
            continue

        # ─────────────────────────────
        # CRASE (CUPOM)
        # ─────────────────────────────
        if "cupom" in l.lower() or "código" in l.lower():
            l = _aplicar_crase_codigo(l)

        # ─────────────────────────────
        # EMOJIS (IMPORTANTE: NÃO SOBRESCREVER EXISTENTES)
        # ─────────────────────────────
        if not _tem_emoji(l):
            emoji = _emoji_de_linha(l, plat, primeiro_texto)
            if emoji:
                l = f"{emoji} {l}"

        # ─────────────────────────────
        # REGRA CARRINHO
        # ─────────────────────────────
        if plat in ("shopee", "magalu") and "carrinho" in l.lower():
            if "🛒" not in l:
                l = "🛒 " + l

        primeiro_texto = False
        saida.append(l)

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

# ─── FUNÇÕES DE APOIO AO ENVIO (ESSENCIAIS) ───────────────────────────────

def _tem_midia(media) -> bool:
    """Verifica se a mensagem tem uma imagem real (ignora o preview de link)."""
    return media is not None and not isinstance(media, MessageMediaWebPage)

def _eh_cupom(texto: str) -> bool:
    """Verifica se o texto é um cupom usando o Radar de Cupom."""
    return bool(_KW_CUPOM.search(texto))



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


async def _processar_interno(event, is_edit: bool = False):
    msg_id = event.message.id
    texto  = event.message.text or ""
    chat   = await event.get_chat()
    uname  = (chat.username or str(event.chat_id)).lower()

    log_tg.info(
        f"{'✏️ EDIT' if is_edit else '📩 NEW'} | "
        f"@{uname} | id={msg_id} | {len(texto)}c | "
        f"fila={len(_buf_fila)} workers={_workers_ativos}")

    if not texto.strip(): return

    if not is_edit:
        if await _foi_processado(msg_id): return
    else:
        loop   = asyncio.get_event_loop()
        mapa_c = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        if str(msg_id) not in mapa_c: return

    if texto_bloqueado(texto): return

    texto_limpo = limpar_ruido_textual(texto)
    if not _tem_contexto_real(texto_limpo): return

    # ── Extração e parse ultra robusto ───────────────────────────────────
    links_conv, links_pres = extrair_links(texto_limpo)
    parsed_validos = parse_links_bulk(links_conv)

    # Reconstrói lista de links usando url_limpa do parser
    links_conv_limpos = [r.url_limpa for r in parsed_validos if r.plat != "expandir"]
    links_expandir    = [r.url_limpa for r in parsed_validos if r.plat == "expandir"]

    if not links_conv_limpos and not links_expandir and not links_pres:
        if "fadadoscupons" not in uname:
            return

    # ── Conversão paralela ────────────────────────────────────────────────
    mapa_links, plat_p = await converter_links(links_conv_limpos + links_expandir)
    if links_conv and not mapa_links and not links_pres: return

    # ── SKU e valor via parser (mais preciso que regex puro) ──────────────
    sku_parsed = ""
    for r in parsed_validos:
        if r.sku:
            sku_parsed = f"{r.plat[:3]}_{r.sku}"
            break
    if not sku_parsed:
        sku_parsed = _extrair_sku(mapa_links)

    valor  = _extrair_valor(texto_limpo)
    cup    = _extrair_cupom(texto_limpo)

    # ── Anti-Saturação Gate (S1/S2 bloqueiam, S3/S4 atrasam) ─────────────
    if not is_edit:
        bloquear, delay_sat, motivo_sat = await antisaturacao_gate(
            plat_p, sku_parsed, valor, texto_limpo)
        if bloquear:
            log_dedup.warning(f"🚫 Saturação: {motivo_sat}")
            return

        # ── Deduplicação Semantic Fingerprint ────────────────────────────
        if not deve_enviar(plat_p, cup, cup, texto_limpo, mapa_links):
            return

        # ── Scheduler Gate ────────────────────────────────────────────────
        delay_sch = await scheduler_gate(plat_p, texto_limpo)
        if delay_sch == -1.0:
            log_sys.warning(f"⚠️ Scheduler: limite/h atingido — descartando")
            return
        delay_total = delay_sch + delay_sat
        if delay_total > 0:
            await asyncio.sleep(delay_total)

    # ── Renderização ──────────────────────────────────────────────────────
    msg_final = renderizar(texto_limpo, mapa_links, links_pres, plat_p)

    # ── Imagem ────────────────────────────────────────────────────────────
    media_orig = event.message.media
    tem_img    = _tem_midia(media_orig)
    img_obj    = None

    if _eh_cupom(texto_limpo):
        if tem_img:
            img_obj, _ = await preparar_imagem(media_orig, True)
        elif plat_p == "amazon" and os.path.exists(_IMG_AMZ):
            img_obj = _IMG_AMZ
        elif plat_p == "magalu" and os.path.exists(_IMG_MGL):
            img_obj = _IMG_MGL
    else:
        if tem_img:
            img_obj, _ = await preparar_imagem(media_orig, True)
        elif mapa_links:
            img_url = await buscar_imagem(list(mapa_links.values())[0])
            if img_url:
                img_obj, _ = await preparar_imagem(img_url, False)

    await _rate_limit()

    # ── Envio ─────────────────────────────────────────────────────────────
    async with _SEM_ENVIO:
        loop = asyncio.get_event_loop()
        mapa = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        try:
            if is_edit:
                id_dest = mapa[str(msg_id)]
                for t in range(1, 4):
                    try:
                        await client.edit_message(GRUPO_DESTINO, id_dest, msg_final)
                        break
                    except MessageNotModifiedError: break
                    except FloodWaitError as e:  await asyncio.sleep(e.seconds)
                    except Exception as e:
                        if t < 3: await asyncio.sleep(2 ** t)
                return

            sent = None
            for t in range(1, 4):
                try:
                    sent = await _enviar(msg_final, img_obj)
                    break
                except FloodWaitError as e: await asyncio.sleep(e.seconds)
                except Exception as e:
                    if t == 1: img_obj = None
                    elif t < 3: await asyncio.sleep(2 ** t)

            if sent:
                mapa[str(msg_id)] = sent.id
                await loop.run_in_executor(_EXECUTOR, salvar_mapa, mapa)
                await _marcar(msg_id)

                # ── Registros pós-envio ───────────────────────────────
                await scheduler_registrar_envio(plat_p)
                antisaturacao_registrar(plat_p, sku_parsed)

                log_sys.info(
                    f"🚀 [OK] @{uname} → {GRUPO_DESTINO} | "
                    f"origem={msg_id} destino={sent.id} | "
                    f"{plat_p.upper()} sku={sku_parsed}")

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

import sqlite3

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


def _init_db():
    conn = sqlite3.connect("ofertas.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ofertas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link TEXT UNIQUE,
            plataforma TEXT,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


async def _run():
    _init_db()

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
    log_sys.info(f"🖼 Pillow: {'OK' if _PIL_OK else 'pip install Pillow'}")
    log_sys.info("🚀 FOGUETÃO v69.0 — IA DE OFERTAS — ONLINE!")

    async def processar(event, is_edit=False):
        texto = event.raw_text if hasattr(event, "raw_text") else ""

        if not texto.strip():
            return

        texto = texto.strip()

        emoji = _emoji_de_linha(texto, "geral", False)
        texto_final = f"{emoji} {texto}"

        log_sys.info(f"📩 Mensagem recebida | edit={is_edit}")
        log_sys.info(f"📝 Texto: {texto_final}")

        try:
            await client.send_message(GRUPO_DESTINO, texto_final)
            log_sys.info("✅ Oferta enviada para o grupo destino")
        except Exception as e:
            log_sys.error(f"❌ Erro ao enviar oferta: {e}", exc_info=True)

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
    

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 23 ▸ BANCO CENTRAL DE OFERTAS — SQLite Persistente
#
# Por que SQLite e não Redis?
#   Railway free tier não garante Redis. SQLite roda no próprio processo,
#   zero dependência externa, persiste em disco, suporta 10k+ writes/s.
#
# TABELAS:
#   ofertas     — toda oferta processada (fingerprint, plat, sku, valor, ts)
#   saturacao   — contagem por plataforma/categoria na janela de tempo
#   scheduler   — controle de horário e score de cada oferta enviada
#
# ÍNDICES: fingerprint (único), plat+ts, sku+plat — queries O(log n)
# ══════════════════════════════════════════════════════════════════════════════

import sqlite3
from contextlib import contextmanager

_DB_PATH = "foguetao.db"
_db_conn: sqlite3.Connection | None = None
_db_lock = Lock()

def _init_db():
    """Cria as tabelas e índices se não existirem. Chamado uma vez no boot."""
    global _db_conn
    _db_conn = sqlite3.connect(_DB_PATH, check_same_thread=False,
                                timeout=10, isolation_level=None)
    _db_conn.execute("PRAGMA journal_mode=WAL")   # write-ahead log — mais rápido
    _db_conn.execute("PRAGMA synchronous=NORMAL") # seguro + rápido
    _db_conn.execute("PRAGMA cache_size=-8000")   # 8MB de cache em memória

    _db_conn.executescript("""
        CREATE TABLE IF NOT EXISTS ofertas (
            fp          TEXT PRIMARY KEY,
            plat        TEXT NOT NULL,
            sku         TEXT,
            valor       TEXT,
            cupons      TEXT,
            camp        TEXT,
            alma        TEXT,
            enviado     INTEGER DEFAULT 1,
            ts          REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS saturacao (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            plat        TEXT NOT NULL,
            categoria   TEXT NOT NULL,
            sku         TEXT,
            ts          REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scheduler (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            plat        TEXT NOT NULL,
            hora        INTEGER NOT NULL,
            score       REAL DEFAULT 1.0,
            enviados    INTEGER DEFAULT 0,
            ts          REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_ofertas_plat_ts
            ON ofertas(plat, ts);
        CREATE INDEX IF NOT EXISTS idx_ofertas_sku
            ON ofertas(sku, plat);
        CREATE INDEX IF NOT EXISTS idx_saturacao_plat_ts
            ON saturacao(plat, ts);
        CREATE INDEX IF NOT EXISTS idx_scheduler_hora
            ON scheduler(plat, hora);
    """)
    log_sys.info(f"🗄 Banco Central ON | {_DB_PATH}")

@contextmanager
def _db():
    """Context manager thread-safe para queries."""
    with _db_lock:
        try:
            yield _db_conn
        except sqlite3.Error as e:
            log_sys.error(f"❌ DB: {e}")
            raise

# ─── API do Banco Central ─────────────────────────────────────────────────────

def db_existe_oferta(fp: str) -> bool:
    """Verifica se o fingerprint já existe. O(1) por índice primário."""
    with _db() as db:
        row = db.execute(
            "SELECT 1 FROM ofertas WHERE fp = ?", (fp,)
        ).fetchone()
    return row is not None

def db_registrar_oferta(fp: str, plat: str, sku: str, valor: str,
                         cupons: list, camp: str, alma: str):
    """Registra oferta enviada no banco central."""
    with _db() as db:
        db.execute("""
            INSERT OR REPLACE INTO ofertas
                (fp, plat, sku, valor, cupons, camp, alma, enviado, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
        """, (fp, plat, sku, valor,
              json.dumps(cupons, ensure_ascii=False),
              camp, alma, time.time()))

def db_buscar_por_sku(sku: str, plat: str,
                       janela_s: float = 3600) -> list[dict]:
    """
    Retorna todas as ofertas do mesmo SKU+plataforma na janela.
    Usado pelo Anti-Saturação para detectar mesmo produto em formas diferentes.
    """
    if not sku:
        return []
    limite = time.time() - janela_s
    with _db() as db:
        rows = db.execute("""
            SELECT fp, valor, cupons, ts FROM ofertas
            WHERE sku = ? AND plat = ? AND ts >= ?
            ORDER BY ts DESC LIMIT 20
        """, (sku, plat, limite)).fetchall()
    return [{"fp": r[0], "valor": r[1],
             "cupons": json.loads(r[2] or "[]"), "ts": r[3]}
            for r in rows]

def db_registrar_saturacao(plat: str, categoria: str, sku: str = ""):
    """Registra 1 envio na tabela de saturação."""
    with _db() as db:
        db.execute(
            "INSERT INTO saturacao (plat, categoria, sku, ts) VALUES (?,?,?,?)",
            (plat, categoria, sku, time.time()))

def db_contagem_saturacao(plat: str, categoria: str,
                           janela_s: float = 3600) -> int:
    """Conta envios de plat+categoria na janela. Usado pelo Anti-Saturação."""
    limite = time.time() - janela_s
    with _db() as db:
        row = db.execute("""
            SELECT COUNT(*) FROM saturacao
            WHERE plat = ? AND categoria = ? AND ts >= ?
        """, (plat, categoria, limite)).fetchone()
    return row[0] if row else 0

def db_registrar_scheduler(plat: str, hora: int, score: float = 1.0):
    """Registra envio no scheduler para cálculo de score por hora."""
    with _db() as db:
        db.execute(
            "INSERT INTO scheduler (plat, hora, score, enviados, ts) VALUES (?,?,?,1,?)",
            (plat, hora, score, time.time()))

def db_score_hora(plat: str, hora: int, janela_dias: int = 7) -> float:
    """
    Retorna o score médio de engajamento para plat+hora nos últimos N dias.
    Score > 1.0 = horário bom. Score < 0.5 = horário ruim.
    """
    limite = time.time() - (janela_dias * 86400)
    with _db() as db:
        row = db.execute("""
            SELECT AVG(score), COUNT(*) FROM scheduler
            WHERE plat = ? AND hora = ? AND ts >= ?
        """, (plat, hora, limite)).fetchone()
    avg, cnt = row if row else (1.0, 0)
    return float(avg or 1.0)

def db_limpar_antigos(dias: int = 7):
    """Remove registros com mais de N dias. Chamado pelo health check."""
    limite = time.time() - (dias * 86400)
    with _db() as db:
        db.execute("DELETE FROM ofertas    WHERE ts < ?", (limite,))
        db.execute("DELETE FROM saturacao  WHERE ts < ?", (limite,))
        db.execute("DELETE FROM scheduler  WHERE ts < ?", (limite,))
    log_sys.info(f"🗑 DB limpeza: registros > {dias}d removidos")

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 24 ▸ PARSER ULTRA ROBUSTO DE LINKS
#
# É o coração técnico do sistema. Toda URL passa por aqui antes de
# ir para o motor da plataforma.
#
# RESPONSABILIDADES:
#   1. Detectar plataforma com 100% de certeza (sem falso positivo)
#   2. Extrair ASIN / SKU / ID do produto de qualquer formato de URL
#   3. Normalizar a URL (remove tracking, corrige encoding, padroniza host)
#   4. Detectar e corrigir links quebrados antes de processar
#   5. Retornar um objeto estruturado ParsedLink — nunca uma string crua
#
# FORMATOS SUPORTADOS:
#   Amazon:  /dp/ASIN, /gp/product/ASIN, /exec/obidos/ASIN,
#            amzn.to/xxx, a.co/xxx, /s?k=, /b?node=
#   Shopee:  /product/SHOPID/ITEMID, /i.SHOPID.ITEMID,
#            shope.ee/xxx, s.shopee.com.br/xxx
#   Magalu:  /produto-nome/p/IDMGL/, /l/lista/, /selecao/nome/,
#            maga.lu/xxx, magazinevoce.com.br/magazineSLUG/
# ══════════════════════════════════════════════════════════════════════════════

from dataclasses import dataclass, field
from typing import Optional

@dataclass
class ParsedLink:
    """Resultado estruturado do parser. Nunca None — sempre tem plat."""
    url_original:  str
    url_limpa:     str          # URL normalizada sem tracking
    plat:          str          # amazon | shopee | magalu | desconhecido
    tipo:          str          # produto | lista | busca | evento | desconhecido
    sku:           str          # ASIN / item_id / ID Magalu (vazio se não achou)
    shop_id:       str          # Shopee: shop_id (vazio para outras plats)
    valido:        bool         # False = link quebrado ou fora da whitelist
    motivo_falha:  str          # Descrição do problema se valido=False


# ─── Padrões de extração por plataforma ──────────────────────────────────────

# Amazon
_AMZ_ASIN_PATTERNS = [
    re.compile(r'/dp/([A-Z0-9]{10})'),
    re.compile(r'/gp/product/([A-Z0-9]{10})'),
    re.compile(r'/exec/obidos/(?:ASIN/)?([A-Z0-9]{10})'),
    re.compile(r'/o/ASIN/([A-Z0-9]{10})'),
    re.compile(r'%2Fdp%2F([A-Z0-9]{10})'),          # encoding duplo
    re.compile(r'asin=([A-Z0-9]{10})', re.I),        # query param
]
_AMZ_BUSCA_PATTERNS = [
    re.compile(r'/s\?'),
    re.compile(r'/s/\?'),
    re.compile(r'/b\?'),
    re.compile(r'/deals'),
    re.compile(r'/gp/goldbox'),
]
_AMZ_EVENTO_PATTERNS = [
    re.compile(r'/events/'),
    re.compile(r'/stores/'),
    re.compile(r'/promotion/psp/'),
]

# Shopee
_SHP_ITEM_PATTERNS = [
    re.compile(r'/product/(\d+)/(\d+)'),              # /product/SHOPID/ITEMID
    re.compile(r'/i\.(\d+)\.(\d+)'),                  # /i.SHOPID.ITEMID
    re.compile(r'[?&]item=(\d+).*?[?&]shop=(\d+)'),  # query params
]
_SHP_BUSCA_PATTERNS = [
    re.compile(r'/search\?'),
    re.compile(r'/category/'),
    re.compile(r'/m/'),                               # campanha mobile
]

# Magalu
_MGL_PRODUTO_PATTERN = re.compile(
    r'/(?:[^/]+/)?p/([a-z0-9]{6,})/?' , re.I)        # /p/IDMGL/
_MGL_LISTA_PATTERN   = re.compile(r'/l/([^/?#]+)')    # /l/nome-lista/
_MGL_SELECAO_PATTERN = re.compile(r'/selecao/([^/?#]+)')
_MGL_SLUG_PATTERN    = re.compile(r'/magazine([^/]+)/')# /magazineSLUG/

# Parâmetros de tracking que o parser remove (lista exaustiva)
_TRACKING_PARAMS = frozenset({
    # Amazon
    "tag","ref","ref_","smid","sprefix","sr","spla","dchild",
    "linkcode","linkid","camp","creative","pf_rd_p","pf_rd_r",
    "pd_rd_wg","pd_rd_w","content-id","pd_rd_r","pd_rd_i",
    "ie","qid","_encoding","dib","dib_tag","m","th","psc",
    "ingress","visitid","s","ascsubtag","btn_ref",
    # Shopee
    "af_siteid","af_sub_siteid","pid","af_click_lookback",
    "is_retargeting","deep_link_value","af_dp",
    # Magalu / genérico
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "partner_id","promoter_id","af_force_deeplink","deep_link_value",
    "pid","c","isretargeting","fbclid","gclid","msclkid",
    "mc_eid","yclid","_ga","_gl",
})

# ─── Funções auxiliares do parser ─────────────────────────────────────────────

def _detectar_plat_por_dominio(netloc: str) -> str:
    """Detecção de plataforma por domínio. Retorna 'desconhecido' se fora."""
    nl = netloc.lower().replace("www.", "")
    for dom in ("amazon.com.br", "amzn.to", "amzn.com", "a.co"):
        if nl == dom or nl.endswith("." + dom): return "amazon"
    for dom in ("shopee.com.br", "s.shopee.com.br", "shope.ee", "shopee.com"):
        if nl == dom or nl.endswith("." + dom): return "shopee"
    for dom in ("magazineluiza.com.br", "magazinevoce.com.br",
                "sacola.magazineluiza.com.br", "maga.lu"):
        if nl == dom or nl.endswith("." + dom): return "magalu"
    return "desconhecido"

def _remover_tracking(parsed: "urllib.parse.ParseResult") -> str:
    """Remove todos os parâmetros de tracking. Preserva parâmetros funcionais."""
    params_orig = parse_qs(parsed.query, keep_blank_values=False)
    params_limpos = {
        k: v[0] for k, v in params_orig.items()
        if k.lower() not in _TRACKING_PARAMS
    }
    return urlunparse(parsed._replace(
        query=urlencode(params_limpos) if params_limpos else "",
        fragment=""
    ))

def _corrigir_encoding(url: str) -> str:
    """Corrige URLs com encoding duplo ou caracteres inválidos."""
    # Decodifica %25XX → %XX (encoding duplo)
    url = re.sub(r'%25([0-9A-Fa-f]{2})', r'%\1', url)
    # Remove espaços escapados incorretamente
    url = url.replace(' ', '%20').replace('\t', '').replace('\n', '')
    return url

def _extrair_sku_amazon(path: str, query: str) -> str:
    """Extrai ASIN de qualquer formato de URL Amazon."""
    texto = path + "?" + query
    for pat in _AMZ_ASIN_PATTERNS:
        m = pat.search(texto)
        if m:
            return m.group(1)
    return ""

def _tipo_amazon(path: str) -> str:
    """Classifica o tipo de URL Amazon."""
    if any(p.search(path) for p in _AMZ_EVENTO_PATTERNS): return "evento"
    if any(p.search(path) for p in _AMZ_BUSCA_PATTERNS):  return "busca"
    for pat in _AMZ_ASIN_PATTERNS:
        if pat.search(path): return "produto"
    return "desconhecido"

def _extrair_sku_shopee(path: str, query: str) -> tuple[str, str]:
    """Extrai (shop_id, item_id) de qualquer formato Shopee."""
    texto = path + "?" + query
    for pat in _SHP_ITEM_PATTERNS:
        m = pat.search(texto)
        if m:
            return m.group(1), m.group(2)   # shop_id, item_id
    return "", ""

def _tipo_shopee(path: str) -> str:
    if any(p.search(path) for p in _SHP_BUSCA_PATTERNS): return "busca"
    for pat in _SHP_ITEM_PATTERNS:
        if pat.search(path): return "produto"
    return "desconhecido"

def _extrair_sku_magalu(path: str) -> str:
    """Extrai ID do produto Magalu."""
    m = _MGL_PRODUTO_PATTERN.search(path)
    if m: return m.group(1)
    return ""

def _tipo_magalu(path: str) -> str:
    if _MGL_LISTA_PATTERN.search(path):   return "lista"
    if _MGL_SELECAO_PATTERN.search(path): return "selecao"
    if _MGL_PRODUTO_PATTERN.search(path): return "produto"
    return "desconhecido"

# ─── Parser principal ─────────────────────────────────────────────────────────

def parse_link(url: str) -> ParsedLink:
    """
    Ponto de entrada único do parser.
    Toda URL do sistema passa por aqui antes de ir para qualquer motor.

    Retorna ParsedLink com:
      - plat, tipo, sku preenchidos
      - url_limpa sem tracking
      - valido=False se link quebrado ou fora da whitelist
    """
    url = url.strip().rstrip('.,;)>')

    # ── Correção de encoding ──────────────────────────────────────────────
    url = _corrigir_encoding(url)

    # ── Validação básica ──────────────────────────────────────────────────
    if not url.startswith(("http://", "https://")):
        return ParsedLink(url, url, "desconhecido", "desconhecido",
                          "", "", False, "Sem esquema HTTP")

    try:
        p = urlparse(url)
    except Exception as e:
        return ParsedLink(url, url, "desconhecido", "desconhecido",
                          "", "", False, f"URL inválida: {e}")

    netloc = p.netloc.lower().replace("www.", "")
    plat   = _detectar_plat_por_dominio(netloc)
    path   = p.path
    query  = p.query

    # ── Fora da whitelist ─────────────────────────────────────────────────
    if plat == "desconhecido":
        # Pode ser encurtador — retorna válido para o pipeline expandir
        for enc in _ENCURTADORES:
            if netloc == enc or netloc.endswith("." + enc):
                url_limpa = _remover_tracking(p)
                return ParsedLink(url, url_limpa, "expandir", "encurtado",
                                  "", "", True, "")
        return ParsedLink(url, url, "desconhecido", "desconhecido",
                          "", "", False, f"Domínio fora da whitelist: {netloc}")

    # ── Remove tracking ───────────────────────────────────────────────────
    url_limpa = _remover_tracking(p)

    # ── Amazon ────────────────────────────────────────────────────────────
    if plat == "amazon":
        sku  = _extrair_sku_amazon(path, query)
        tipo = _tipo_amazon(path)

        # Link quebrado: encurtado sem ASIN e sem path reconhecível
        if not sku and tipo == "desconhecido":
            return ParsedLink(url, url_limpa, plat, tipo,
                              "", "", False,
                              "Amazon sem ASIN e tipo desconhecido")

        return ParsedLink(url, url_limpa, plat, tipo, sku, "", True, "")

    # ── Shopee ────────────────────────────────────────────────────────────
    if plat == "shopee":
        shop_id, item_id = _extrair_sku_shopee(path, query)
        tipo = _tipo_shopee(path)
        sku  = f"{shop_id}.{item_id}" if shop_id and item_id else ""

        return ParsedLink(url, url_limpa, plat, tipo,
                          sku, shop_id, True, "")

    # ── Magalu ────────────────────────────────────────────────────────────
    if plat == "magalu":
        sku  = _extrair_sku_magalu(path)
        tipo = _tipo_magalu(path)

        # Proteção anti-sacola/homepage (mesmo critério do módulo 9)
        if "sacola" in netloc and (not path or path in ("/", "")):
            return ParsedLink(url, url_limpa, plat, tipo,
                              sku, "", False, "Sacola sem produto")

        return ParsedLink(url, url_limpa, plat, tipo, sku, "", True, "")

    # Nunca chega aqui — só para o type checker
    return ParsedLink(url, url_limpa, plat, "desconhecido",
                      "", "", False, "Plataforma não mapeada")


def parse_links_bulk(urls: list[str]) -> list[ParsedLink]:
    """
    Processa uma lista de URLs de uma vez.
    Filtra inválidos e retorna só os válidos (exceto se precisar expandir).
    """
    resultados = [parse_link(u) for u in urls]
    validos = [r for r in resultados if r.valido]
    invalidos = [r for r in resultados if not r.valido]

    for r in invalidos:
        log_lnk.debug(f"🗑 Parser inválido: {r.url_original[:60]} | {r.motivo_falha}")

    log_lnk.info(
        f"🔍 Parser | total={len(urls)} válidos={len(validos)} "
        f"inválidos={len(invalidos)}")
    return validos

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 25 ▸ SCHEDULER INTELIGENTE
#
# O bot aprende quais horários têm mais engajamento por plataforma.
# Não bloqueia envios — aplica um delay adaptativo baseado em score.
#
# LÓGICA:
#   • Cada hora do dia tem um score por plataforma (calculado pelo DB)
#   • Score >= 1.0 → horário bom   → delay = 0s (envia imediato)
#   • Score 0.5–1.0 → horário médio → delay leve (até 5s)
#   • Score < 0.5  → horário ruim  → delay maior (até 15s) — nunca bloqueia
#
#   • Eventos (Quiz/Roleta) → NUNCA sofrem delay (urgente por natureza)
#   • Limite de envios por hora por plataforma (anti-flood orgânico)
#   • Janela noturna configurável (silencia ou reduz entre 00h-07h)
#
# IMPORTANTE: o scheduler NUNCA descarta uma oferta. Só aplica delay.
# ══════════════════════════════════════════════════════════════════════════════

# ─── Configuração ─────────────────────────────────────────────────────────────
_SCH_LIMITE_HORA   = 15    # máx envios/hora por plataforma (anti-flood)
_SCH_JANELA_NOTURNA= (0, 7)  # hora início, hora fim (00h–07h = delay +10s)
_SCH_DELAY_MAX     = 15.0  # delay máximo em segundos
_SCH_DELAY_NOTURNO = 10.0  # delay extra na janela noturna

# ─── Contador em memória (complementa o DB) ───────────────────────────────────
_sch_contadores: dict[str, list[float]] = {}  # {plat: [ts1, ts2, ...]}
_sch_cont_lock = asyncio.Lock()

async def _sch_incrementar(plat: str):
    """Registra 1 envio no contador em memória."""
    async with _sch_cont_lock:
        agora = time.monotonic()
        fila  = _sch_contadores.setdefault(plat, [])
        fila.append(agora)
        # Purga registros > 1h
        _sch_contadores[plat] = [t for t in fila if agora - t < 3600]

async def _sch_count_hora(plat: str) -> int:
    """Retorna quantos envios foram feitos na última hora para a plataforma."""
    async with _sch_cont_lock:
        agora = time.monotonic()
        fila  = _sch_contadores.get(plat, [])
        return sum(1 for t in fila if agora - t < 3600)

def _sch_delay_por_score(score: float, hora_atual: int) -> float:
    """
    Calcula o delay em segundos baseado no score do horário.
    Nunca retorna valor negativo. Nunca bloqueia — só atrasa.
    """
    # Janela noturna: delay base extra
    h_ini, h_fim = _SCH_JANELA_NOTURNA
    delay_base = _SCH_DELAY_NOTURNO if h_ini <= hora_atual < h_fim else 0.0

    if score >= 1.0:
        return delay_base          # horário bom → sem delay adicional
    elif score >= 0.5:
        # Interpolação linear: score 1.0 = 0s, score 0.5 = 5s
        extra = (1.0 - score) * 10.0
        return min(delay_base + extra, _SCH_DELAY_MAX)
    else:
        # Score baixo → delay maior, mas nunca acima do máximo
        extra = (0.5 - score) * 20.0
        return min(delay_base + extra, _SCH_DELAY_MAX)


async def scheduler_gate(plat: str, texto: str) -> float:
    """
    Gate do scheduler. Retorna o delay em segundos que deve ser aplicado.
    Chamado pelo pipeline antes do envio.

    Retorna 0.0 se:
      - É um evento urgente (Quiz/Roleta/Missão)
      - Score do horário é bom
    Retorna delay > 0 se:
      - Horário com score baixo
      - Janela noturna
    Retorna -1.0 se:
      - Limite de envios/hora atingido (o pipeline deve aguardar)
    """
    # Eventos sempre passam sem delay
    if _KW_EVENTO.search(texto):
        log_sys.debug(f"⚡ Scheduler: evento urgente — sem delay | plat={plat}")
        return 0.0

    hora_atual = int(time.strftime("%H"))

    # Verifica limite de envios/hora
    count = await _sch_count_hora(plat)
    if count >= _SCH_LIMITE_HORA:
        log_sys.warning(
            f"⚠️ Scheduler: limite {_SCH_LIMITE_HORA}/h atingido | "
            f"plat={plat} count={count}")
        return -1.0  # sinal de back-pressure

    # Score do horário baseado no histórico do DB
    score = db_score_hora(plat, hora_atual, janela_dias=7)
    delay = _sch_delay_por_score(score, hora_atual)

    if delay > 0:
        log_sys.debug(
            f"⏱ Scheduler delay={delay:.1f}s | "
            f"plat={plat} hora={hora_atual} score={score:.2f}")

    return delay


async def scheduler_registrar_envio(plat: str, score: float = 1.0):
    """
    Registra o envio no scheduler após confirmação de sucesso.
    Permite que o bot aprenda com o histórico real.
    """
    hora = int(time.strftime("%H"))
    await _sch_incrementar(plat)
    db_registrar_scheduler(plat, hora, score)

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 26 ▸ ANTI-SATURAÇÃO ALGORÍTMICA
#
# Vai além da deduplicação. Detecta e previne:
#
#   S1 — FADIGA DE AUDIÊNCIA
#        Mesmo produto aparecendo muitas vezes em poucos minutos
#        (mesmo que com cupons/preços diferentes).
#        Limite: MAX_MESMO_SKU por janela de 1h.
#
#   S2 — REPETIÇÃO SEMÂNTICA
#        Mesmo produto em forma diferente (outro texto, outro link, outro cupom).
#        Detectado via SKU no banco + similaridade de valor.
#
#   S3 — DENSIDADE DE PLATAFORMA
#        Muitas ofertas da mesma plataforma em sequência.
#        Limite: MAX_MESMA_PLAT ofertas em janela de 30min.
#        Não bloqueia — aplica delay maior via scheduler.
#
#   S4 — PADRÃO TELEGRAM (anti-spam orgânico)
#        Detecta se o canal está enviando em rajadas (burst).
#        Se > BURST_LIMITE mensagens em < BURST_JANELA segundos → pausa.
#
# IMPORTANTE:
#   S1 e S2 BLOQUEIAM (a oferta é descartada — é spam real).
#   S3 e S4 ATRASAM (via scheduler — a oferta vai, mas com delay).
#   Eventos nunca são bloqueados por S3/S4.
# ══════════════════════════════════════════════════════════════════════════════

# ─── Configuração ─────────────────────────────────────────────────────────────
_SAT_MAX_MESMO_SKU    = 2      # S1: máx vezes o mesmo SKU em 1h
_SAT_MAX_MESMA_PLAT   = 8      # S3: máx ofertas da mesma plat em 30min
_SAT_JANELA_SKU       = 3600   # S1: janela em segundos (1h)
_SAT_JANELA_PLAT      = 1800   # S3: janela em segundos (30min)
_SAT_BURST_LIMITE     = 5      # S4: máx mensagens em rajada
_SAT_BURST_JANELA     = 60     # S4: janela de rajada em segundos
_SAT_SIM_SEMANTICA    = 0.85   # S2: limiar de similaridade de valor para "mesmo produto"

# ─── Rastreador de burst em memória ───────────────────────────────────────────
_burst_ts: list[float] = []
_burst_lock = asyncio.Lock()

async def _registrar_burst():
    async with _burst_lock:
        agora = time.monotonic()
        _burst_ts.append(agora)
        # Mantém só os da janela atual
        while _burst_ts and agora - _burst_ts[0] > _SAT_BURST_JANELA:
            _burst_ts.pop(0)

async def _count_burst() -> int:
    async with _burst_lock:
        agora = time.monotonic()
        return sum(1 for t in _burst_ts if agora - t <= _SAT_BURST_JANELA)

# ─── Funções de análise ───────────────────────────────────────────────────────

def _sat_verificar_s1_s2(plat: str, sku: str, valor: str) -> tuple[bool, str]:
    """
    Verifica S1 (fadiga de SKU) e S2 (repetição semântica).
    Retorna (bloquear: bool, motivo: str).
    """
    if not sku:
        return False, ""  # sem SKU não tem como detectar repetição de produto

    historico = db_buscar_por_sku(sku, plat, _SAT_JANELA_SKU)

    # S1: mesmo SKU apareceu muitas vezes
    if len(historico) >= _SAT_MESMO_SKU:
        return True, (
            f"S1-FADIGA | sku={sku} plat={plat} "
            f"aparições={len(historico)} janela={_SAT_JANELA_SKU//60}min")

    # S2: mesmo SKU com valor parecido (mesmo produto em outra forma)
    if valor and historico:
        for entrada in historico:
            val_ant = entrada.get("valor", "")
            if not val_ant:
                continue
            try:
                v_novo = float(re.sub(r'[^\d]', '', valor) or "0")
                v_ant  = float(re.sub(r'[^\d]', '', val_ant) or "0")
                if v_novo > 0 and v_ant > 0:
                    diff = abs(v_novo - v_ant) / max(v_novo, v_ant)
                    if diff <= (1 - _SAT_SIM_SEMANTICA):  # ex: <= 15% diferença
                        return True, (
                            f"S2-SEMANTICO | sku={sku} val_novo={valor} "
                            f"val_ant={val_ant} diff={diff:.1%}")
            except (ValueError, ZeroDivisionError):
                continue

    return False, ""

_SAT_MESMO_SKU = _SAT_MAX_MESMO_SKU  # alias para clareza interna


def _sat_verificar_s3(plat: str) -> bool:
    """
    S3: verifica densidade de plataforma. Retorna True se densa.
    Não bloqueia — sinaliza para o scheduler aplicar delay.
    """
    count = db_contagem_saturacao(plat, "envio", _SAT_JANELA_PLAT)
    return count >= _SAT_MAX_MESMA_PLAT


async def _sat_verificar_s4() -> bool:
    """S4: detecta burst. Retorna True se em rajada."""
    return await _count_burst() >= _SAT_BURST_LIMITE


# ─── Gate principal do Anti-Saturação ────────────────────────────────────────

async def antisaturacao_gate(plat: str, sku: str, valor: str,
                              texto: str) -> tuple[bool, float, str]:
    """
    Gate único de anti-saturação. Chamado pelo pipeline antes do envio.

    Retorna:
      (bloquear: bool, delay_extra: float, motivo: str)

      bloquear=True  → oferta descartada (S1/S2)
      bloquear=False, delay_extra>0 → envia com delay extra (S3/S4)
      bloquear=False, delay_extra=0 → envia normalmente
    """
    # Eventos nunca sofrem S3/S4
    eh_evt = _KW_EVENTO.search(texto)

    # S1 + S2: verificação de fadiga e semântica
    bloquear, motivo = _sat_verificar_s1_s2(plat, sku, valor)
    if bloquear:
        log_dedup.warning(f"🚫 Anti-Saturação BLOQUEIA | {motivo}")
        return True, 0.0, motivo

    delay_extra = 0.0

    # S3: densidade de plataforma
    if not eh_evt and _sat_verificar_s3(plat):
        delay_extra += 8.0
        log_dedup.info(
            f"⏱ Anti-Saturação S3 | plat={plat} delay+8s")

    # S4: burst
    if not eh_evt and await _sat_verificar_s4():
        delay_extra += 5.0
        log_dedup.info(
            f"⏱ Anti-Saturação S4 | burst detectado delay+5s")

    return False, delay_extra, ""


def antisaturacao_registrar(plat: str, sku: str):
    """Registra envio bem-sucedido nas tabelas de saturação."""
    db_registrar_saturacao(plat, "envio", sku)

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║   FOGUETÃO v74.0 — ARQUITETURA SÊNIOR CONSOLIDADA                      ║
# ║   Pipeline único · Dedup semântica · Isolamento total por plataforma    ║
# ╚══════════════════════════════════════════════════════════════════════════╝
#
# DECISÃO CENTRAL: cupom + texto + contexto semântico
# NUNCA: link, emoji, preço, formatação, encurtador
#
# ORDEM DOS MÓDULOS
# 1 Registros    Erro SessionPasswordNeededError,
# Erro de mensagem não modificada,
# 3 Persistência JSON
# 4 Filtro de texto
#5 Whitelist/classificação de domínio
# 6 Descurtador
# 7 Motor Amazon (isolado)
# 8 Motor Shopee (isolado)
# 9 Motor Magalu (isolado)
# 10 Extração de links
# 11 Pipeline de conversão paralela
#12 Limpeza de ruído textual
#13 Emojis + radares semânticos
# 14 Renderizador
# 15 Deduplicação semântica (DNA de impressão digital)
# 16 Buscador de imagem
# 17 Limite de taxa interno
# 18 Anti-loop de edição
# 19 Envio (prioridade imagem)
# 20 Banco central SQLite
# 21 Parser ultra robusto
# 22 Agendador inteligente
# 23 Anti-saturação algorítmica
#24 Orquestrador de buffer + principal de pipeline
# 25 Verificação de saúde
#26 Inicialização com reinicialização automática

from __future__ import annotations

import asyncio
importar futuros concorrentes
import hashlib
Bretanha heapq
importar ei io
json
registro de importação

grega re
sqlite3
tempo de exportação
importar dados unicode
from contextlib import contextmanager
from dataclasses import dataclass
from difflib import SequenceMatcher
from threading import Lock
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

import aiohttp
Importe BeautifulSoup para bs4
from telethon import TelegramClient, events

_DB_PATH = os.getenv("DB_PATH") or "database.db"
_db_conn = None

if os.path.dirname(_DB_PATH):
    os makedirs (os.path.dirname(_DB_PATH),exist_ok=True)
    
from telethon.errors import   (
    AuthKeyUnregisteredError,
    Erro de esperança de inundação,
    Erro de mensagem não modificada,
    Erro SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage

tentar :
    Importar imagem de PIL
    _PIL_OK = Verdadeiro
exceto ImportError:
    _PIL_OK = Falso


# ════════════════════════════════════════════════════════════════════════
# MÓDULO 1 ▸ REGISTRO DE REGRAS
# ════════════════════════════════════════════════════════════════════════

def  _mk_log ( nome: str, cor: str ) -> logging. Logger :
    lg = registro. getLogger ( nome )
    se  não forem manipuladores de nível :
        h = logging. StreamHandler ( )
        h. setFormatter ( logging. Formatter (
            f'\033[ { cor } m[%(name)-10s]\033[0m %(asctime)s | %(levelname)-8s | %(message)s' ,
            datefmt= '%H:%M:%S' ) )
        lg. adicionarManipulador ( h )
        lg. setLevel ( logging. DEBUG )
    retornar lg

log_amz = _mk_log ( 'AMAZON' ,    '1;33' )
log_shp = _mk_log ( 'LOJA' ,    '1;38;5;208' )
log_mgl = _mk_log ( 'MAGALU' ,    '1;34' )
log_dedup= _mk_log ( 'DEDUP' ,     '1;35' )
log_img = _mk_log ( 'IMAGEM' ,    '1;36' )
log_tg = _mk_log ( 'TELEGRAM' , '1;32' )
log_fil = _mk_log ( 'FILTRO' ,    '1;31' )
log_lnk = _mk_log ( 'LINKS' ,     '1;38;5;51' )
log_fmt = _mk_log ( 'FORMAT' ,    '1;33' )
log_sys = _mk_log ( 'SISTEMA' ,   '1;37' )
log_hc   = _mk_log('HEALTH',   '1;38;5;118')


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 2 ▸ CONFIGURAÇÕES
# ══════════════════════════════════════════════════════════════════════════════

API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION", "")

GRUPOS_ORIGEM  = ["promotom", "fadapromos", "SamuelF3lipePromo", "paraseubaby", "fumotom", "botofera", "fadadoscupons"]
GRUPO_DESTINO  = "@ofertap"

# Credenciais isoladas — NUNCA misturar entre plataformas
_AMZ_TAG      = os.environ.get("AMAZON_TAG",         "leo21073-20")
_SHP_APP_ID   = os.environ.get("SHOPEE_APP_ID",      "18348480261")
_SHP_SECRET   = os.environ.get("SHOPEE_SECRET",      "")
_MGL_PARTNER  = os.environ.get("MAGALU_PARTNER_ID",  "3440")
_MGL_PROMOTER = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
_MGL_PID      = os.environ.get("MAGALU_PID",         "magazinevoce")
_MGL_SLUG     = os.environ.get("MAGALU_SLUG",        "magazineleo12")
_CUTTLY_KEY   = os.environ.get("CUTTLY_API_KEY",     "")

_IMG_SHP = "cupom-shopee.jpg"
_IMG_AMZ = "cupom-amazon.jpg"
_IMG_MGL = "magalu_promo.jpg"

ARQUIVO_CACHE      = "cache_dedup.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

_SEM_ENVIO     = asyncio.Semaphore(3)
_SEM_HTTP      = asyncio.Semaphore(20)
_EXECUTOR      = concurrent.futures.ThreadPoolExecutor(max_workers=4)
_RATE_LOCK     = asyncio.Lock()
_ULTIMO_ENV_TS = 0.0
_INTERVALO_MIN = 1.5

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

_ENCURTADORES = frozenset([
    "bit.ly", "cutt.ly", "tinyurl.com", "t.co", "ow.ly",
    "goo.gl", "rb.gy", "is.gd", "tiny.cc", "buff.ly",
    "short.io", "bl.ink", "rebrand.ly", "shorturl.at",
])

_PRESERVE = frozenset(["wa.me", "api.whatsapp.com"])
_DELETAR  = frozenset(["t.me", "telegram.me", "telegram.org", "chat.whatsapp.com"])


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 3 ▸ PERSISTÊNCIA JSON
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
# MÓDULO 4 ▸ FILTRO DE TEXTO
# Produto único na lista → bloqueia.
# Múltiplos produtos na mesma mensagem → passa sempre.
# ══════════════════════════════════════════════════════════════════════════════

_FILTRO_TEXTO = [
    "Monitor Samsung", "Computador Home Essential", "Monitor gamer","Roleta Shopee Flamengo", "Fonte Mancer", "Placa de video", "Monitor LG",
    "PC home Essential", "Suporte articulado", "Gabinetes em oferta",
    "VHAGAR", "Superframe", "AM5", "AM4", "GTX", "DDR5", "DDR4",
    "Dram", "Monitor Safe", "Monitor Redragon", "CL18", "CL16", "CL32",
    "MT/s", "MHz", "RX 580", "Ryzen", "Placa Mãe", "Gabinete Gamer",
    "Water Cooler", "Air Cooler",
]

_RE_MULTI_OFERTA = re.compile(
    r'\b(?:ofertas?|promoções?)\s+(?:na\s+|no\s+|da\s+)?'
    r'(?:shopee|amazon|magalu|magazine\s*luiza)\b',
    re.I,
)

# Detecta múltiplos produtos: 3+ linhas com preço OU 3+ URLs na mesma msg
_RE_PRECO_LINHA = re.compile(r'R\$\s?[\d.,]+')
_RE_URL_COUNT   = re.compile(r'https?://')

def _eh_multi_produto(texto: str) -> bool:
    """Retorna True se a mensagem contém vários produtos."""
    if _RE_MULTI_OFERTA.search(texto):
        return True
    linhas_preco = sum(1 for l in texto.splitlines()
                       if _RE_PRECO_LINHA.search(l))
    urls         = len(_RE_URL_COUNT.findall(texto))
    return linhas_preco >= 2 or urls >= 3

def texto_bloqueado(texto: str) -> bool:
    """
    Multi-produto → nunca bloqueia.
    Produto único → bloqueia se tiver palavra da lista.
    """
    if _eh_multi_produto(texto):
        log_fil.debug("✅ Multi-produto — bypass filtro")
        return False
    tl = texto.lower()
    for p in _FILTRO_TEXTO:
        if p.lower() in tl:
            log_fil.debug(f"🚫 Filtro bloqueou: '{p}'")
            return True
    return False

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 5 ▸ WHITELIST + CLASSIFICAÇÃO
# Adiciona amzlink.to e variantes Amazon na whitelist
# ══════════════════════════════════════════════════════════════════════════════

_PRESERVE = frozenset(["wa.me", "api.whatsapp.com"])
_DELETAR  = frozenset([
    "t.me", "telegram.me", "telegram.org", "chat.whatsapp.com"])

# Todos os domínios Amazon encurtados conhecidos
_AMZ_DOMINIOS = frozenset({
    "amazon.com.br", "amazon.com",
    "amzn.to", "amzn.com", "a.co",
    "amzlink.to",          # ← adicionado
    "amzn.eu",
})

_SHP_DOMINIOS = frozenset({
    "shopee.com.br", "s.shopee.com.br",
    "shopee.com", "shope.ee",
})

_MGL_DOMINIOS = frozenset({
    "magazineluiza.com.br", "sacola.magazineluiza.com.br",
    "magazinevoce.com.br", "maga.lu",
})

_ENCURTADORES = frozenset([
    "bit.ly", "cutt.ly", "tinyurl.com", "t.co", "ow.ly",
    "goo.gl", "rb.gy", "is.gd", "tiny.cc", "buff.ly",
    "short.io", "bl.ink", "rebrand.ly", "shorturl.at",
])

def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""

def _eh_link_grupo_externo(url: str) -> bool:
    nl = _netloc(url)
    return any(nl == d or nl.endswith("." + d) for d in _DELETAR)

def classificar(url: str) -> Optional[str]:
    """
    Retorna: 'amazon' | 'shopee' | 'magalu' | 'preservar' | 'expandir' | None
    amzlink.to agora é detectado como 'amazon' diretamente.
    """
    nl = _netloc(url)
    if not nl:
        return None
    if _eh_link_grupo_externo(url):
        return None
    for d in _PRESERVE:
        if nl == d or nl.endswith("." + d):
            return "preservar"

    # Magalu primeiro — evita cross-linking
    for dom in _MGL_DOMINIOS:
        if nl == dom or nl.endswith("." + dom):
            return "magalu"

    # Amazon — inclui amzlink.to
    for dom in _AMZ_DOMINIOS:
        if nl == dom or nl.endswith("." + dom):
            return "amazon"

    # Shopee
    for dom in _SHP_DOMINIOS:
        if nl == dom or nl.endswith("." + dom):
            return "shopee"

    # Encurtadores genéricos
    for enc in _ENCURTADORES:
        if nl == enc or nl.endswith("." + enc):
            return "expandir"

    return None

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 6 ▸ DESENCURTADOR — 15 CAMADAS
# Suporta amzlink.to, amzn.to, bit.ly e qualquer outro.
# HEAD → GET completo → meta-refresh → JS → og:url → canônico
# ══════════════════════════════════════════════════════════════════════════════

# Domínios que SEMPRE precisam de GET (bloqueiam HEAD)
_FORCA_GET = frozenset({
    "amzlink.to", "amzn.to", "a.co", "amzn.com",
    "bit.ly", "tinyurl.com", "rb.gy", "is.gd",
    "cutt.ly", "ow.ly", "buff.ly",
})

async def desencurtar(url: str, sessao: aiohttp.ClientSession,
                       depth: int = 0) -> str:
    if depth > 15:
        log_lnk.warning(f"⚠️ Profundidade máx atingida d={depth}: {url[:60]}")
        return url

    url = url.strip().rstrip('.,;)>')
    if not url.startswith(("http://", "https://")):
        return url

    nl = _netloc(url)

    hdrs = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    try:
        # HEAD só para domínios que aceitam
        usar_head = nl not in _FORCA_GET and not any(
            nl.endswith("." + d) for d in _FORCA_GET)

        if usar_head:
            try:
                async with sessao.head(
                    url, headers=hdrs, allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=8),
                    max_redirects=20,
                ) as r:
                    final = str(r.url)
                    if final != url:
                        log_lnk.debug(f"  HEAD d={depth} → {final[:70]}")
                        return await desencurtar(final, sessao, depth + 1)
                    # HEAD retornou mesma URL mas pode ter JS redirect
                    # não retorna ainda, cai no GET
            except Exception:
                pass

        # GET completo — lê HTML para detectar qualquer tipo de redirect
        async with sessao.get(
            url, headers=hdrs, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=15),
            max_redirects=20,
        ) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")

            # Redirect HTTP já resolvido
            if pos != url:
                log_lnk.debug(f"  GET-redir d={depth} → {pos[:70]}")
                return await desencurtar(pos, sessao, depth + 1)

            soup = BeautifulSoup(html, "html.parser")

            # Meta refresh
            ref = soup.find(
                "meta", attrs={"http-equiv": re.compile("refresh", re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)",
                              ref["content"], re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    if novo.startswith("http"):
                        log_lnk.debug(f"  META d={depth} → {novo[:70]}")
                        return await desencurtar(novo, sessao, depth + 1)

            # JS location — amzlink.to usa window.location
            for pat in [
                r'window\.location(?:\.href)?\s*=\s*["\']([^"\']{15,})["\']',
                r'location\.replace\s*\(\s*["\']([^"\']{15,})["\']\s*\)',
                r'location\.assign\s*\(\s*["\']([^"\']{15,})["\']\s*\)',
                r'(?:var|let|const)\s+\w*[Uu]rl\w*\s*=\s*["\']([^"\']{15,})["\']',
                r'window\.open\s*\(\s*["\']([^"\']{15,})["\']',
            ]:
                mj = re.search(pat, html)
                if mj:
                    destino = mj.group(1)
                    if destino.startswith("http"):
                        log_lnk.debug(f"  JS d={depth} → {destino[:70]}")
                        return await desencurtar(destino, sessao, depth + 1)

            # og:url como fallback
            og = soup.find("meta", attrs={"property": "og:url"})
            if og and og.get("content", "").startswith("http"):
                if og["content"] != url:
                    log_lnk.debug(f"  OG:URL d={depth} → {og['content'][:70]}")
                    return await desencurtar(og["content"], sessao, depth + 1)

            # Link canônico
            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href", "").startswith("http"):
                if canon["href"] != url:
                    log_lnk.debug(f"  CANON d={depth} → {canon['href'][:70]}")
                    return await desencurtar(canon["href"], sessao, depth + 1)

            log_lnk.debug(f"  FIM d={depth}: {pos[:70]}")
            return pos

    except asyncio.TimeoutError:
        log_lnk.warning(f"⏱ Timeout d={depth}: {url[:60]}")
        return url
    except Exception as e:
        log_lnk.error(f"❌ Desencurtar d={depth}: {e} | {url[:50]}")
        return url

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 7 ▸ MOTOR AMAZON
# Corrige: link com nome do produto → sempre /dp/ASIN?tag=
# Corrige: amzlink.to desencurtado corretamente
# ══════════════════════════════════════════════════════════════════════════════

_AMZ_LIXO = frozenset({
    "ascsubtag","btn_ref","ref_","ref","smid","sprefix","spla",
    "dchild","linkcode","linkid","camp","creative",
    "pf_rd_p","pf_rd_r","pd_rd_wg","pd_rd_w","content-id",
    "pd_rd_r","pd_rd_i","ie","qid","_encoding","dib","dib_tag",
    "m","marketplaceid","ufe","th","psc","ingress","visitid",
    "lp_context_asin","redirectasin","redirectmerchantid",
    "redirectasincustomeraction","ds","rnid","sr",
})
_AMZ_MANTER = frozenset({
    "tag","keywords","node","k","i","rh","n","field-keywords",
})

# Domínios que precisam de desencurtamento antes de limpar
_AMZ_PRECISA_EXPAND = frozenset({
    "amzlink.to","amzn.to","a.co","amzn.com","amzn.eu",
})


def _extrair_asin_da_url(url: str) -> Optional[str]:
    """
    Extrai ASIN de qualquer formato de URL Amazon.
    Retorna None se não encontrar.
    """
    path = urlparse(url).path

    # /dp/ASIN ou /dp/ASIN/
    m = re.search(r'/dp/([A-Z0-9]{10})(?:/|$|\?)', path)
    if m: return m.group(1)

    # /gp/product/ASIN
    m = re.search(r'/gp/product/([A-Z0-9]{10})(?:/|$|\?)', path)
    if m: return m.group(1)

    # /exec/obidos/ASIN/ASIN
    m = re.search(r'/exec/obidos/(?:ASIN/)?([A-Z0-9]{10})', path)
    if m: return m.group(1)

    # query param asin=
    m = re.search(r'[?&]asin=([A-Z0-9]{10})', url, re.I)
    if m: return m.group(1)

    return None


def _limpar_url_amazon(url: str) -> str:
    """
    Reconstrói URL Amazon SEMPRE no formato mais limpo possível.

    COM ASIN → amazon.com.br/dp/ASIN?tag=  (sem nome do produto no path)
    Campanha  → path exato + tag
    Busca     → path + params funcionais + tag
    """
    p    = urlparse(url)
    path = p.path

    # Tenta extrair ASIN de qualquer formato
    asin = _extrair_asin_da_url(url)

    if asin:
        # SEMPRE usa formato limpo /dp/ASIN — remove nome do produto do path
        return f"https://www.amazon.com.br/dp/{asin}?tag={_AMZ_TAG}"

    # Campanha /promotion/psp/
    if "/promotion/psp/" in path:
        return urlunparse(p._replace(
            scheme="https",
            netloc="www.amazon.com.br",
            query=f"tag={_AMZ_TAG}",
            fragment=""))

    # Busca, eventos, landing pages — preserva params funcionais
    params: dict = {}
    for k, v in parse_qs(p.query, keep_blank_values=False).items():
        kl = k.lower()
        if kl in _AMZ_MANTER:
            params[k] = v[0]
        elif kl not in _AMZ_LIXO and len(v[0]) < 60:
            params[k] = v[0]
    params["tag"] = _AMZ_TAG

    return urlunparse(p._replace(
        scheme="https",
        netloc="www.amazon.com.br",
        query=urlencode(params),
        fragment=""))


async def motor_amazon(url: str,
                        sessao: aiohttp.ClientSession) -> Optional[str]:
    """
    Motor exclusivo Amazon.
    Desencurta SEMPRE — captura amzlink.to, amzn.to e redirects silenciosos.
    Retorna /dp/ASIN?tag= sem nome do produto no path.
    """
    log_amz.debug(f"▶ IN: {url[:80]}")

    # Verifica cache antes de desencurtar
    cached = db_get_link(url)
    if cached:
        log_amz.debug(f"💾 Cache: {cached[:60]}")
        return cached

    # Sempre desencurta
    try:
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)
    except Exception as e:
        log_amz.error(f"❌ Desencurtar: {e} | {url[:60]}")
        return None

    log_amz.debug(f"  EXP: {exp[:80]}")

    if classificar(exp) != "amazon":
        log_amz.warning(f"  ⚠️ Não é Amazon após expansão: {exp[:70]}")
        return None

    final = _limpar_url_amazon(exp)
    log_amz.info(f"  ✅ OUT: {final}")

    # Salva no cache
    db_set_link(url, final, "amazon")
    return final

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ MOTOR SHOPEE — ISOLADO
# Chega pronta. NUNCA desencurta. API GraphQL oficial. Retry 3x.
# Retorna None em falha — pipeline descarta link sem comissão.
# ══════════════════════════════════════════════════════════════════════════════

async def motor_shopee(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    log_shp.debug(f"🔗 {url[:80]}")
    for t in range(1, 4):
        try:
            ts      = str(int(time.time()))
            payload = json.dumps(
                {"query": (
                    f'mutation {{ generateShortLink(input: '
                    f'{{ originUrl: "{url}" }}) {{ shortLink }} }}'
                )},
                separators=(",", ":"),
            )
            sig  = hashlib.sha256(
                f"{_SHP_APP_ID}{ts}{payload}{_SHP_SECRET}".encode()
            ).hexdigest()
            hdrs = {
                "Authorization": (
                    f"SHA256 Credential={_SHP_APP_ID},"
                    f"Timestamp={ts},Signature={sig}"
                ),
                "Content-Type": "application/json",
            }
            async with _SEM_HTTP:
                async with sessao.post(
                    "https://open-api.affiliate.shopee.com.br/graphql",
                    data=payload, headers=hdrs,
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as r:
                    res  = await r.json()
                    link = (res.get("data", {})
                               .get("generateShortLink", {})
                               .get("shortLink"))
                    if link:
                        log_shp.info(f"✅ {link}")
                        return link
                    raise ValueError("shortLink vazio")
        except Exception as e:
            log_shp.warning(f"⚠️ t={t}/3: {e}")
            await asyncio.sleep(2 ** t)

    log_shp.error("❌ Shopee API falhou 3x — link descartado")
    return None

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 9 ▸ MOTOR MAGALU + CUTTLY
# Corrige: HTTP 429 (rate limit) e 401 (auth) com backoff inteligente
# HTTP 429 → espera 60s antes de tentar de novo
# HTTP 401 → log de erro de chave, não tenta mais
# Fallback: envia link longo afiliado + edita depois
# ══════════════════════════════════════════════════════════════════════════════

# Controle de rate limit do Cuttly
_CUTTLY_LAST_429: float = 0.0
_CUTTLY_BACKOFF:  float = 60.0   # espera após 429

def _validar_magalu(url: str) -> bool:
    p    = urlparse(url)
    host = p.netloc.lower()
    path = p.path.rstrip("/")
    if "sacola" in host and path in ("", "/"):
        log_mgl.warning(f"⚠️ Sacola sem produto: {url[:60]}")
        return False
    if not path or path == "/" or len(path.split("/")) < 2:
        log_mgl.warning(f"⚠️ Homepage sem produto: {url[:60]}")
        return False
    return True


def _afiliar_magalu(url: str) -> str:
    """Substitui slug do afiliado e injeta IDs. Preserva 100% do path."""
    p    = urlparse(url)
    path = p.path

    # Troca /magazineQUALQUERCOISA/ pelo slug configurado
    path = re.sub(r'^(/magazine)[^/]+', rf'\1{_MGL_SLUG}', path)
    log_mgl.debug(f"  Slug: {path[:60]}")

    params = {k: v[0] for k, v in
              parse_qs(p.query, keep_blank_values=True).items()}
    for k in [
        "tag","partnerid","promoterid","afforcedeeplink",
        "deeplinkvalue","isretargeting","partner_id","promoter_id",
        "utm_source","utm_medium","utm_campaign","pid","c",
        "af_force_deeplink","deep_link_value",
    ]:
        params.pop(k, None)

    params.update({
        "partner_id":        _MGL_PARTNER,
        "promoter_id":       _MGL_PROMOTER,
        "utm_source":        "divulgador",
        "utm_medium":        "magalu",
        "utm_campaign":      _MGL_PROMOTER,
        "pid":               _MGL_PID,
        "c":                 _MGL_PROMOTER,
        "af_force_deeplink": "true",
    })

    base = urlunparse(p._replace(path=path, query="", fragment=""))
    params["deep_link_value"] = (
        f"{base}?utm_source=divulgador&utm_medium=magalu"
        f"&partner_id={_MGL_PARTNER}&promoter_id={_MGL_PROMOTER}"
    )
    return urlunparse(p._replace(
        path=path, query=urlencode(params), fragment=""))


async def _cuttly_tentar(url: str,
                          sessao: aiohttp.ClientSession) -> Optional[str]:
    """
    Tenta encurtar via Cuttly.
    429 → backoff de 60s (rate limit)
    401 → erro de chave API, para imediatamente
    Retorna None se falhar.
    """
    global _CUTTLY_LAST_429

    api = (f"https://cutt.ly/api/api.php"
           f"?key={_CUTTLY_KEY}&short={quote(url, safe='')}")

    for t in range(1, 4):
        # Verifica se ainda está em backoff de 429
        tempo_desde_429 = time.time() - _CUTTLY_LAST_429
        if tempo_desde_429 < _CUTTLY_BACKOFF:
            espera = _CUTTLY_BACKOFF - tempo_desde_429
            log_mgl.debug(f"  ⏳ Backoff 429: {espera:.0f}s restantes")
            await asyncio.sleep(espera)

        try:
            async with _SEM_HTTP:
                async with sessao.get(
                    api, timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    # 429: rate limit — backoff
                    if r.status == 429:
                        _CUTTLY_LAST_429 = time.time()
                        log_mgl.warning(
                            f"  ⚠️ Cuttly 429 (rate limit) t={t}/3 "
                            f"— backoff {_CUTTLY_BACKOFF}s")
                        await asyncio.sleep(_CUTTLY_BACKOFF)
                        continue

                    # 401: chave inválida — para imediatamente
                    if r.status == 401:
                        log_mgl.error(
                            f"  ❌ Cuttly 401 — chave API inválida "
                            f"| verifique CUTTLY_API_KEY")
                        return None

                    if r.status != 200:
                        log_mgl.warning(
                            f"  ⚠️ Cuttly HTTP {r.status} t={t}/3")
                        await asyncio.sleep(2 ** t)
                        continue

                    try:
                        data = await r.json(content_type=None)
                    except Exception as je:
                        log_mgl.warning(f"  ⚠️ JSON parse t={t}: {je}")
                        await asyncio.sleep(2 ** t)
                        continue

                    status = data.get("url", {}).get("status")
                    if status in (7, 2):
                        short = data["url"].get("shortLink")
                        if short:
                            log_mgl.info(f"  ✂️ Cuttly OK t={t}: {short}")
                            return short
                    log_mgl.warning(
                        f"  ⚠️ Cuttly status={status} t={t}/3")
                    await asyncio.sleep(2 ** t)

        except asyncio.TimeoutError:
            log_mgl.warning(f"  ⏱ Cuttly timeout t={t}/3")
            await asyncio.sleep(2 ** t)
        except Exception as e:
            log_mgl.error(f"  ❌ Cuttly t={t}: {e}")
            await asyncio.sleep(2 ** t)

    return None


async def _cuttly_background(url_longo: str, msg_id_origem: int):
    """
    Tenta encurtar em background.
    Quando conseguir, edita a mensagem no canal destino.
    """
    log_mgl.info(f"  🔄 Background encurtador | msg_orig={msg_id_origem}")

    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as sessao:
        for tentativa in range(15):      # até ~12 min
            await asyncio.sleep(45)
            try:
                short = await _cuttly_tentar(url_longo, sessao)
                if short:
                    loop  = asyncio.get_event_loop()
                    mapa  = await loop.run_in_executor(_EXECUTOR, ler_mapa)
                    id_dest = mapa.get(str(msg_id_origem))
                    if id_dest:
                        try:
                            msg_atual = await client.get_messages(
                                GRUPO_DESTINO, ids=id_dest)
                            if msg_atual and msg_atual.text:
                                novo = msg_atual.text.replace(
                                    url_longo, short)
                                if novo != msg_atual.text:
                                    await client.edit_message(
                                        GRUPO_DESTINO, id_dest,
                                        novo, parse_mode="md")
                                    log_mgl.info(
                                        f"  ✅ Editado com curto "
                                        f"| dest={id_dest}")
                        except Exception as e:
                            log_mgl.warning(f"  ⚠️ Edição bg: {e}")
                    # Salva no cache para próxima vez
                    db_set_link(url_longo, short, "magalu")
                    return
            except Exception as e:
                log_mgl.warning(f"  ⚠️ BG t={tentativa}: {e}")

    log_mgl.warning(f"  ❌ Background esgotou tentativas")


async def _agendar_edicao_magalu(url_longo: str, msg_id_origem: int):
    asyncio.create_task(_cuttly_background(url_longo, msg_id_origem))


async def motor_magalu(url: str,
                        sessao: aiohttp.ClientSession) -> Optional[str]:
    """Motor exclusivo Magalu. 429 tratado com backoff."""
    log_mgl.debug(f"▶ IN: {url[:80]}")

    # Cache
    cached = db_get_link(url)
    if cached:
        log_mgl.debug(f"💾 Cache: {cached[:60]}")
        return cached

    nl = _netloc(url)
    if "maga.lu" in nl or nl in _ENCURTADORES or classificar(url) == "expandir":
        try:
            async with _SEM_HTTP:
                url = await desencurtar(url, sessao)
            log_mgl.debug(f"  EXP: {url[:80]}")
        except Exception as e:
            log_mgl.error(f"  ❌ Desencurtar: {e}")
            return None

    if classificar(url) != "magalu":
        log_mgl.warning(f"  ⚠️ Não é Magalu: {url[:70]}")
        return None

    if not _validar_magalu(url):
        return None

    afiliado = _afiliar_magalu(url)
    log_mgl.debug(f"  AFL: {afiliado[:80]}")

    # Tenta encurtar
    short = await _cuttly_tentar(afiliado, sessao)

    if short:
        db_set_link(url, short, "magalu")
        log_mgl.info(f"  ✅ OUT (curto): {short}")
        return short

    # Cuttly falhou → retorna link longo afiliado para envio imediato
    log_mgl.warning(f"  ⚠️ Cuttly falhou — link longo, editará depois")
    return afiliado


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 10 ▸ EXTRAÇÃO DE LINKS
# ══════════════════════════════════════════════════════════════════════════════

_RE_URL = re.compile(
    r'https?://[^\s\)\]>,"\'<\u200b\u200c\u200d\u2060]+')

def extrair_links(texto: str) -> Tuple[List[str], List[str]]:
    brutos    = [u.strip().rstrip('.,;)>]}') for u in _RE_URL.findall(texto)]
    converter: List[str] = []
    preservar: List[str] = []
    vistos: set           = set()

    for url in brutos:
        if url in vistos:
            continue
        vistos.add(url)
        plat = classificar(url)
        if plat == "preservar":
            preservar.append(url)
        elif plat in ("amazon", "shopee", "magalu", "expandir"):
            converter.append(url)

    log_lnk.debug(f"🔗 {len(converter)} converter | {len(preservar)} preservar")
    return converter, preservar


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 11 ▸ PIPELINE DE CONVERSÃO — COM CACHE SQLite
# Antes de chamar a API, verifica o cache.
# Só chama API se o link não estiver no cache.
# ══════════════════════════════════════════════════════════════════════════════

async def _converter_um(url: str,
                         sessao: aiohttp.ClientSession
                         ) -> Tuple[Optional[str], Optional[str]]:
    plat = classificar(url)

    # Verifica cache SQLite antes de chamar qualquer API
    cached = db_get_link(url)
    if cached:
        log_lnk.debug(f"💾 Cache: [{plat}] {cached[:50]}")
        return cached, plat if plat in ("amazon","shopee","magalu") else "amazon"

    async def _rota_com_cache(u: str, motor, nome: str):
        r = await motor(u, sessao)
        if r:
            db_set_link(url, r, nome)
            return r, nome
        return None, None

    if plat == "amazon":
        return await _rota_com_cache(url, motor_amazon, "amazon")

    if plat == "shopee":
        return await _rota_com_cache(url, motor_shopee, "shopee")

    if plat == "magalu":
        return await _rota_com_cache(url, motor_magalu, "magalu")

    if plat == "expandir":
        try:
            async with _SEM_HTTP:
                exp = await desencurtar(url, sessao)
        except Exception:
            return None, None
        p2 = classificar(exp)
        if p2 in ("amazon", "shopee", "magalu"):
            return await _converter_um(exp, sessao)
        return None, None

    return None, None


async def converter_links(links: List[str]) -> Tuple[Dict[str, str], str]:
    if not links:
        return {}, "desconhecido"

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

    mapa: Dict[str, str] = {}
    plats: List[str]      = []
    for i, res in enumerate(resultados):
        if isinstance(res, Exception):
            log_lnk.error(f"❌ [{i}]: {res}")
            continue
        novo, plat = res
        if novo and plat:
            mapa[links[i]] = novo
            plats.append(plat)

    plat_p = max(set(plats), key=plats.count) if plats else (
    classificar(links[0]) if links else "desconhecido"
    )
    log_lnk.info(f"✅ {len(mapa)}/{len(links)} | plat={plat_p}")
    return mapa, plat_p

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 12 ▸ LIMPEZA DE RUÍDO TEXTUAL
# Corrige: "Leo Indica / Ofertas Insanas 🔥" aparecendo no topo
# Remove header do Telegram que vem colado na primeira linha
# ══════════════════════════════════════════════════════════════════════════════

_RE_INVISIVEIS = re.compile(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]')
_RE_GRUPO_EXT  = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)[^\s]*',
    re.I)
_RE_LIXO_STRUCT = re.compile(
    r'^\s*(?:-?\s*An[uú]ncio|Publicidade|:::+|---+|===+'
    r'|[-–—]\s*(?:ML|MG|AMZ)|(?:ML|MG|AMZ)\s*:)\s*$', re.I)
_RE_CTA = re.compile(
    r'^\s*(?:link\s+(?:do\s+)?produto|link\s+da\s+oferta|'
    r'resgate\s+aqui|clique\s+aqui|acesse\s+aqui|'
    r'compre\s+aqui|grupo\s+vip|entrar\s+no\s+grupo|'
    r'acessar\s+grupo|saiba\s+mais\s*:|confira\s+no\s+app)\s*:?\s*$', re.I)
_RE_REDES_BLOCO = re.compile(
    r'^\s*(?:redes\s+\w+|[-–]\s*grupo\s*(?:cupons?|promoções?|vip)?\s*:?\s*$'
    r'|[-–]\s*(?:chat|twitter|whatsapp|instagram|tiktok|youtube)\s*:?\s*$'
    r'|[-–]\s*link\s+(?:do\s+)?grupo\s*:?\s*$'
    r'|acesse\s+nossas\s+redes|nossas\s+redes\s+sociais)', re.I)
_RE_ROTULO_VAZIO = re.compile(r'^\s*[-–•]\s*\w[\w\s]{0,30}:\s*$')

# Header do Telegram: "Nome do Canal / Grupo 🔥" ou "Nome Bot:"
# Detecta padrões como "Leo Indica / Ofertas Insanas 🔥"
_RE_HEADER_CANAL = re.compile(
    r'^[A-ZÀ-Ú][^\n]{3,50}'   # começa com maiúscula
    r'(?:\s*/\s*[A-ZÀ-Ú][^\n]{2,40})?'  # opcional: " / Segundo Nome"
    r'[\s🔥💥⚡🚀🎯🛒]+$',  # termina com espaço ou emoji
    re.UNICODE)


def _eh_header_canal(linha: str) -> bool:
    """
    Detecta se a linha é o cabeçalho do canal/grupo do Telegram.
    Exemplos que devem ser removidos:
      "Leo Indica / Ofertas Insanas 🔥"
      "Promotom Ofertas"
      "NINJA OFERTAS 🔥"
    Exemplos que NÃO devem ser removidos:
      "🔥 Produto XYZ em oferta"  (começa com emoji)
      "Creme Crocante Kit Kat"    (título de produto)
    """
    l = linha.strip()
    if not l:
        return False

    # Se começa com emoji → é título de oferta, não header
    primeiro_char = l[0]
    if _tem_emoji(primeiro_char):
        return False

    # Header tem "/" separando nome do canal
    if re.match(r'^[A-ZÀ-Ú][\w\s]{2,30}\s*/\s*[\w\s]{2,30}', l):
        return True

    # Header em maiúsculas com emoji no final
    if re.match(r'^[A-ZÀÁÂÃÉÊÍÓÔÕÚ\s]{4,30}[\s🔥💥⚡🚀]+$', l, re.UNICODE):
        return True

    return False


def limpar_ruido_textual(texto: str) -> str:
    """
    Pipeline de limpeza em 5 camadas.
    Camada 0: Unicode invisíveis
    Camada 1: Header do canal (nova)
    Camada 2: CTAs e lixo estrutural
    Camada 3: Links de grupos externos
    Camada 4: Blocos de redes sociais
    Camada 5: Linhas vazias consecutivas
    """
    texto  = _RE_INVISIVEIS.sub(" ", texto)
    texto  = texto.replace("\r\n", "\n").replace("\r", "\n")
    linhas = texto.split("\n")

    saida:          List[str] = []
    vazio                     = False
    em_bloco_redes            = False
    primeira_linha_texto      = True   # controle para remover só o 1º header

    for linha in linhas:
        l = linha.strip()

        if not l:
            if not vazio:
                saida.append("")
            vazio = True
            em_bloco_redes = False
            continue
        vazio = False

        # Camada 1: Remove header do canal SOMENTE na primeira linha de texto
        if primeira_linha_texto:
            primeira_linha_texto = False
            if _eh_header_canal(l):
                log_fmt.debug(f"🗑 Header canal removido: {l[:50]}")
                continue

        # Camada 2: Blocos de redes sociais
        if _RE_REDES_BLOCO.match(l):
            em_bloco_redes = True
            continue

        if em_bloco_redes:
            if _RE_ROTULO_VAZIO.match(l) or not l:
                continue
            if not re.match(r'https?://', l):
                em_bloco_redes = False
            else:
                continue

        # Camada 3: CTAs de outros bots
        if _RE_CTA.match(l):
            continue

        # Camada 4: Lixo estrutural
        if _RE_LIXO_STRUCT.match(l):
            continue

        # Camada 5: Links de grupos externos
        if _RE_GRUPO_EXT.search(l):
            l = _RE_GRUPO_EXT.sub("", l).strip()
            if not l:
                continue

        saida.append(l)

    return "\n".join(saida).strip()


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 13 ▸ EMOJIS FIXOS + RADARES SEMÂNTICOS
# Rotação circular determinística.
# Multi-produto (≥2 itens com preço): usa 🔹 nos produtos, não emojis de dinheiro.
# Se a linha já tiver emoji → NÃO adiciona nenhum.
# ══════════════════════════════════════════════════════════════════════════════

_EMJ: Dict[str, List[str]] = {
    "titulo_oferta": ["🔥"],
    "titulo_cupom":  ["🚨"],
    "titulo_evento": ["⚠️"],
    "preco":         ["💵", "💰"],
    "cupom_cod":     ["🎟", "🏷"],
    "Resgate aqui":     ["✅"],
    "carrinho":      ["🛒"],
    "frete":         ["🚚", "📦"],
    "multi_item":    ["🔹"],   # fixo para itens em lista multi-produto
}
_EMJ_IDX: Dict[str, int] = {k: 0 for k in _EMJ}

def _prox_emoji(cat: str) -> str:
    lista         = _EMJ[cat]
    idx           = _EMJ_IDX[cat]
    emoji         = lista[idx % len(lista)]
    _EMJ_IDX[cat] = (idx + 1) % len(lista)
    return emoji

_KW_CUPOM    = re.compile(
    r'\b(?:cupom|cupon|c[oó]digo|coupon|off|resgate|cod)\b', re.I)
_KW_PRECO    = re.compile(r'R\$\s?[\d.,]+', re.I)
_KW_FRETE    = re.compile(
    r'\b(?:frete\s+gr[aá]t|entrega\s+gr[aá]t|sem\s+frete|frete\s+0)\b', re.I)
_KW_EVENTO   = re.compile(
    r'\b(?:quiz|roleta|miss[aã]o|arena|girar|gire|roda|jogar|jogue|desafio)\b',
    re.I)
_KW_STATUS   = re.compile(
    r'\b(?:voltando|voltou|normalizou|renovado|estoque\s+renovado|regularizou)\b',
    re.I)
_KW_RESGATE  = re.compile(
    r'\b(?:resgate|clique|acesse|ative|use\s+o\s+cupom)\b', re.I)
_KW_CARRINHO = re.compile(r'\b(?:carrinho|cart)\b', re.I)
_KW_COD      = re.compile(r'\b([A-Z][A-Z0-9_-]{3,19})\b')

_RE_EMOJI_CHECK = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF"
    r"\U0001F900-\U0001F9FF\u2B50\u2B55\u231A\u231B"
    r"\U0001F100-\U0001F1FF]")

def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI_CHECK.search(s))

def _contar_produtos(texto: str) -> int:
    """Conta linhas com preço — indica quantidade de produtos."""
    return sum(1 for l in texto.splitlines() if _KW_PRECO.search(l))

def _emoji_de_linha(linha: str, eh_titulo: bool,
                    is_multi: bool = False) -> Optional[str]:
    """
    Retorna emoji para a linha.
    Se is_multi=True e a linha tem preço → usa 🔹 (item de lista).
    Se a linha já tem emoji → retorna None (não duplica).
    """
    # Já tem emoji → não adiciona
    if _tem_emoji(linha):
        return None

    if eh_titulo:
        if _KW_EVENTO.search(linha): return _prox_emoji("titulo_evento")
        if _KW_CUPOM.search(linha):  return _prox_emoji("titulo_cupom")
        return _prox_emoji("titulo_oferta")

    # Multi-produto: linhas com preço recebem 🔹
    if is_multi and _KW_PRECO.search(linha):
        return "🔹"

    if _KW_FRETE.search(linha):    return _prox_emoji("frete")
    if _KW_CUPOM.search(linha):    return _prox_emoji("cupom_cod")
    if _KW_PRECO.search(linha):    return _prox_emoji("preco")
    if _KW_RESGATE.search(linha):  return _prox_emoji("resgate")
    if _KW_CARRINHO.search(linha): return _prox_emoji("carrinho")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 14 ▸ RENDERIZADOR
# - Não duplica emojis existentes
# - Multi-produto usa 🔹 nos itens
# - Remove nome do canal/grupo da primeira linha
# - Preserva espaçamento original
# - Clique-e-copie nos cupons
# ══════════════════════════════════════════════════════════════════════════════

_RE_LIXO_PREF  = re.compile(
    r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:|[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?)\s*',
    re.I)
_RE_ANUNCIO    = re.compile(
    r'^\s*[-#]?\s*(?:an[uú]ncio|publicidade|patrocinado)\s*$', re.I)
_RE_URL_RENDER = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c]+')

# Remove "Nome do Canal 🔥" ou "Nome do Bot:" da primeira linha de texto
# Detecta padrões: "Redes PromoTom", "PromoTom Ofertas", "Canal XYZ"
_RE_NOME_CANAL = re.compile(
    r'^\s*(?:redes\s+)?\w[\w\s]{2,30}'
    r'(?:\s+(?:ofertas?|promos?|cupons?|indica|bot|canal|grupo))?\s*[:\-]?\s*$',
    re.I)

_FALSO_CUPOM = frozenset({
    "FRETE","GRÁTIS","GRATIS","AMAZON","SHOPEE","MAGALU","LINK",
    "CLIQUE","ACESSE","CONFIRA","HOJE","AGORA","PROMO","OFF",
    "BLACK","SUPER","MEGA","ULTRA","VIP","NOVO","NOVA","NUM","PRECO","PCT",
    "PS5","PS4","XBOX","USB","ATX","RGB","LED","HD","SSD","RAM",
    "APP","BOT","API","URL","HTTP","HTTPS",
})

def _crases(linha: str) -> str:
    """Coloca crases em códigos de cupom para clique-e-copie no Telegram."""
    if "http" in linha or "`" in linha:
        return linha
    if not (_KW_CUPOM.search(linha) or _KW_COD.search(linha)):
        return linha

    def _sub(m: re.Match) -> str:
        c = m.group(0)
        return c if (c in _FALSO_CUPOM or len(c) < 4) else f"`{c}`"

    return re.sub(r'\b([A-Z][A-Z0-9_-]{4,20})\b', _sub, linha)


def renderizar(texto: str, mapa_links: Dict[str, str],
               links_preservar: List[str], plat: str) -> str:
    mapa     = {**mapa_links, **{u: u for u in links_preservar}}
    is_multi = _contar_produtos(texto) >= 2

    linhas   = texto.split("\n")
    saida: List[str] = []
    primeiro = True
    idx_linha = 0

    for linha in linhas:
        l = linha.strip()
        idx_linha += 1

        if not l:
            saida.append("")
            continue

        # Remove linha de anúncio explícito (preserva como texto simples)
        if _RE_ANUNCIO.match(l):
            saida.append(l)
            continue

        l = _RE_LIXO_PREF.sub("", l).strip()
        if not l:
            continue

        # ── Substitui URLs ────────────────────────────────────────────────
        urls_na_linha = _RE_URL_RENDER.findall(l)
        sem_urls      = _RE_URL_RENDER.sub("", l).strip()

        if urls_na_linha and not sem_urls:
            # Linha só de link
            for u in urls_na_linha:
                uc = u.rstrip('.,;)>')
                convertido = mapa.get(uc)
                if convertido:
                    saida.append(convertido)
            continue

        l = _RE_URL_RENDER.sub(
            lambda m: mapa.get(m.group(0).rstrip('.,;)>'), ""),
            l).strip()
        if not l:
            continue

        # ── Clique-e-copie ────────────────────────────────────────────────
        l = _crases(l)

        # ── Emoji ─────────────────────────────────────────────────────────
        # Regra 1: se já tem emoji, não adiciona nenhum
        if not _tem_emoji(l):
            emoji = _emoji_de_linha(l, eh_titulo=primeiro, is_multi=is_multi)
            if emoji:
                l = f"{emoji} {l}"

        if primeiro:
            primeiro = False

        saida.append(l)

    return "\n".join(saida).strip()


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 15 ▸ DEDUPLICAÇÃO SEMÂNTICA — CUPONS INVARIANTES À ORDEM
#
# Corrige: "CUPOM_A + CUPOM_B" e "CUPOM_B + CUPOM_A" → mesma oferta
# Corrige: variações de texto com mesmos cupons → bloqueadas
#
# CHAVE DA OFERTA:
#   plat + frozenset(cupons) + alma_normalizada
#   frozenset é invariante à ordem — não importa a sequência dos cupons
# ══════════════════════════════════════════════════════════════════════════════

_SIM_FORTE  = 0.85
_SIM_MEDIO  = 0.75
_SIM_EVENTO = 0.60

_RUIDO_NORM = frozenset({
    "promo","promocao","promoção","oferta","desconto","cupom","corre",
    "aproveita","urgente","gratis","grátis","frete","hoje","agora",
    "imperdivel","imperdível","exclusivo","limitado","corra","ative",
    "use","saiu","vazou","resgate","acesse","confira","link","clique",
    "app","relampago","relâmpago","click","veja","novo","nova",
    "valido","válido","somente","apenas","ate","até","partir",
    "ainda","volta","ativo","disponivel","disponível",
})

_RE_EMJ_STRIP = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF"
    r"\U0001F100-\U0001F1FF\u2B50\u2B55]+",
    flags=re.UNICODE)

_FALSO_CUPOM = frozenset({
    "FRETE","GRÁTIS","GRATIS","AMAZON","SHOPEE","MAGALU","LINK",
    "CLIQUE","ACESSE","CONFIRA","HOJE","AGORA","PROMO","OFF",
    "BLACK","SUPER","MEGA","ULTRA","VIP","NOVO","NOVA","NUM","PRECO","PCT",
    "PS5","PS4","XBOX","USB","ATX","RGB","LED","HD","SSD","RAM",
    "APP","BOT","API","URL","HTTP","HTTPS","OK","BR","EM","DE","DA","DO",
    "COM","SEM","POR","PARA","OFF",
})

def _rm_ac(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", t)
                   if unicodedata.category(c) != "Mn")

def _extrair_todos_cupons(texto: str) -> frozenset:
    """
    Extrai todos os cupons. frozenset garante que a ordem não importa.
    "CUPOM_A + CUPOM_B" == "CUPOM_B + CUPOM_A" no hash.
    """
    # Padrões de cupom: maiúsculas+números, 4-20 chars
    raw = frozenset(re.findall(r'\b([A-Z][A-Z0-9_-]{3,19})\b', texto))
    return raw - _FALSO_CUPOM

def _extrair_cupom(texto: str) -> str:
    cupons = _extrair_todos_cupons(texto)
    return next(iter(sorted(cupons)), "")

def _extrair_valor(texto: str) -> str:
    m = _KW_PRECO.search(texto)
    if not m: return ""
    return re.sub(r'[R$\s.,]', '', m.group(0))

def _extrair_asin(texto: str, mapa: dict) -> str:
    for url in list(mapa.values()) + re.findall(r'https?://\S+', texto):
        for pat in [r'/dp/([A-Z0-9]{10})', r'/gp/product/([A-Z0-9]{10})']:
            m = re.search(pat, url)
            if m: return m.group(1)
    return ""

def _extrair_id_magalu(texto: str, mapa: dict) -> str:
    for url in list(mapa.values()) + re.findall(r'https?://\S+', texto):
        m = re.search(r'/p/([a-z0-9]{6,})/?', url, re.I)
        if m: return m.group(1)
    return ""

def _extrair_sku(mapa: dict) -> str:
    asin = _extrair_asin("", mapa)
    if asin: return f"amz_{asin}"
    id_m = _extrair_id_magalu("", mapa)
    if id_m: return f"mgl_{id_m}"
    for url in mapa.values():
        m = re.search(r'/i\.(\d+\.\d+)', url)
        if m: return f"shp_{m.group(1)}"
    return ""

def _normalizar_alma(texto: str) -> str:
    """
    Extrai essência semântica.
    Invariante a: ordem, emojis, formatação, maiúsculas/minúsculas.
    """
    t = _rm_ac(texto.lower())
    t = re.sub(r'https?://\S+', ' ', t)
    t = _RE_EMJ_STRIP.sub(' ', t)
    t = re.sub(r'r\$\s*[\d.,]+', ' PRECO ', t)
    t = re.sub(r'\b\d+\s*%', ' PCT ', t)
    t = re.sub(r'\b\d+\b', ' NUM ', t)
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    # Ordena palavras-chave para invariância
    return ' '.join(sorted(
        w for w in t.split()
        if w not in _RUIDO_NORM and len(w) > 2))

def _normalizar_beneficios(texto: str) -> frozenset:
    """
    Extrai benefícios da oferta (OFF%, frete grátis, etc).
    Invariante à ordem e formatação.
    """
    beneficios = set()

    # Percentuais de desconto: "50% OFF", "OFF 50%"
    for m in re.finditer(r'(\d+)\s*%?\s*off|off\s*(\d+)\s*%?', texto, re.I):
        pct = m.group(1) or m.group(2)
        if pct: beneficios.add(f"off_{pct}pct")

    # Valores OFF: "R$50 OFF em R$250", "R$5 OFF em R$25"
    for m in re.finditer(
        r'r\$\s*(\d+)\s*off\s+em\s+r\$\s*(\d+)', texto, re.I
    ):
        beneficios.add(f"off_{m.group(1)}_em_{m.group(2)}")

    # Frete grátis
    if re.search(r'frete\s+gr[aá]t|entrega\s+gr[aá]t', texto, re.I):
        beneficios.add("frete_gratis")

    return frozenset(beneficios)

def _detectar_campanha(texto: str) -> str:
    tl     = texto.lower()
    tokens = []
    m = _KW_EVENTO.search(texto)
    if m: tokens.append(f"evt_{m.group(0).lower()}")
    if "amazon app"  in tl: tokens.append("amazon_app")
    if "mastercard"  in tl: tokens.append("mastercard")
    if "prime"       in tl: tokens.append("prime")
    if "magalu app"  in tl: tokens.append("magalu_app")
    if "whatsapp"    in tl: tokens.append("whatsapp")
    return "|".join(sorted(tokens)) if tokens else "geral"

def _eh_reativacao(texto: str) -> bool:
    return bool(re.search(
        r'\b(?:voltou|voltando|ativo\s+novamente|liberado\s+novamente|'
        r'normalizou|renovado|estoque\s+renovado|regularizou|'
        r'ainda\s+ativo|oferta\s+ativa|reativado|de\s+volta|'
        r'está\s+de\s+volta|voltei|saiu\s+novamente)\b',
        texto, re.I))

# ── Fingerprints — todos usam frozenset para invariância de ordem ─────────────

def _fp_amazon(asin: str) -> str:
    return hashlib.sha256(f"amz|{asin}".encode()).hexdigest()

def _fp_magalu(id_prod: str) -> str:
    return hashlib.sha256(f"mgl|{id_prod}".encode()).hexdigest()

def _fp_shopee_cupom(cupons: frozenset) -> str:
    """Hash só dos cupons — invariante à ordem."""
    return hashlib.sha256(
        f"shp_cup|{'|'.join(sorted(cupons))}".encode()).hexdigest()

def _fp_shopee_semantico(cupons: frozenset, alma: str,
                          beneficios: frozenset) -> str:
    """Hash semântico completo — invariante à ordem."""
    raw = (f"shp_sem|{'|'.join(sorted(cupons))}"
           f"|{alma}|{'|'.join(sorted(beneficios))}")
    return hashlib.sha256(raw.encode()).hexdigest()

def _fp_generico(plat: str, cupons: frozenset, alma: str) -> str:
    raw = f"{plat}|{'|'.join(sorted(cupons))}|{alma}"
    return hashlib.sha256(raw.encode()).hexdigest()


def deve_enviar(plat: str, cupom: str, texto: str,
                mapa_links: dict = None) -> bool:
    """
    Dedup invariante à ordem dos cupons.
    "CUPOM_A + CUPOM_B" == "CUPOM_B + CUPOM_A" → bloqueado.
    """
    mapa_links = mapa_links or {}

    # Reativação → sempre reenvia
    if _eh_reativacao(texto):
        log_dedup.info("✅ Reativação — reenvia")
        _registrar_dedupe(plat, cupom, texto, mapa_links)
        return True

    cupons    = _extrair_todos_cupons(texto)
    alma      = _normalizar_alma(texto)
    beneficios= _normalizar_beneficios(texto)
    campanha  = _detectar_campanha(texto)
    eh_evt    = bool(_KW_EVENTO.search(texto))
    asin      = _extrair_asin(texto, mapa_links)
    id_mgl    = _extrair_id_magalu(texto, mapa_links)

    # ── AMAZON: chave = ASIN ──────────────────────────────────────────────
    if plat == "amazon":
        if asin:
            fp = _fp_amazon(asin)
            entradas = db_buscar_dedupe_por_asin(asin, plat)
            rapidas  = [e for e in entradas
                        if time.time() - e["ts"] < JANELA_RAPIDA]
            if rapidas:
                log_dedup.info(
                    f"🔁 [AMZ] ASIN={asin} | {len(rapidas)}x/10min")
                return False
            if db_get_dedupe(fp):
                log_dedup.info(f"🔁 [AMZ-24H] ASIN={asin}")
                return False
        else:
            fp = _fp_generico(plat, cupons, alma)
            if db_get_dedupe(fp):
                log_dedup.info(f"🔁 [AMZ-ALMA]")
                return False

        _registrar_dedupe(plat, cupom, texto, mapa_links)
        log_dedup.debug(f"✅ Amazon | ASIN={asin or 'N/A'}")
        return True

    # ── MAGALU: chave = ID do produto ─────────────────────────────────────
    if plat == "magalu":
        if id_mgl:
            fp = _fp_magalu(id_mgl)
            entradas = db_buscar_dedupe_por_id(id_mgl, plat)
            rapidas  = [e for e in entradas
                        if time.time() - e["ts"] < JANELA_RAPIDA]
            if rapidas:
                log_dedup.info(
                    f"🔁 [MGL] ID={id_mgl} | {len(rapidas)}x/10min")
                return False
            if db_get_dedupe(fp):
                log_dedup.info(f"🔁 [MGL-24H] ID={id_mgl}")
                return False
        else:
            fp = _fp_generico(plat, cupons, alma)
            if db_get_dedupe(fp):
                log_dedup.info(f"🔁 [MGL-ALMA]")
                return False

        _registrar_dedupe(plat, cupom, texto, mapa_links)
        log_dedup.debug(f"✅ Magalu | ID={id_mgl or 'N/A'}")
        return True

    # ── SHOPEE: dedup semântico ultra ─────────────────────────────────────
    if plat == "shopee":
        if cupons:
            # Camada 1: mesmos cupons (qualquer ordem) → duplicado
            fp_cup = _fp_shopee_cupom(cupons)
            if db_get_dedupe(fp_cup):
                log_dedup.info(
                    f"🔁 [SHP-CUPOM] {sorted(cupons)} "
                    f"(ordem ignorada)")
                return False

        # Camada 2: mesmos benefícios + alma similar
        fp_sem = _fp_shopee_semantico(cupons, alma, beneficios)
        if db_get_dedupe(fp_sem):
            log_dedup.info(f"🔁 [SHP-SEM]")
            return False

        # Camada 3: janela rápida — alma similar
        rapidas = db_buscar_dedupe_janela_rapida(plat)
        for e in rapidas:
            cupons_c    = frozenset(e.get("cupons", []))
            # Verifica se há cupons em COMUM (não apenas iguais todos)
            cupom_comum = bool(cupons & cupons_c) if cupons else False
            alma_c      = e.get("alma", "")
            sim         = SequenceMatcher(
                None, alma, alma_c).ratio() if alma_c else 0.0

            # Cupons em comum + alma parecida
            if cupom_comum and sim >= _SIM_MEDIO:
                log_dedup.info(
                    f"🔁 [SHP-JANELA] "
                    f"cupons_comuns={cupons & cupons_c} "
                    f"sim={sim:.2f}")
                return False

            # Alma muito similar mesmo sem cupom
            if sim >= _SIM_FORTE:
                log_dedup.info(f"🔁 [SHP-SIM] sim={sim:.2f}")
                return False

            # Mesmos benefícios (OFF, frete) → mesma campanha
            if beneficios:
                benef_c = frozenset(e.get("benef", []))
                if beneficios == benef_c and sim >= 0.50:
                    log_dedup.info(
                        f"🔁 [SHP-BENEF] mesmos benefícios "
                        f"sim={sim:.2f}")
                    return False

            if eh_evt and cupom_comum:
                log_dedup.info(f"🔁 [SHP-EVT]")
                return False

        _registrar_dedupe(plat, cupom, texto, mapa_links)
        log_dedup.debug(f"✅ Shopee | cupons={sorted(cupons)}")
        return True

    # ── GENÉRICO ──────────────────────────────────────────────────────────
    fp = _fp_generico(plat, cupons, alma)
    if db_get_dedupe(fp):
        log_dedup.info(f"🔁 [GEN] {plat}")
        return False

    rapidas = db_buscar_dedupe_janela_rapida(plat)
    for e in rapidas:
        cupons_c    = frozenset(e.get("cupons", []))
        cupom_comum = bool(cupons & cupons_c) if cupons else False
        alma_c      = e.get("alma", "")
        sim         = SequenceMatcher(
            None, alma, alma_c).ratio() if alma_c else 0.0
        if cupom_comum and sim >= _SIM_MEDIO:
            log_dedup.info(f"🔁 [GEN-CUPOM] sim={sim:.2f}")
            return False
        if sim >= _SIM_FORTE:
            log_dedup.info(f"🔁 [GEN-SIM] sim={sim:.2f}")
            return False

    _registrar_dedupe(plat, cupom, texto, mapa_links)
    log_dedup.debug(f"✅ Genérico | {plat}")
    return True


def _registrar_dedupe(plat: str, cupom: str, texto: str,
                       mapa_links: dict = None):
    mapa_links = mapa_links or {}
    cupons     = _extrair_todos_cupons(texto)
    alma       = _normalizar_alma(texto)
    camp       = _detectar_campanha(texto)
    beneficios = _normalizar_beneficios(texto)
    asin       = _extrair_asin(texto, mapa_links)
    id_mgl     = _extrair_id_magalu(texto, mapa_links)

    if plat == "amazon" and asin:
        fp = _fp_amazon(asin)
    elif plat == "magalu" and id_mgl:
        fp = _fp_magalu(id_mgl)
    elif plat == "shopee":
        fp_cup = _fp_shopee_cupom(cupons)
        fp_sem = _fp_shopee_semantico(cupons, alma, beneficios)
        # Registra os dois fingerprints Shopee
        db_set_dedupe(fp_cup, plat, list(cupons), alma, camp,
                      asin, id_mgl, list(beneficios))
        db_set_dedupe(fp_sem, plat, list(cupons), alma, camp,
                      asin, id_mgl, list(beneficios))
        _atualizar_cache_json(fp_cup, plat, cupom, cupons, camp, alma)
        return
    else:
        fp = _fp_generico(plat, cupons, alma)

    db_set_dedupe(fp, plat, list(cupons), alma, camp,
                  asin, id_mgl, list(beneficios))
    _atualizar_cache_json(fp, plat, cupom, cupons, camp, alma)


def _atualizar_cache_json(fp: str, plat: str, cupom: str,
                           cupons: frozenset, camp: str, alma: str):
    """Mantém cache JSON para compatibilidade."""
    try:
        cache = ler_cache()
        cache[fp] = {
            "plat": plat, "cupom": cupom.upper(),
            "cupons": list(cupons), "camp": camp,
            "alma": alma, "ts": time.time(),
        }
        salvar_cache(cache)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 16 ▸ IMAGEM
# Corrige: SyntaxError 'await' outside async function
# Corrige: object str can't be used in 'await' expression
# preparar_imagem é SEMPRE async — nunca chame sem await
# buscar_imagem retorna Optional[str] — não é coroutine, é string simples
# ══════════════════════════════════════════════════════════════════════════════

async def preparar_imagem(fonte, eh_midia_telegram: bool) -> tuple:
    """
    SEMPRE async. Retorna (BytesIO | path_str | None, None).
    Telegram media  → client.download_media(fonte, file=BytesIO())
    URL http        → baixa bytes para BytesIO em memória
    Arquivo local   → retorna path direto (string)
    """
    if eh_midia_telegram:
        try:
            buf = io.BytesIO()
            await client.download_media(fonte, file=buf)
            buf.seek(0)
            buf.name = "imagem.jpg"
            log_img.debug(f"✅ Mídia TG baixada | {buf.getbuffer().nbytes}b")
            return buf, None
        except Exception as e:
            log_img.warning(f"⚠️ download_media: {e}")
            return None, None

    if isinstance(fonte, str):
        if fonte.startswith("http"):
            try:
                hdrs = {"User-Agent": random.choice(USER_AGENTS)}
                async with aiohttp.ClientSession(headers=hdrs) as s:
                    async with s.get(
                        fonte,
                        timeout=aiohttp.ClientTimeout(total=20),
                        allow_redirects=True,
                    ) as r:
                        if r.status == 200:
                            data     = await r.read()
                            buf      = io.BytesIO(data)
                            buf.name = "produto.jpg"
                            log_img.debug(
                                f"✅ URL baixada | {len(data)}b | {fonte[:60]}")
                            return buf, None
                        log_img.warning(
                            f"⚠️ HTTP {r.status} ao baixar: {fonte[:60]}")
            except Exception as e:
                log_img.warning(f"⚠️ Download URL: {e} | {fonte[:60]}")
            return None, None

        # Arquivo local
        if os.path.exists(fonte):
            return fonte, None
        log_img.warning(f"⚠️ Arquivo não existe: {fonte}")
        return None, None

    log_img.warning(f"⚠️ fonte inválida: type={type(fonte)}")
    return None, None


async def buscar_imagem(url: str) -> Optional[str]:
    """
    Busca URL da melhor imagem do produto.
    Retorna Optional[str] — NUNCA é coroutine dentro de coroutine.
    Chame assim: img_url = await buscar_imagem(url)
    Depois:      img, _ = await preparar_imagem(img_url, False)
    """
    if not url or not url.startswith("http"):
        return None

    hdrs = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "pt-BR,pt;q=0.9",
    }
    for t in range(1, 4):
        try:
            async with aiohttp.ClientSession(headers=hdrs) as s:
                async with s.get(
                    url, allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as r:
                    ct = r.headers.get("content-type", "")
                    if "image" in ct:
                        return str(r.url)

                    html = await r.text(errors="ignore")
                    soup = BeautifulSoup(html, "html.parser")

                    # og:image — tenta pegar resolução máxima
                    for attr in [
                        {"property": "og:image"},
                        {"property": "og:image:secure_url"},
                        {"name": "twitter:image"},
                    ]:
                        tag = soup.find("meta", attrs=attr)
                        if tag and tag.get("content", "").startswith("http"):
                            img_url = tag["content"]
                            # Remove params de resize
                            img_url = re.sub(
                                r'[?&](width|height|w|h|size|resize|'
                                r'fit|quality|q|maxwidth|maxheight)=[^&]+',
                                '', img_url).rstrip('?&')
                            log_img.info(f"✅ og:image t={t}: {img_url[:70]}")
                            return img_url

                    # JSON-LD schema.org
                    for scr in soup.find_all("script",
                                              type="application/ld+json"):
                        try:
                            data  = json.loads(scr.string or "")
                            items = data if isinstance(data, list) else [data]
                            for item in items:
                                img = item.get("image")
                                if isinstance(img, str) and img.startswith("http"):
                                    return img
                                if isinstance(img, list) and img:
                                    c = img[0]
                                    if isinstance(c, str): return c
                                    if isinstance(c, dict):
                                        u = c.get("url", "")
                                        if u.startswith("http"): return u
                        except Exception:
                            pass

                    # Maior <img> na página
                    melhor_src  = None
                    melhor_area = 0
                    for img_tag in soup.find_all("img", src=True):
                        src = img_tag.get("src", "")
                        if not src.startswith("http"):
                            continue
                        try:
                            w    = int(img_tag.get("width",  0))
                            h    = int(img_tag.get("height", 0))
                            area = w * h
                            if area > melhor_area:
                                melhor_area = area
                                melhor_src  = src
                        except (ValueError, TypeError):
                            if any(x in src.lower() for x in [
                                "product","produto","item","image",
                                "foto","zoom","large","xl","hd","original",
                            ]):
                                if not melhor_src:
                                    melhor_src = src
                    if melhor_src:
                        log_img.info(f"✅ img tag t={t}: {melhor_src[:70]}")
                        return melhor_src

        except asyncio.TimeoutError:
            log_img.warning(f"⏱ Timeout t={t}/3: {url[:60]}")
        except Exception as e:
            log_img.warning(f"⚠️ buscar t={t}/3: {e} | {url[:50]}")

        if t < 3:
            await asyncio.sleep(1.0)

    log_img.warning(f"❌ Sem imagem: {url[:60]}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 17 ▸ RATE-LIMIT ADAPTATIVO
# Horário comercial (8h–22h): 0.5s
# Madrugada (22h–8h): 1.2s
# Nunca bloqueia mais que o necessário.
# ══════════════════════════════════════════════════════════════════════════════

_RATE_LOCK     = asyncio.Lock()
_ULTIMO_ENV_TS = 0.0

def _intervalo_atual() -> float:
    hora = int(time.strftime("%H"))
    return 0.5 if 8 <= hora < 22 else 1.2

async def _rate_limit():
    global _ULTIMO_ENV_TS
    async with _RATE_LOCK:
        agora    = time.monotonic()
        intervalo = _intervalo_atual()
        espera   = intervalo - (agora - _ULTIMO_ENV_TS)
        if espera > 0:
            await asyncio.sleep(espera)
        _ULTIMO_ENV_TS = time.monotonic()


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

async def _foi_processado(msg_id: int) -> bool:
    async with _IDS_LOCK:
        return msg_id in _IDS_PROC

# =========================
# MÓDULO — CAPTURA DE IMAGEM + PREPARO
# =========================

from io import BytesIO
import re
import requests
from PIL import Image


def extrair_primeiro_link(texto):
    """
    Extrai o primeiro link encontrado no texto.
    """
    if not texto:
        return None

    match = re.search(r'https?://\S+', texto)
    return match.group(0) if match else None


def baixar_imagem_do_link(url, timeout=10):
    """
    Tenta baixar imagem diretamente do link.
    """
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").lower()

        if "image" in content_type:
            return resp.content

        return None

    except Exception as e:
        print(f"[IMG LINK] Erro ao baixar imagem do link: {e}")
        return None


def preparar_imagem(img_bytes, max_size=(1080, 1080), qualidade=90):
    """
    Prepara imagem para envio:
    - converte para RGB
    - redimensiona
    - comprime
    """
    try:
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
        img.thumbnail(max_size)

        output = BytesIO()
        img.save(output, format="JPEG", quality=qualidade, optimize=True)
        output.seek(0)

        return output.getvalue()

    except Exception as e:
        print(f"[PREPARAR IMG] Erro: {e}")
        return img_bytes


async def obter_imagem(message, client):
    """
    Tenta obter imagem da mensagem.
    Ordem:
    1. Mídia da mensagem
    2. Link dentro do texto
    """

    # 1) tenta mídia do Telegram
    try:
        if message.photo or message.document:
            img_bytes = await client.download_media(message, file=bytes)

            if img_bytes:
                print("[IMG] Capturada da mensagem")
                return preparar_imagem(img_bytes)

    except Exception as e:
        print(f"[IMG MSG] Erro ao baixar mídia: {e}")

    # 2) tenta pelo link
    try:
        texto = message.message or ""
        link = extrair_primeiro_link(texto)

        if link:
            img_bytes = baixar_imagem_do_link(link)

            if img_bytes:
                print("[IMG] Capturada do link")
                return preparar_imagem(img_bytes)

    except Exception as e:
        print(f"[IMG FALLBACK] Erro no fallback: {e}")

    print("[IMG] Nenhuma imagem encontrada")
    return None

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 19 ▸ ENVIO LIMPO
# Corrige: BLOQUEIO Amazon sem tag quebrando ofertas legítimas
# A validação de tag agora é só WARNING, não bloqueia
# Nunca usa forward — sempre send_message / send_file
# ══════════════════════════════════════════════════════════════════════════════

def _tem_midia(media) -> bool:
    return media is not None and not isinstance(media, MessageMediaWebPage)

def _eh_cupom_texto(texto: str) -> bool:
    return bool(_KW_CUPOM.search(texto))

async def _enviar(msg: str, img_obj) -> object:
    """
    Envio limpo. Nunca forward. Sempre constrói do zero.
    Tenta send_file+caption → send_file sem caption + texto → só texto.
    """
    # Log de aviso se Amazon sem tag (não bloqueia mais — motor já garante)
    if "amazon.com.br" in msg and f"tag={_AMZ_TAG}" not in msg:
        log_amz.warning(
            f"⚠️ Mensagem Amazon enviada sem tag confirmada no texto "
            f"(pode estar no link encurtado)")

    if img_obj:
        if len(msg) <= 1024:
            try:
                return await client.send_file(
                    GRUPO_DESTINO, img_obj,
                    caption=msg, parse_mode="md",
                    force_document=False)
            except Exception as e:
                log_tg.warning(f"⚠️ send_file+caption: {e}")
                try:
                    await client.send_file(
                        GRUPO_DESTINO, img_obj,
                        force_document=False)
                    return await client.send_message(
                        GRUPO_DESTINO, msg,
                        parse_mode="md", link_preview=True)
                except Exception as e2:
                    log_tg.warning(f"⚠️ send_file sem caption: {e2}")
        else:
            try:
                await client.send_file(
                    GRUPO_DESTINO, img_obj,
                    force_document=False)
                return await client.send_message(
                    GRUPO_DESTINO, msg,
                    parse_mode="md", link_preview=False)
            except Exception as e:
                log_tg.warning(f"⚠️ send_file longo: {e}")

    # Fallback texto puro
    return await client.send_message(
        GRUPO_DESTINO, msg,
        parse_mode="md", link_preview=True)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 20 ▸ BANCO CENTRAL — SQLite
# Adiciona coluna benef (benefícios da oferta) na dedupe_temp
# ══════════════════════════════════════════════════════════════════════════════

def _init_db():
    global _db_conn
    _db_conn = sqlite3.connect(
        _DB_PATH, check_same_thread=False, timeout=10, isolation_level=None)
    _db_conn.execute("PRAGMA journal_mode=WAL")
    _db_conn.execute("PRAGMA synchronous=NORMAL")
    _db_conn.execute("PRAGMA cache_size=-16000")
    _db_conn.execute("PRAGMA temp_store=MEMORY")
    _db_conn.executescript("""
        CREATE TABLE IF NOT EXISTS links_cache (
            url_orig  TEXT PRIMARY KEY,
            url_conv  TEXT NOT NULL,
            plat      TEXT NOT NULL,
            ts        REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dedupe_temp (
            fp        TEXT PRIMARY KEY,
            plat      TEXT NOT NULL,
            cupons    TEXT,
            alma      TEXT,
            camp      TEXT,
            asin      TEXT,
            id_prod   TEXT,
            benef     TEXT,
            ts        REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS saturacao (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            plat  TEXT NOT NULL,
            sku   TEXT,
            ts    REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS scheduler (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            plat  TEXT NOT NULL,
            hora  INTEGER NOT NULL,
            score REAL DEFAULT 1.0,
            ts    REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_lc_plat  ON links_cache(plat);
        CREATE INDEX IF NOT EXISTS idx_dt_plat  ON dedupe_temp(plat, ts);
        CREATE INDEX IF NOT EXISTS idx_dt_asin  ON dedupe_temp(asin);
        CREATE INDEX IF NOT EXISTS idx_dt_id    ON dedupe_temp(id_prod);
        CREATE INDEX IF NOT EXISTS idx_sat      ON saturacao(plat, ts);
        CREATE INDEX IF NOT EXISTS idx_sch      ON scheduler(plat, hora);
    """)

    # Migração: adiciona coluna benef se não existir (banco antigo)
    try:
        _db_conn.execute("ALTER TABLE dedupe_temp ADD COLUMN benef TEXT")
        log_sys.info("🗄 Migração: coluna benef adicionada")
    except sqlite3.OperationalError:
        pass  # coluna já existe

    log_sys.info(f"🗄 DB ON | {_DB_PATH}")


def db_set_dedupe(fp: str, plat: str, cupons: list, alma: str,
                   camp: str, asin: str = "", id_prod: str = "",
                   benef: list = None):
    """Salva entrada de dedup com benefícios."""
    with _db() as db:
        db.execute(
            "INSERT OR REPLACE INTO dedupe_temp "
            "(fp,plat,cupons,alma,camp,asin,id_prod,benef,ts) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (fp, plat,
             json.dumps(cupons),
             alma, camp, asin, id_prod,
             json.dumps(benef or []),
             time.time()))


def db_get_dedupe(fp: str) -> Optional[dict]:
    limite = time.time() - TTL_DEDUPE
    with _db() as db:
        row = db.execute(
            "SELECT plat,cupons,alma,camp,asin,id_prod,benef,ts "
            "FROM dedupe_temp WHERE fp=? AND ts>=?",
            (fp, limite)
        ).fetchone()
    if row:
        return {
            "plat": row[0], "cupons": json.loads(row[1] or "[]"),
            "alma": row[2],  "camp":  row[3],
            "asin": row[4],  "id_prod": row[5],
            "benef": json.loads(row[6] or "[]"),
            "ts": row[7],
        }
    return None


def db_buscar_dedupe_janela_rapida(plat: str) -> list:
    limite = time.time() - JANELA_RAPIDA
    with _db() as db:
        rows = db.execute(
            "SELECT fp,cupons,alma,asin,id_prod,benef,ts "
            "FROM dedupe_temp "
            "WHERE plat=? AND ts>=? ORDER BY ts DESC",
            (plat, limite)
        ).fetchall()
    return [{
        "fp": r[0], "cupons": json.loads(r[1] or "[]"),
        "alma": r[2], "asin": r[3], "id_prod": r[4],
        "benef": json.loads(r[5] or "[]"), "ts": r[6],
    } for r in rows]

def db_get_link(url_orig: str):
    try:
        with _db() as db:
            row = db.execute(
                "SELECT url_conv FROM links_cache WHERE url_orig = ?",
                (url_orig,)
            ).fetchone()

            if row:
                return row[0]
    except Exception as e:
        log_links.error(f"[DB GET ERRO] {e}", exc_info=True)

    return None


def db_set_link(url_orig: str, url_conv: str, plat: str):
    try:
        with _db() as db:
            db.execute("""
                INSERT OR REPLACE INTO links_cache
                (url_orig, url_conv, plat, ts)
                VALUES (?, ?, ?, ?)
            """, (url_orig, url_conv, plat, time.time()))
    except Exception as e:
        log_links.error(f"[DB SET ERRO] {e}", exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 21 ▸ PARSER ULTRA ROBUSTO
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ParsedLink:
    url_original: str
    url_limpa:    str
    plat:         str
    tipo:         str
    sku:          str
    valido:       bool
    motivo:       str

_TRACKING = frozenset({
    "tag","ref","ref_","smid","sprefix","sr","spla","dchild","linkcode",
    "linkid","camp","creative","pf_rd_p","pf_rd_r","pd_rd_wg","pd_rd_w",
    "content-id","pd_rd_r","pd_rd_i","ie","qid","_encoding","dib",
    "dib_tag","m","th","psc","ingress","visitid","s","ascsubtag","btn_ref",
    "af_siteid","pid","af_click_lookback","is_retargeting","deep_link_value",
    "af_dp","utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "partner_id","promoter_id","af_force_deeplink","c","isretargeting",
    "fbclid","gclid","msclkid","mc_eid","_ga","_gl",
})

_P_AMZ = [
    re.compile(r'/dp/([A-Z0-9]{10})'),
    re.compile(r'/gp/product/([A-Z0-9]{10})'),
    re.compile(r'asin=([A-Z0-9]{10})', re.I),
]
_P_SHP = [
    re.compile(r'/product/(\d+)/(\d+)'),
    re.compile(r'/i\.(\d+)\.(\d+)'),
]
_P_MGL = re.compile(r'/(?:[^/]+/)?p/([a-z0-9]{6,})/?', re.I)

def _sem_tracking(p) -> str:
    params = {k: v[0] for k, v in parse_qs(p.query, keep_blank_values=False).items()
              if k.lower() not in _TRACKING}
    return urlunparse(p._replace(
        query=urlencode(params) if params else "", fragment=""))

def parse_link(url: str) -> ParsedLink:
    url = url.strip().rstrip('.,;)>').replace(' ', '%20')
    url = re.sub(r'%25([0-9A-Fa-f]{2})', r'%\1', url)

    if not url.startswith(("http://", "https://")):
        return ParsedLink(url, url, "desconhecido", "desconhecido",
                          "", False, "sem esquema")
    try:
        p = urlparse(url)
    except Exception as e:
        return ParsedLink(url, url, "desconhecido", "desconhecido",
                          "", False, str(e))

    nl   = p.netloc.lower().replace("www.", "")
    plat = classificar(url)

    if plat in (None, "preservar"):
        return ParsedLink(url, url, plat or "desconhecido", "desconhecido",
                          "", plat == "preservar", "")

    url_limpa = _sem_tracking(p)

    if plat == "expandir":
        return ParsedLink(url, url_limpa, "expandir", "encurtado", "", True, "")

    if plat == "amazon":
        sku = ""
        for pat in _P_AMZ:
            m = pat.search(p.path + "?" + p.query)
            if m: sku = m.group(1); break
        tipo = ("produto" if sku else
                "busca"   if re.search(r'/s[/?]|/deals|/b[/?]', p.path) else
                "evento"  if re.search(r'/events/|/stores/', p.path) else
                "desconhecido")
        return ParsedLink(url, url_limpa, plat, tipo, sku, True, "")

    if plat == "shopee":
        sku = ""
        for pat in _P_SHP:
            m = pat.search(p.path + "?" + p.query)
            if m: sku = f"{m.group(1)}.{m.group(2)}"; break
        tipo = "produto" if sku else "busca"
        return ParsedLink(url, url_limpa, plat, tipo, sku, True, "")

    if plat == "magalu":
        sku  = ""
        m    = _P_MGL.search(p.path)
        if m: sku = m.group(1)
        if "sacola" in nl and not p.path.strip("/"):
            return ParsedLink(url, url_limpa, plat, "invalido",
                              sku, False, "sacola sem produto")
        tipo = ("produto"  if sku else
                "lista"    if "/l/" in p.path else
                "selecao"  if "/selecao/" in p.path else
                "desconhecido")
        return ParsedLink(url, url_limpa, plat, tipo, sku, True, "")

    return ParsedLink(url, url_limpa, "desconhecido", "desconhecido",
                      "", False, "plat não mapeada")

def parse_links_bulk(urls: List[str]) -> List[ParsedLink]:
    res     = [parse_link(u) for u in urls]
    validos = [r for r in res if r.valido]
    log_lnk.info(f"🔍 Parser {len(validos)}/{len(urls)} válidos")
    return validos


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 22 ▸ SCHEDULER INTELIGENTE
# ══════════════════════════════════════════════════════════════════════════════

_SCH_LIM_H      = 15
_SCH_NOTURNO    = (0, 7)
_SCH_DELAY_MAX  = 15.0
_SCH_DELAY_NOT  = 10.0

_sch_cnt: Dict[str, List[float]] = {}
_sch_cnt_lock = asyncio.Lock()

async def _sch_add(plat: str):
    async with _sch_cnt_lock:
        agora = time.monotonic()
        fila  = _sch_cnt.setdefault(plat, [])
        fila.append(agora)
        _sch_cnt[plat] = [t for t in fila if agora - t < 3600]

async def _sch_count(plat: str) -> int:
    async with _sch_cnt_lock:
        agora = time.monotonic()
        return sum(1 for t in _sch_cnt.get(plat, []) if agora - t < 3600)

def _sch_delay(score: float, hora: int) -> float:
    h0, h1     = _SCH_NOTURNO
    base       = _SCH_DELAY_NOT if h0 <= hora < h1 else 0.0
    if score >= 1.0: return base
    if score >= 0.5: return min(base + (1.0 - score) * 10.0, _SCH_DELAY_MAX)
    return min(base + (0.5 - score) * 20.0, _SCH_DELAY_MAX)

async def scheduler_gate(plat: str, texto: str) -> float:
    if _KW_EVENTO.search(texto): return 0.0
    hora  = int(time.strftime("%H"))
    count = await _sch_count(plat)
    if count >= _SCH_LIM_H:
        log_sys.warning(f"⚠️ Scheduler limite/h | {plat}")
        return -1.0
    score = db_score_hora(plat, hora)
    return _sch_delay(score, hora)

async def scheduler_ok(plat: str):
    hora = int(time.strftime("%H"))
    await _sch_add(plat)
    db_registrar_sch(plat, hora)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 23 ▸ ANTI-SATURAÇÃO
# S1 — Mesma plataforma muito densa (delay)
# S2 — Burst de envios rápidos (delay)
# ══════════════════════════════════════════════════════════════════════════════

_SAT_MAX_PLAT   = 8
_SAT_BURST_LIM  = 5
_SAT_BURST_JAN  = 60

_burst: List[float] = []
_burst_lock = asyncio.Lock()

async def _burst_add():
    async with _burst_lock:
        agora = time.monotonic()
        _burst.append(agora)
        while _burst and agora - _burst[0] > _SAT_BURST_JAN:
            _burst.pop(0)

async def _burst_count() -> int:
    async with _burst_lock:
        agora = time.monotonic()
        return sum(1 for t in _burst if agora - t <= _SAT_BURST_JAN)

async def antisaturacao_gate(plat: str, texto: str) -> float:
    """Retorna delay extra em segundos. Nunca bloqueia."""
    if _KW_EVENTO.search(texto):
        return 0.0
    delay = 0.0
    if db_count_sat(plat) >= _SAT_MAX_PLAT:
        delay += 8.0
    if await _burst_count() >= _SAT_BURST_LIM:
        delay += 5.0
    return delay

def antisaturacao_ok(plat: str, sku: str):
    db_registrar_sat(plat, sku)

async def processar(event, is_edit=False):
    try:
        msg = event.message
        texto = msg.message or ""
        msg_id = msg.id

        # evita loop duplicado
        if msg_id in _IDS_PROC:
            return
        _IDS_PROC.add(msg_id)

        uname = getattr(event.chat, "username", "origem")

        # normalização básica
        tc = texto.lower()

        # ── DETECÇÃO DE PLATAFORMA (ajusta se já tiver pronto) ──
        if "shopee" in tc:
            plat = "shopee"
        elif "amazon" in tc:
            plat = "amazon"
        elif "magalu" in tc or "magazineluiza" in tc:
            plat = "magalu"
        else:
            return  # ignora se não for relevante

        # ── EXTRAÇÃO DE LINKS (simples, usa o seu depois se já tiver) ──
        urls = re.findall(r'https?://\S+', texto)
        if not urls:
            return

        mapa = {u: u for u in urls}  # aqui depois entra sua conversão real
        sku = None  # se você usa SKU, mantém seu sistema

        msg_final = texto  # aqui entra seu formatador depois

        # 🔥 CHAMA O PIPELINE (AGORA EXISTE)
        await _pipeline(
            event,
            plat,
            tc,
            mapa,
            sku,
            is_edit,
            msg_id,
            msg_final,
            uname,
            GRUPO_DESTINO
        )

    except Exception as e:
        log_sys.error(f"❌ ERRO processar: {e}", exc_info=True)

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 24 ▸ BUFFER ORCHESTRATOR + PIPELINE
# Corrige: name 'processar' is not defined
# Corrige: name '_iniciar_orchestrator' is not defined
# Corrige: name '_buf' is not defined no health check
# Todas as variáveis declaradas no escopo global ANTES dos handlers
# ══════════════════════════════════════════════════════════════════════════════

import heapq

# ── Variáveis globais do orchestrator — DEVEM estar antes de qualquer uso ────
_WORKERS_MAX: int              = 4
_FILA_MAX:    int              = 200
_COALESCE_MS: int              = 800

_buf:          list            = []        # heap de prioridade
_buf_lck:      asyncio.Lock    = None      # inicializado em _init_globals()
_buf_evt:      asyncio.Event   = None      # inicializado em _init_globals()
_w_ativos:     int             = 0
_w_lck:        asyncio.Lock    = None      # inicializado em _init_globals()
_coal:         dict            = {}        # {fp_rapido: ts}

# Inicializa locks — chamado no início de _run() antes de qualquer task
def _init_globals():
    global _buf_lck, _buf_evt, _w_lck
    _buf_lck = asyncio.Lock()
    _buf_evt = asyncio.Event()
    _w_lck   = asyncio.Lock()

_RE_TITULO_GEN = re.compile(
    r'^\s*(?:cupons?\s+(?:shopee|amazon|magalu)|novos?\s+cupons?|'
    r'links?\s+de\s+cupom)\s*$',
    re.I | re.M)


def _prio(texto: str) -> int:
    tl = texto.lower()
    if "amazon" in tl: return 1
    if "shopee" in tl: return 2
    if "magalu" in tl: return 3
    return 9

def _fp_r(texto: str) -> str:
    return hashlib.sha256(
        re.sub(r'\s+', '', texto.lower())[:80].encode()
    ).hexdigest()[:12]

def _tem_contexto(texto: str) -> bool:
    linhas = [l.strip() for l in texto.splitlines()
              if l.strip() and not re.match(r'https?://', l.strip())]
    if not linhas:
        return False
    total = " ".join(linhas)
    for ind in [
        r'off', r'%', r'r\$', r'cupom', r'desconto',
        r'promoção', r'oferta', r'grátis', r'evento',
        r'live', r'relâmpago', r'flash', r'volta',
        r'normalizou', r'a\s+partir', r'ativo',
    ]:
        if re.search(ind, total, re.I):
            return True
    return len(total) > 20


async def _enfileirar(event, is_edit: bool):
    """Coloca evento na fila de prioridade com coalescência."""
    texto = event.message.text or ""
    if not texto.strip():
        return

    fp    = _fp_r(texto)
    agora = time.monotonic()

    async with _buf_lck:
        if not is_edit and agora - _coal.get(fp, 0.0) < _COALESCE_MS / 1000:
            log_sys.debug(f"⚡ Coalesce descartado fp={fp}")
            return
        _coal[fp] = agora

        if len(_buf) >= _FILA_MAX:
            log_sys.warning(
                f"⚠️ Fila cheia ({_FILA_MAX}) — descartando "
                f"id={event.message.id}")
            return

        prio = 0 if is_edit else _prio(texto)
        heapq.heappush(_buf, (prio, agora, event, is_edit))
        log_sys.debug(
            f"📥 Enfileirado | prio={prio} fila={len(_buf)} "
            f"id={event.message.id}")

    _buf_evt.set()


async def _worker_loop():
    """Worker que consome a fila. Controla workers ativos."""
    global _w_ativos
    while True:
        await _buf_evt.wait()
        while True:
            item = None
            async with _buf_lck:
                if _buf:
                    item = heapq.heappop(_buf)
                else:
                    _buf_evt.clear()
                    break

            if item is None:
                break

            prio, ts, event, is_edit = item

            async with _w_lck:
                if _w_ativos >= _WORKERS_MAX:
                    async with _buf_lck:
                        heapq.heappush(_buf, item)
                        _buf_evt.set()
                    await asyncio.sleep(0.2)
                    break
                _w_ativos += 1

            try:
                if time.monotonic() - ts > 60:
                    log_sys.warning(
                        f"⏱ Item expirado (>60s) — descartando "
                        f"id={event.message.id}")
                    continue
                await _pipeline(event, is_edit)
            except Exception as e:
                log_sys.error(f"❌ Worker erro: {e}", exc_info=True)
            finally:
                async with _w_lck:
                    _w_ativos -= 1


async def _pipeline(event, is_edit: bool = False):
    """
    Pipeline principal. Chamado pelo worker.
    Trata todos os erros internamente — nunca deixa o worker crashar.
    """
    msg_id = event.message.id
    texto  = event.message.text or ""

    try:
        chat  = await event.get_chat()
        uname = (chat.username or str(event.chat_id)).lower()
    except Exception as e:
        log_sys.error(f"❌ get_chat: {e}")
        return

    log_tg.info(
        f"{'✏️' if is_edit else '📩'} @{uname} | id={msg_id} | "
        f"{len(texto)}c | fila={len(_buf)} w={_w_ativos}")

    if not texto.strip():
        return

    # Anti-loop
    try:
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
    except Exception as e:
        log_sys.error(f"❌ Anti-loop check: {e}")
        return

    # Filtro de texto
    try:
        if texto_bloqueado(texto):
            return
    except Exception as e:
        log_sys.error(f"❌ texto_bloqueado: {e}")
        return

    # Limpeza
    try:
        tc = limpar_ruido_textual(texto)
    except Exception as e:
        log_sys.error(f"❌ limpar_ruido: {e}")
        tc = texto

    if not _tem_contexto(tc):
        log_fil.debug(f"🗑 Sem contexto | @{uname}")
        return

    # Extração + parser
    try:
        links_c, links_p = extrair_links(tc)
        parsed            = parse_links_bulk(links_c)
        diretos           = [r.url_limpa for r in parsed
                             if r.plat not in ("expandir", "desconhecido")]
        expandir_lst      = [r.url_limpa for r in parsed
                             if r.plat == "expandir"]
    except Exception as e:
        log_sys.error(f"❌ Extração links: {e}")
        return

    if not diretos and not expandir_lst and not links_p:
        if "fadadoscupons" not in uname:
            log_sys.debug(f"⏩ Sem links | @{uname}")
            return

    # Conversão paralela
    try:
        mapa, plat = await converter_links(diretos + expandir_lst)
    except Exception as e:
        log_sys.error(f"❌ converter_links: {e}")
        mapa, plat = {}, "amazon"

    if links_c and not mapa and not links_p:
        log_sys.warning(f"🚫 Zero links convertidos | @{uname}")
        return

    # SKU e identificadores
    try:
        sku = next(
            (f"{r.plat[:3]}_{r.sku}" for r in parsed if r.sku), ""
        ) or _extrair_sku(mapa)
        cup = _extrair_cupom(tc)
    except Exception as e:
        log_sys.error(f"❌ SKU/cupom: {e}")
        sku, cup = "", ""

    # Anti-saturação + dedup + scheduler
    if not is_edit:
        try:
            delay_sat = await antisaturacao_gate(plat, tc)
        except Exception as e:
            log_sys.error(f"❌ antisaturacao_gate: {e}")
            delay_sat = 0.0

        try:
            if not deve_enviar(plat, cup, tc, mapa):
                return
        except Exception as e:
            log_sys.error(f"❌ deve_enviar: {e}")
            return

        try:
            delay_sch = await scheduler_gate(plat, tc)
            if delay_sch == -1.0:
                log_sys.warning("⚠️ Limite/h atingido — descartando")
                return
        except Exception as e:
            log_sys.error(f"❌ scheduler_gate: {e}")
            delay_sch = 0.0

        total_delay = delay_sch + delay_sat
        if total_delay > 0:
            log_sys.debug(f"⏱ Delay {total_delay:.1f}s | {plat}")
            await asyncio.sleep(total_delay)

    # Renderização
    try:
        msg_final = renderizar(tc, mapa, links_p, plat)
    except Exception as e:
        log_sys.error(f"❌ renderizar: {e}")
        return

    # ── Imagem ────────────────────────────────────────────────────────────
    media_orig = event.message.media
    tem_img    = _tem_midia(media_orig)
    img        = None
    eh_cup     = _eh_cupom_texto(tc)

    # 1. Imagem original da mensagem
    if tem_img:
        try:
            img, _ = await preparar_imagem(media_orig, True)
            if img:
                log_img.debug("✅ Imagem mensagem original")
        except Exception as e:
            log_img.warning(f"⚠️ Imagem original: {e}")
            img = None

    # 2. Sem imagem → fallback ou busca
    if img is None:
        if eh_cup:
            if plat == "shopee" and os.path.exists(_IMG_SHP):
                img = _IMG_SHP
            elif plat == "amazon" and os.path.exists(_IMG_AMZ):
                img = _IMG_AMZ
            elif plat == "magalu" and os.path.exists(_IMG_MGL):
                img = _IMG_MGL
        elif mapa:
            try:
                img_url = await buscar_imagem(list(mapa.values())[0])
                if img_url:
                    img, _ = await preparar_imagem(img_url, False)
            except Exception as e:
                log_img.warning(f"⚠️ busca imagem produto: {e}")
                img = None

    # Rate-limit
    await _rate_limit()

    # ── Envio / Edição ────────────────────────────────────────────────────
    async with _SEM_ENVIO:
        loop = asyncio.get_event_loop()
        try:
            mp = await loop.run_in_executor(_EXECUTOR, ler_mapa)
        except Exception as e:
            log_sys.error(f"❌ ler_mapa: {e}")
            mp = {}

        try:
            if is_edit:
                id_d = mp.get(str(msg_id))
                if not id_d:
                    log_sys.debug(f"⏩ Edit sem destino: {msg_id}")
                    return
                for t in range(1, 4):
                    try:
                        await client.edit_message(
                            GRUPO_DESTINO, id_d, msg_final,
                            parse_mode="md")
                        log_tg.info(f"✏️ Editado | dest={id_d}")
                        break
                    except MessageNotModifiedError:
                        break
                    except FloodWaitError as e:
                        log_tg.warning(f"⏱ FloodWait edit: {e.seconds}s")
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        log_tg.error(f"❌ Edit t={t}: {e}")
                        if t < 3:
                            await asyncio.sleep(2 ** t)
                return

            # Novo envio
            sent = None
            for t in range(1, 4):
                try:
                    sent = await _enviar(msg_final, img)
                    break
                except FloodWaitError as e:
                    log_tg.warning(f"⏱ FloodWait envio: {e.seconds}s")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    log_tg.error(f"❌ Envio t={t}: {e}")
                    if t == 1:
                        img = None  # tenta sem imagem
                    elif t < 3:
                        await asyncio.sleep(2 ** t)

            if sent:
                mp[str(msg_id)] = sent.id
                try:
                    await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
                except Exception as e:
                    log_sys.error(f"❌ salvar_mapa: {e}")

                await _marcar(msg_id)

                try:
                    await scheduler_ok(plat)
                except Exception as e:
                    log_sys.debug(f"⚠️ scheduler_ok: {e}")
                try:
                    antisaturacao_ok(plat, sku)
                except Exception as e:
                    log_sys.debug(f"⚠️ antisaturacao_ok: {e}")
                try:
                    await _burst_add()
                except Exception as e:
                    log_sys.debug(f"⚠️ burst_add: {e}")

                # Magalu com link longo → agenda encurtamento em background
                if plat == "magalu" and mapa:
                    for url_orig, url_conv in mapa.items():
                        if "partner_id" in url_conv and "cutt.ly" not in url_conv:
                            try:
                                await _agendar_edicao_magalu(
                                    url_conv, msg_id)
                            except Exception as e:
                                log_mgl.warning(f"⚠️ agendar edicao: {e}")

                log_sys.info(
                    f"🚀 [OK] @{uname} → {GRUPO_DESTINO} | "
                    f"{msg_id}→{sent.id} | {plat.upper()} sku={sku}")
            else:
                log_sys.error(f"❌ Envio falhou após 3 tentativas | @{uname}")

        except Exception as e:
            log_sys.error(f"❌ CRÍTICO pipeline: {e}", exc_info=True)


# Ponto de entrada público — chamado pelos handlers on_new e on_edit
async def processar(event, is_edit: bool = False):
    """Enfileira o evento no orchestrator."""
    await _enfileirar(event, is_edit)


async def _iniciar_orchestrator():
    """Inicia o worker loop como task assíncrona."""
    log_sys.info(
        f"🎛 Orchestrator | workers={_WORKERS_MAX} "
        f"fila_max={_FILA_MAX} coalesce={_COALESCE_MS}ms")
    asyncio.create_task(_worker_loop())


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 25 ▸ HEALTH CHECK
# Corrige: name '_buf' is not defined
# Usa _buf diretamente (escopo global, já declarado acima)
# ══════════════════════════════════════════════════════════════════════════════

async def _health_check():
    while True:
        await asyncio.sleep(300)
        try:
            db_limpar()

            try:
                with _db() as db:
                    n_links = db.execute(
                        "SELECT COUNT(*) FROM links_cache").fetchone()[0]
                    n_dedup = db.execute(
                        "SELECT COUNT(*) FROM dedupe_temp").fetchone()[0]
                    n_sat   = db.execute(
                        "SELECT COUNT(*) FROM saturacao").fetchone()[0]
            except Exception:
                n_links = n_dedup = n_sat = "?"

            log_hc.info(
                f"💚 links_cache={n_links}(perm) | "
                f"dedupe={n_dedup} | sat={n_sat} | "
                f"anti-loop={len(_IDS_PROC)} | "
                f"fila={len(_buf)} w={_w_ativos} | "
                f"PIL={'OK' if _PIL_OK else 'OFF'}")
        except Exception as e:
            log_hc.error(f"❌ Health: {e}", exc_info=True)

def db_limpar():
    try:
        with _db() as db:
            agora = int(time.time())

            # 🔹 links cache (24h)
            db.execute(
                "DELETE FROM links_cache WHERE ts < ?",
                (agora - 86400,)
            )

            # 🔹 dedupe (6h)
            db.execute(
                "DELETE FROM dedupe_temp WHERE ts < ?",
                (agora - 21600,)
            )

            # 🔹 saturação (12h)
            db.execute(
                "DELETE FROM saturacao WHERE ts < ?",
                (agora - 43200,)
            )

    except Exception as e:
        log_hc.error(f"[DB LIMPAR ERRO] {e}", exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 26 ▸ INICIALIZAÇÃO COM AUTO-RESTART
# Corrige: name '_iniciar_orchestrator' is not defined
# Corrige: name 'event' is not defined (handlers corretos)
# _init_globals() chamado ANTES de qualquer task
# ══════════════════════════════════════════════════════════════════════════════

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


async def _run():
    # Inicializa locks assíncronos no loop correto
    _init_globals()
    _init_db()

    log_sys.info("🔌 Conectando...")
    await client.connect()

    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida — verifique TELEGRAM_SESSION")
        return False

    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 Grupos: {GRUPOS_ORIGEM}")
    log_sys.info(f"📣 Destino: {GRUPO_DESTINO}")
    log_sys.info(f"🟠 Amazon: {_AMZ_TAG}")
    log_sys.info(f"🟣 Shopee: {_SHP_APP_ID}")
    log_sys.info(f"🔵 Magalu: promoter={_MGL_PROMOTER} slug={_MGL_SLUG}")
    log_sys.info(f"🖼  Pillow: {'OK' if _PIL_OK else 'pip install Pillow'}")
    log_sys.info("🚀 FOGUETÃO v74.0 — ONLINE")

    # Handlers — usam processar() que está no escopo global
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

    # Tasks — na ordem correta
    asyncio.create_task(_health_check())
    await _iniciar_orchestrator()

    await client.run_until_disconnected()
    return True


async def main():
    while True:
        try:
            await _run()
        except (AuthKeyUnregisteredError, SessionPasswordNeededError) as e:
            log_sys.error(f"❌ Auth fatal — encerrando: {e}")
            break
        except Exception as e:
            log_sys.error(
                f"💥 Caiu: {e} — restart em 15s...", exc_info=True)
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(15)


if __name__ == "__main__":
    asyncio.run(main())
    

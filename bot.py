# ╔══════════════════════════════════════════════════════════════════════╗
# ║  FOGUETÃO v75.0 — PRODUÇÃO                                          ║
# ║  Entrada → Processamento → Saída                                    ║
# ╚══════════════════════════════════════════════════════════════════════╝
from __future__ import annotations
import asyncio, concurrent.futures, hashlib, heapq, io, json, logging
import os, random, re, sqlite3, time, unicodedata
from contextlib import contextmanager
from dataclasses import dataclass
from difflib import SequenceMatcher
from threading import Lock
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse
import aiohttp
from bs4 import BeautifulSoup
from telethon import TelegramClient, events
from telethon.errors import (AuthKeyUnregisteredError, FloodWaitError,
    MessageNotModifiedError, SessionPasswordNeededError)
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage
try:
    from PIL import Image
    _PIL_OK = True
except ImportError:
    _PIL_OK = False

# ── MÓDULO 1: LOGS ────────────────────────────────────────────────────────────
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

# ── MÓDULO 2: CONFIGURAÇÕES ───────────────────────────────────────────────────
API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION", "")
GRUPOS_ORIGEM  = ["promotom", "fumotom", "botofera", "fadadoscupons"]
GRUPO_DESTINO  = "@ofertap"
_AMZ_TAG       = os.environ.get("AMAZON_TAG",         "leo21073-20")
_SHP_APP_ID    = os.environ.get("SHOPEE_APP_ID",      "18348480261")
_SHP_SECRET    = os.environ.get("SHOPEE_SECRET",      "")
_MGL_PARTNER   = os.environ.get("MAGALU_PARTNER_ID",  "3440")
_MGL_PROMOTER  = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
_MGL_PID       = os.environ.get("MAGALU_PID",         "magazinevoce")
_MGL_SLUG      = os.environ.get("MAGALU_SLUG",        "magazineleo12")
_CUTTLY_KEY    = os.environ.get("CUTTLY_API_KEY",     "")
_IMG_AMZ = "cupom-amazon.jpg"
_IMG_SHP = "shopee_promo.jpg"
_IMG_MGL = "magalu_promo.jpg"
ARQUIVO_CACHE      = "cache_dedup.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"
_SEM_ENVIO = asyncio.Semaphore(3)
_SEM_HTTP  = asyncio.Semaphore(20)
_EXECUTOR  = concurrent.futures.ThreadPoolExecutor(max_workers=4)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]
_ENCURTADORES = frozenset(["bit.ly","cutt.ly","tinyurl.com","t.co","ow.ly","goo.gl","rb.gy","is.gd","tiny.cc","buff.ly","short.io","bl.ink","rebrand.ly","shorturl.at"])
_PRESERVE = frozenset(["wa.me","api.whatsapp.com"])
_DELETAR  = frozenset(["t.me","telegram.me","telegram.org","chat.whatsapp.com"])

# ── MÓDULO 3: PERSISTÊNCIA JSON ───────────────────────────────────────────────
_MAP_LOCK   = Lock()
_CACHE_LOCK = Lock()

def _ler_json(path: str) -> dict:
    if not os.path.exists(path): return {}
    try:
        with open(path,"r",encoding="utf-8") as f: return json.load(f)
    except Exception as e:
        log_sys.error(f"❌ Ler {path}: {e}"); return {}

def _gravar_json(path: str, data: dict, lock: Lock):
    with lock:
        try:
            with open(path,"w",encoding="utf-8") as f: json.dump(data,f,ensure_ascii=False,indent=2)
        except Exception as e: log_sys.error(f"❌ Gravar {path}: {e}")

ler_mapa     = lambda: _ler_json(ARQUIVO_MAPEAMENTO)
salvar_mapa  = lambda m: _gravar_json(ARQUIVO_MAPEAMENTO, m, _MAP_LOCK)
ler_cache    = lambda: _ler_json(ARQUIVO_CACHE)
salvar_cache = lambda c: _gravar_json(ARQUIVO_CACHE, c, _CACHE_LOCK)

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 4 ▸ FILTRO DE TEXTO
# Bloqueia: hardware, periféricos, Mercado Livre
# Passa: multi-produto, listas de ofertas
# ══════════════════════════════════════════════════════════════════════════════

_FILTRO_TEXTO = [
    "Monitor Samsung", "Fonte Mancer", "Placa de video", "Monitor LG",
    "PC home Essential", "Suporte articulado", "VHAGAR", "Superframe",
    "AM5", "AM4", "GTX", "DDR5", "DDR4", "Dram", "Monitor Safe",
    "Monitor Redragon", "CL18", "CL16", "CL32", "MT/s", "MHz",
    "RX 580", "Ryzen", "Placa Mãe", "Gabinete Gamer",
    "Water Cooler", "Air Cooler",
]

# Mercado Livre — nunca passa (não é plataforma afiliada)
_RE_MERCADO_LIVRE = re.compile(
    r'\b(?:mercado\s*livre|mercadolivre|mercado\s*pago)\b', re.I)

_RE_MULTI_OFERTA = re.compile(
    r'\b(?:ofertas?|promoções?)\s+(?:na\s+|no\s+|da\s+)?'
    r'(?:shopee|amazon|magalu|magazine\s*luiza)\b', re.I)

_RE_PRECO_LINHA = re.compile(r'R\$\s?[\d.,]+')
_RE_URL_COUNT   = re.compile(r'https?://')


def _eh_multi_produto(texto: str) -> bool:
    if _RE_MULTI_OFERTA.search(texto):
        return True
    linhas_preco = sum(1 for l in texto.splitlines()
                       if _RE_PRECO_LINHA.search(l))
    urls = len(_RE_URL_COUNT.findall(texto))
    return linhas_preco >= 2 or urls >= 3


def texto_bloqueado(texto: str) -> bool:
    """
    Retorna True se o texto deve ser bloqueado.
    Mercado Livre: sempre bloqueado (não é plataforma afiliada).
    Multi-produto: bypass do filtro de hardware.
    """
    # Mercado Livre — bloqueia sempre, antes de qualquer outra regra
    if _RE_MERCADO_LIVRE.search(texto):
        log_fil.debug("🚫 Mercado Livre bloqueado")
        return True

    # Multi-produto — bypass do filtro de hardware
    if _eh_multi_produto(texto):
        log_fil.debug("✅ Multi-produto — bypass filtro")
        return False

    tl = texto.lower()
    for p in _FILTRO_TEXTO:
        if p.lower() in tl:
            log_fil.debug(f"🚫 Filtro: '{p}'")
            return True

    return False

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 5 ▸ WHITELIST + CLASSIFICAÇÃO
# flapremios.com.br → classificado como shopee (campanha afiliada)
# Mercado Livre → None (fora da whitelist, não processa)
# ══════════════════════════════════════════════════════════════════════════════

_AMZ_DOMINIOS = frozenset({
    "amazon.com.br", "amazon.com",
    "amzn.to", "amzn.com", "a.co",
    "amzlink.to", "amzn.eu",
})
_SHP_DOMINIOS = frozenset({
    "shopee.com.br", "s.shopee.com.br",
    "shopee.com", "shope.ee",
    "flapremios.com.br",    # campanha afiliada Shopee
})
_MGL_DOMINIOS = frozenset({
    "magazineluiza.com.br", "sacola.magazineluiza.com.br",
    "magazinevoce.com.br", "maga.lu",
})

# Domínios bloqueados explicitamente — nunca processados
_BLOQUEADOS = frozenset({
    "mercadolivre.com.br", "mercadopago.com.br",
    "mercadolivre.com", "meli.com",
    "ml.com.br",
})


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

    flapremios.com.br → 'shopee'
    Mercado Livre     → None (bloqueado)
    """
    nl = _netloc(url)
    if not nl:
        return None

    # Domínios bloqueados explicitamente
    for dom in _BLOQUEADOS:
        if nl == dom or nl.endswith("." + dom):
            log_lnk.debug(f"🚫 Domínio bloqueado: {nl}")
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

    # Amazon
    for dom in _AMZ_DOMINIOS:
        if nl == dom or nl.endswith("." + dom):
            return "amazon"

    # Shopee (inclui flapremios.com.br)
    for dom in _SHP_DOMINIOS:
        if nl == dom or nl.endswith("." + dom):
            return "shopee"

    # Encurtadores genéricos
    for enc in _ENCURTADORES:
        if nl == enc or nl.endswith("." + enc):
            return "expandir"

    return None

# ── MÓDULO 6: DESENCURTADOR 15 CAMADAS ───────────────────────────────────────
_FORCA_GET = frozenset({"amzlink.to","amzn.to","a.co","amzn.com","bit.ly","tinyurl.com","rb.gy","is.gd","cutt.ly","ow.ly","buff.ly"})

async def desencurtar(url: str, sessao: aiohttp.ClientSession, depth: int = 0) -> str:
    if depth > 15: log_lnk.warning(f"⚠️ Max depth d={depth}"); return url
    url = url.strip().rstrip('.,;)>')
    if not url.startswith(("http://","https://")): return url
    nl   = _netloc(url)
    hdrs = {"User-Agent": random.choice(USER_AGENTS),"Accept":"text/html,*/*;q=0.9","Accept-Language":"pt-BR,pt;q=0.9"}
    try:
        usar_head = nl not in _FORCA_GET and not any(nl.endswith("."+d) for d in _FORCA_GET)
        if usar_head:
            try:
                async with sessao.head(url,headers=hdrs,allow_redirects=True,timeout=aiohttp.ClientTimeout(total=8),max_redirects=20) as r:
                    final = str(r.url)
                    if final != url: return await desencurtar(final,sessao,depth+1)
            except Exception: pass
        async with sessao.get(url,headers=hdrs,allow_redirects=True,timeout=aiohttp.ClientTimeout(total=15),max_redirects=20) as r:
            pos  = str(r.url)
            html = await r.text(errors="ignore")
            if pos != url: return await desencurtar(pos,sessao,depth+1)
            soup = BeautifulSoup(html,"html.parser")
            ref  = soup.find("meta",attrs={"http-equiv":re.compile("refresh",re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)",ref["content"],re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    if novo.startswith("http"): return await desencurtar(novo,sessao,depth+1)
            for pat in [r'window\.location(?:\.href)?\s*=\s*["\']([^"\']{15,})["\']',r'location\.replace\s*\(\s*["\']([^"\']{15,})["\']\s*\)']:
                mj = re.search(pat,html)
                if mj and mj.group(1).startswith("http"): return await desencurtar(mj.group(1),sessao,depth+1)
            og = soup.find("meta",attrs={"property":"og:url"})
            if og and og.get("content","").startswith("http") and og["content"] != url:
                return await desencurtar(og["content"],sessao,depth+1)
            canon = soup.find("link",rel="canonical")
            if canon and canon.get("href","").startswith("http") and canon["href"] != url:
                return await desencurtar(canon["href"],sessao,depth+1)
            return pos
    except asyncio.TimeoutError: log_lnk.warning(f"⏱ Timeout d={depth}: {url[:60]}"); return url
    except Exception as e: log_lnk.error(f"❌ Desencurtar d={depth}: {e}"); return url

# ── MÓDULO 7: MOTOR AMAZON ────────────────────────────────────────────────────

CACHE_TTL=86400

def db_get_link(url:str)->Optional[str]:
    try:
        with _db() as db:
            row=db.execute("SELECT url_conv,ts FROM links_cache WHERE url_orig=?",(url,)).fetchone()
        if row and time.time()-row[1]<CACHE_TTL:return row[0]
    except:pass
    return None

def db_set_link(url_orig:str,url_conv:str,plat:str):
    try:
        with _db() as db:
            db.execute("INSERT OR REPLACE INTO links_cache VALUES (?,?,?,?)",(url_orig,url_conv,plat,time.time()))
    except:pass

async def desencurtar(url:str,sessao:aiohttp.ClientSession)->str:
    try:
        async with sessao.head(url,allow_redirects=True,timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.url:return str(r.url)
    except:pass
    try:
        async with sessao.get(url,allow_redirects=True,timeout=aiohttp.ClientTimeout(total=8)) as r:
            return str(r.url)
    except:return url

_AMZ_LIXO=frozenset({"ascsubtag","btn_ref","ref_","ref","smid","sprefix","spla","dchild","linkcode","linkid","camp","creative","pf_rd_p","pf_rd_r","pd_rd_wg","pd_rd_w","content-id","pd_rd_r","pd_rd_i","ie","qid","_encoding","dib","dib_tag","m","marketplaceid","ufe","th","psc","ingress","visitid","lp_context_asin","redirectasin","redirectmerchantid","redirectasincustomeraction","ds","rnid","sr"})
_AMZ_MANTER=frozenset({"keywords","node","k","i","rh","n","field-keywords"})

def _extrair_asin(url:str)->Optional[str]:
    p=urlparse(url);t=p.path+"?"+p.query
    for pat in [r'/dp/([A-Z0-9]{10})',r'/gp/product/([A-Z0-9]{10})',r'[?&]asin=([A-Z0-9]{10})']:
        m=re.search(pat,t,re.I)
        if m:return m.group(1).upper()
    return None

def _limpar_url_amazon(url:str)->Optional[str]:
    try:
        p=urlparse(url);asin=_extrair_asin(url)
        if asin:return f"https://www.amazon.com.br/dp/{asin}?tag={_AMZ_TAG}"
        if "/promotion/" in p.path:return f"https://www.amazon.com.br{p.path}?tag={_AMZ_TAG}"
        params={}
        for k,v in parse_qs(p.query).items():
            if k.lower() in _AMZ_MANTER and len(v[0])<60:params[k]=v[0]
        params["tag"]=_AMZ_TAG
        return urlunparse(p._replace(scheme="https",netloc="www.amazon.com.br",query=urlencode(params),fragment=""))
    except:return None

async def motor_amazon(url:str,sessao:aiohttp.ClientSession)->Optional[str]:
    log_amz.debug(f"▶ IN: {url[:80]}")
    cached=db_get_link(url)
    if cached:log_amz.debug("💾 Cache");return cached
    try:
        async with _SEM_HTTP:exp=await desencurtar(url,sessao)
    except:exp=url
    log_amz.debug(f"  EXP: {exp[:80]}")
    try:
        if classificar(exp)!="amazon":log_amz.warning("⚠️ Classificação falhou")
    except:pass
    final=_limpar_url_amazon(exp)
    if not final:
        base=exp.split("?",1)[0]
        final=f"{base}?tag={_AMZ_TAG}"
    log_amz.info(f"  ✅ OUT: {final}")
    db_set_link(url,final,"amazon")
    return final

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 8 ▸ MOTOR SHOPEE — ISOLADO
#
# Aceita QUALQUER link da Shopee: produto, roleta, campanha, evento.
# flapremios.com.br → repassa direto (campanha, não precisa converter)
# Se API falhar → retorna URL original (nunca descarta)
# ══════════════════════════════════════════════════════════════════════════════

# Domínios Shopee que são repassados diretamente (não passam pela API)
_SHP_REPASSE_DIRETO = frozenset({
    "flapremios.com.br",
})


async def motor_shopee(url: str,
                        sessao: aiohttp.ClientSession) -> Optional[str]:
    """
    Motor exclusivo Shopee.
    flapremios.com.br → repasse direto sem chamar API.
    Outros links Shopee → tenta API, fallback para original.
    """
    log_shp.debug(f"▶ IN: {url[:80]}")

    nl = _netloc(url)

    # Campanha de repasse direto — não precisa converter
    for dom in _SHP_REPASSE_DIRETO:
        if nl == dom or nl.endswith("." + dom):
            log_shp.info(f"  ↩️ Repasse direto (campanha): {url[:60]}")
            return url

    # Cache primeiro
    cached = db_get_link(url)
    if cached:
        log_shp.debug(f"  💾 Cache: {cached[:60]}")
        return cached

    # Tenta API GraphQL — 3 tentativas com backoff
    for t in range(1, 4):
        try:
            ts      = str(int(time.time()))
            payload = json.dumps({
                "query": (
                    f'mutation {{ generateShortLink(input: '
                    f'{{ originUrl: "{url}" }}) {{ shortLink }} }}'
                )
            }, separators=(",", ":"))
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
                        log_shp.info(f"  ✅ Convertido t={t}: {link}")
                        db_set_link(url, link, "shopee")
                        return link

                    erro = res.get("errors") or res.get("error")
                    log_shp.warning(
                        f"  ⚠️ API sem shortLink t={t}: {erro or res}")

        except Exception as e:
            log_shp.warning(f"  ⚠️ t={t}/3: {e}")

        await asyncio.sleep(1.5 * t)

    # Fallback: URL original — nunca perde a oferta
    log_shp.warning(f"  ↩️ Fallback original: {url[:60]}")
    return url

# ── MÓDULO 9: MOTOR MAGALU ────────────────────────────────────────────────────
_CUTTLY_LAST_429: float = 0.0
_CUTTLY_BACKOFF:  float = 65.0

def _validar_magalu(url: str) -> bool:
    p = urlparse(url); host = p.netloc.lower(); path = p.path.rstrip("/")
    if "sacola" in host and path in ("","/"): return False
    if not path or path == "/" or len(path.split("/")) < 2: return False
    return True

def _afiliar_magalu(url: str) -> str:
    p      = urlparse(url)
    path   = re.sub(r'^(/magazine)[^/]+', rf'\1{_MGL_SLUG}', p.path)
    params = {k:v[0] for k,v in parse_qs(p.query,keep_blank_values=True).items()}
    for k in ["tag","partnerid","promoterid","afforcedeeplink","deeplinkvalue","isretargeting","partner_id","promoter_id","utm_source","utm_medium","utm_campaign","pid","c","af_force_deeplink","deep_link_value"]:
        params.pop(k,None)
    params.update({"partner_id":_MGL_PARTNER,"promoter_id":_MGL_PROMOTER,"utm_source":"divulgador","utm_medium":"magalu","utm_campaign":_MGL_PROMOTER,"pid":_MGL_PID,"c":_MGL_PROMOTER,"af_force_deeplink":"true"})
    base = urlunparse(p._replace(path=path,query="",fragment=""))
    params["deep_link_value"] = f"{base}?utm_source=divulgador&utm_medium=magalu&partner_id={_MGL_PARTNER}&promoter_id={_MGL_PROMOTER}"
    return urlunparse(p._replace(path=path,query=urlencode(params),fragment=""))

async def _cuttly_tentar(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    global _CUTTLY_LAST_429
    api = f"https://cutt.ly/api/api.php?key={_CUTTLY_KEY}&short={quote(url,safe='')}"
    for t in range(1,4):
        espera = _CUTTLY_BACKOFF - (time.time() - _CUTTLY_LAST_429)
        if espera > 0: await asyncio.sleep(espera)
        try:
            async with _SEM_HTTP:
                async with sessao.get(api,timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status == 429: _CUTTLY_LAST_429 = time.time(); await asyncio.sleep(_CUTTLY_BACKOFF); continue
                    if r.status == 401: log_mgl.error("  ❌ Cuttly 401 — chave inválida"); return None
                    if r.status != 200: await asyncio.sleep(2**t); continue
                    try: data = await r.json(content_type=None)
                    except Exception: await asyncio.sleep(2**t); continue
                    status = data.get("url",{}).get("status")
                    if status in (7,2):
                        short = data["url"].get("shortLink")
                        if short: log_mgl.info(f"  ✂️ t={t}: {short}"); return short
                    await asyncio.sleep(2**t)
        except asyncio.TimeoutError: await asyncio.sleep(2**t)
        except Exception as e: log_mgl.error(f"  ❌ Cuttly t={t}: {e}"); await asyncio.sleep(2**t)
    return None

async def _cuttly_background(url_longo: str, msg_id_origem: int):
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as sessao:
        for tent in range(15):
            await asyncio.sleep(45)
            try:
                short = await _cuttly_tentar(url_longo,sessao)
                if short:
                    loop = asyncio.get_event_loop()
                    mapa = await loop.run_in_executor(_EXECUTOR,ler_mapa)
                    id_dest = mapa.get(str(msg_id_origem))
                    if id_dest:
                        try:
                            msg_atual = await client.get_messages(GRUPO_DESTINO,ids=id_dest)
                            if msg_atual and msg_atual.text:
                                novo = msg_atual.text.replace(url_longo,short)
                                if novo != msg_atual.text:
                                    await client.edit_message(GRUPO_DESTINO,id_dest,novo,parse_mode="md")
                                    log_mgl.info(f"  ✅ Editado: {id_dest}")
                        except Exception as e: log_mgl.warning(f"  ⚠️ Edição bg: {e}")
                    db_set_link(url_longo,short,"magalu"); return
            except Exception as e: log_mgl.warning(f"  ⚠️ BG t={tent}: {e}")

async def _agendar_edicao_magalu(url_longo: str, msg_id: int):
    asyncio.create_task(_cuttly_background(url_longo,msg_id))

async def motor_magalu(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    log_mgl.debug(f"▶ IN: {url[:80]}")
    cached = db_get_link(url)
    if cached: return cached
    nl = _netloc(url)
    if "maga.lu" in nl or nl in _ENCURTADORES or classificar(url) == "expandir":
        try:
            async with _SEM_HTTP: url = await desencurtar(url,sessao)
        except Exception as e: log_mgl.error(f"  ❌ Desencurtar: {e}"); return None
    if classificar(url) != "magalu": return None
    if not _validar_magalu(url): return None
    afiliado = _afiliar_magalu(url)
    short    = await _cuttly_tentar(afiliado,sessao)
    if short: db_set_link(url,short,"magalu"); log_mgl.info(f"  ✅ OUT: {short}"); return short
    log_mgl.warning("  ⚠️ Cuttly falhou — link longo, editará depois"); return afiliado

# ── MÓDULO 10: EXTRAÇÃO DE LINKS ─────────────────────────────────────────────

_RE_URL = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c\u200d\u2060]+')

def extrair_links(texto: str) -> Tuple[List[str],List[str]]:
    brutos = [u.strip().rstrip('.,;)>]}') for u in _RE_URL.findall(texto)]
    converter: List[str] = []
    preservar: List[str] = []
    vistos: set = set()

    for url in brutos:
        if url in vistos:
            continue

        vistos.add(url)
        plat = classificar(url)

        if plat == "preservar":
            preservar.append(url)

        elif plat in ("amazon", "shopee", "magalu", "ifood", "expandir"):
            converter.append(url)

    return converter, preservar

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 11 ▸ PIPELINE DE CONVERSÃO
#
# Mantém sua lógica modular e adiciona converter_links() que o pipeline chama.
# converter_links() é o adaptador entre seu Módulo 11 e o _pipeline().
# ══════════════════════════════════════════════════════════════════════════════

LOJAS_PERMITIDAS = ("amazon", "shopee", "Ifood", "magalu")


def detectar_plataforma(url: str) -> str:
    """Detecta plataforma. flapremios.com.br → shopee."""
    try:
        dominio = urlparse(url).netloc.lower()
    except Exception:
        dominio = ""
    # flapremios é campanha Shopee — classificar() já retorna 'shopee'
    return classificar(url) or "desconhecido"


def verificar_cache(url: str, plat: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        cached = db_get_link(url)
        if cached and plat in LOJAS_PERMITIDAS:
            return cached, plat
    except Exception as e:
        log_lnk.error(f"❌ Cache erro: {e}")
    return None, None


async def tratar_expandir(url: str,
                           sessao: aiohttp.ClientSession) -> str:
    try:
        async with _SEM_HTTP:
            exp = await desencurtar(url, sessao)
        if exp and exp != url:
            return exp
    except Exception as e:
        log_lnk.error(f"❌ Expandir: {e}")
    return url


async def processar_plataforma(url: str, plat: str,
                                 sessao: aiohttp.ClientSession
                                 ) -> Tuple[Optional[str], Optional[str]]:

    if plat not in LOJAS_PERMITIDAS:
        return None, None

    # 🟡 iFood → passa direto (ANTES de qualquer motor)
    if plat == "ifood":
        return url, "ifood"

    motores = {
        "shopee": motor_shopee,
        "amazon": motor_amazon,
        "magalu": motor_magalu,
    }

    motor = motores[plat]

    link = await motor(url, sessao)

    if plat == "shopee":
        return (link, "shopee") if link else (None, None)

    if plat in ("amazon", "magalu"):
        return (link, plat) if link else (None, None)

    return None, None

    motores = {
        "shopee": motor_shopee,
        "amazon": motor_amazon,
        "magalu": motor_magalu,
    }
    motor = motores[plat]

    # Tenta converter com retry interno de cada motor
    link = await motor(url, sessao)

    if plat == "shopee":
        # Shopee: motor_shopee nunca retorna None
        # (sempre retorna original como fallback)
        return (link, "shopee") if link else (None, None)

    if plat in ("amazon", "magalu"):
        return (link, plat) if link else (None, None)

    return None, None


async def converter_link(url: str,
                          sessao: aiohttp.ClientSession
                          ) -> Tuple[Optional[str], Optional[str]]:
    """Converte um único link."""
    try:
        plat = detectar_plataforma(url)

        cached_link, cached_plat = verificar_cache(url, plat)
        if cached_link:
            return cached_link, cached_plat

        if plat == "expandir":
            url_exp = await tratar_expandir(url, sessao)
            if url_exp != url:
                return await converter_link(url_exp, sessao)
            plat = detectar_plataforma(url)

        return await processar_plataforma(url, plat, sessao)

    except Exception as e:
        log_lnk.error(f"❌ converter_link: {e}")
        return None, None


async def _converter_um(url: str,
                         sessao: aiohttp.ClientSession
                         ) -> Tuple[Optional[str], Optional[str]]:
    """Interface para gather paralelo."""
    return await converter_link(url, sessao)


async def converter_links(links: List[str]) -> Tuple[Dict[str, str], str]:
    """
    Adaptador principal — chamado por _pipeline().
    Converte até 50 links em paralelo.
    Retorna (mapa_original→convertido, plataforma_principal).

    CRÍTICO: esta função DEVE existir com este nome exato.
    _pipeline() chama: mapa, plat = await converter_links(...)
    """
    if not links:
        return {}, "amazon"

    log_lnk.info(f"🚀 Convertendo {len(links)} links")

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

    mapa:  Dict[str, str] = {}
    plats: List[str]       = []

    for i, res in enumerate(resultados):
        if isinstance(res, Exception):
            log_lnk.error(f"❌ [{i}] {links[i][:50]}: {res}")
            continue
        novo, plat = res
        if novo and plat:
            mapa[links[i]] = novo
            plats.append(plat)
            log_lnk.debug(f"  [{plat.upper()}] → {novo[:50]}")

    plat_p = max(set(plats), key=plats.count) if plats else "amazon"
    log_lnk.info(f"✅ {len(mapa)}/{len(links)} | plat={plat_p}")
    return mapa, plat_p


# ── MÓDULO 12: LIMPEZA DE RUÍDO ──────────────────────────────────────────────
_RE_INVISIVEIS  = re.compile(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]')
_RE_GRUPO_EXT   = re.compile(r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)[^\s]*',re.I)
_RE_LIXO_STRUCT = re.compile(r'^\s*(?:-?\s*An[uú]ncio|Publicidade|:::+|---+|===+|[-–—]\s*(?:ML|MG|AMZ)|(?:ML|MG|AMZ)\s*:)\s*$',re.I)
_RE_CTA         = re.compile(r'^\s*(?:link\s+(?:do\s+)?produto|link\s+da\s+oferta|resgate\s+aqui|clique\s+aqui|acesse\s+aqui|compre\s+aqui|grupo\s+vip|entrar\s+no\s+grupo|acessar\s+grupo)\s*:?\s*$',re.I)
_RE_REDES       = re.compile(r'^\s*(?:redes\s+\w+|[-–]\s*grupo\s*(?:cupons?|promoções?|vip)?\s*:?\s*$|[-–]\s*(?:chat|twitter|whatsapp|instagram|tiktok|youtube)\s*:?\s*$|acesse\s+nossas\s+redes)',re.I)
_RE_ROTULO      = re.compile(r'^\s*[-–•]\s*\w[\w\s]{0,30}:\s*$')
_RE_EMOJI_CHECK = re.compile(r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF\u2B50\u2B55]")

def _tem_emoji(s: str) -> bool: return bool(_RE_EMOJI_CHECK.search(s))

def _eh_header_canal(linha: str) -> bool:
    l = linha.strip()
    if not l or _tem_emoji(l[0]): return False
    if re.match(r'^[A-ZÀ-Ú][\w\s]{2,30}\s*/\s*[\w\s]{2,30}',l): return True
    if re.match(r'^[A-ZÀÁÂÃÉÊÍÓÔÕÚ\s]{4,30}[\s🔥💥⚡🚀]+$',l,re.UNICODE): return True
    return False

def limpar_ruido_textual(texto: str) -> str:
    texto = _RE_INVISIVEIS.sub(" ",texto).replace("\r\n","\n").replace("\r","\n")
    linhas = texto.split("\n"); saida: List[str] = []; vazio = False; em_redes = False; primeira = True
    for linha in linhas:
        l = linha.strip()
        if not l:
            if not vazio: saida.append("")
            vazio = True; em_redes = False; continue
        vazio = False
        if primeira:
            primeira = False
            if _eh_header_canal(l): continue
        if _RE_REDES.match(l): em_redes = True; continue
        if em_redes:
            if _RE_ROTULO.match(l) or not l: continue
            if not re.match(r'https?://',l): em_redes = False
            else: continue
        if _RE_CTA.match(l): continue
        if _RE_LIXO_STRUCT.match(l): continue
        if _RE_GRUPO_EXT.search(l):
            l = _RE_GRUPO_EXT.sub("",l).strip()
            if not l: continue
        saida.append(l)
    return "\n".join(saida).strip()

# ── MÓDULO 13: EMOJIS + RADARES ──────────────────────────────────────────────
_EMJ: Dict[str,List[str]] = {
    "titulo_oferta":["🔥"],"titulo_cupom":["🚨"],
    "titulo_evento":["⚠️"],"preco":["💵","💰"],
    "cupom_cod":["🎟"],"resgate_aqui":["✅"],
    "carrinho":["🛒"],"frete":["🚚","📦"],"multi_item":["🔹"],
}
_EMJ_IDX: Dict[str,int] = {k:0 for k in _EMJ}

def _prox_emoji(cat: str) -> str:
    lista = _EMJ[cat]; idx = _EMJ_IDX[cat]; emoji = lista[idx%len(lista)]
    _EMJ_IDX[cat] = (idx+1)%len(lista); return emoji

_KW_CUPOM    = re.compile(r'\b(?:cupom|cupon|c[oó]digo|coupon|off|resgate|cod)\b',re.I)
_KW_PRECO    = re.compile(r'R\$\s?[\d.,]+',re.I)
_KW_FRETE    = re.compile(r'\b(?:frete\s+gr[aá]t|entrega\s+gr[aá]t|sem\s+frete|frete\s+0)\b',re.I)
_KW_EVENTO   = re.compile(r'\b(?:quiz|roleta|miss[aã]o|arena|girar|gire|roda|jogar|jogue|desafio)\b',re.I)
_KW_STATUS   = re.compile(r'\b(?:voltando|voltou|normalizou|renovado|estoque\s+renovado|regularizou|ainda\s+ativo|de\s+volta|reativado)\b',re.I)
_KW_RESGATE  = re.compile(r'\b(?:resgate|clique|acesse|ative|use\s+o\s+cupom)\b',re.I)
_KW_CARRINHO = re.compile(r'\b(?:carrinho|cart)\b',re.I)
_KW_COD      = re.compile(r'\b([A-Z][A-Z0-9_-]{3,19})\b')

def _contar_produtos(texto: str) -> int:
    return sum(1 for l in texto.splitlines() if _KW_PRECO.search(l))

def _emoji_de_linha(linha: str, eh_titulo: bool, is_multi: bool = False) -> Optional[str]:
    if _tem_emoji(linha): return None
    if eh_titulo:
        if _KW_EVENTO.search(linha): return _prox_emoji("titulo_evento")
        if _KW_CUPOM.search(linha): return _prox_emoji("titulo_cupom")
        return _prox_emoji("titulo_oferta")
    if is_multi and _KW_PRECO.search(linha): return "🔹"
    if _KW_FRETE.search(linha): return _prox_emoji("frete")
    if _KW_CUPOM.search(linha): return _prox_emoji("cupom_cod")
    if _KW_PRECO.search(linha): return _prox_emoji("preco")
    if _KW_RESGATE.search(linha): return _prox_emoji("resgate")
    if _KW_CARRINHO.search(linha): return _prox_emoji("carrinho")
    return None

# ── MÓDULO 14: RENDERIZADOR ───────────────────────────────────────────────────
_RE_LIXO_PREF  = re.compile(r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:|[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?)\s*',re.I)
_RE_ANUNCIO    = re.compile(r'^\s*[-#]?\s*(?:an[uú]ncio|publicidade|patrocinado)\s*$',re.I)
_RE_URL_RENDER = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c]+')
_FALSO_CUPOM   = frozenset({"FRETE","GRÁTIS","GRATIS","AMAZON","SHOPEE","MAGALU","LINK","CLIQUE","ACESSE","CONFIRA","HOJE","AGORA","PROMO","OFF","BLACK","SUPER","MEGA","ULTRA","VIP","NOVO","NOVA","NUM","PRECO","PCT","PS5","PS4","XBOX","USB","ATX","RGB","LED","HD","SSD","RAM","APP","BOT","API","URL","OK","BR"})

def _crases(linha: str) -> str:
    if "http" in linha or "`" in linha: return linha
    if not (_KW_CUPOM.search(linha) or _KW_COD.search(linha)): return linha
    def _sub(m: re.Match) -> str:
        c = m.group(0); return c if (c in _FALSO_CUPOM or len(c) < 4) else f"`{c}`"
    return re.sub(r'\b([A-Z][A-Z0-9_-]{4,20})\b',_sub,linha)

def renderizar(texto: str, mapa_links: Dict[str,str], links_preservar: List[str], plat: str) -> str:
    mapa = {**mapa_links,**{u:u for u in links_preservar}}
    is_multi = _contar_produtos(texto) >= 2; saida = []; primeiro = True
    for linha in texto.split("\n"):
        l = linha.strip()
        if not l: saida.append(""); continue
        if _RE_ANUNCIO.match(l): saida.append(l); continue
        l = _RE_LIXO_PREF.sub("",l).strip()
        if not l: continue
        urls_na_linha = _RE_URL_RENDER.findall(l); sem_urls = _RE_URL_RENDER.sub("",l).strip()
        if urls_na_linha and not sem_urls:
            for u in urls_na_linha:
                uc = u.rstrip('.,;)>')
                if uc in mapa: saida.append(mapa[uc])
            continue
        l = _RE_URL_RENDER.sub(lambda m: mapa.get(m.group(0).rstrip('.,;)>'),""), l).strip()
        if not l: continue
        l = _crases(l)
        if not _tem_emoji(l):
            e = _emoji_de_linha(l,eh_titulo=primeiro,is_multi=is_multi)
            if e: l = f"{e} {l}"
        primeiro = False; saida.append(l)
    return "\n".join(saida).strip()

# ── MÓDULO 15: DEDUPLICAÇÃO GLOBAL INTELIGENTE ──────────────────────────────
TTL_DEDUPE=86400;JANELA_RAPIDA=600;_SIM_FORTE=0.88;_SIM_MEDIO=0.75

_RUIDO_NORM=frozenset({"promo","promocao","promoção","oferta","desconto","cupom","corre","aproveita","urgente","gratis","grátis","frete","hoje","agora","imperdivel","imperdível","exclusivo","limitado","corra","ative","use","saiu","vazou","resgate","acesse","confira","link","clique","app","relampago","relâmpago","click","veja","novo","nova","valido","válido","somente","apenas","ate","até","partir","ainda","volta","ativo","disponivel","disponível","pix","parcelas","unidades","estoque"})
_RE_EMJ=re.compile(r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF]+",re.UNICODE)

def _hash(s:str)->str:return hashlib.sha256(s.encode()).hexdigest()[:32]
def _rm_ac(t:str)->str:return "".join(c for c in unicodedata.normalize("NFD",t) if unicodedata.category(c)!="Mn")
def _cupons(t:str)->frozenset:return frozenset(re.findall(r'\b([A-Z0-9_-]{4,20})\b',t))
def _benef(t:str)->frozenset:
    b=set()
    if re.search(r'frete\s+gr[aá]t',t,re.I):b.add("frete_gratis")
    for m in re.findall(r'(\d+)\s*%?\s*off',t,re.I):b.add(f"off_{m}")
    return frozenset(b)
def _alma(t:str)->str:
    t=_rm_ac(t.lower());t=re.sub(r'https?://\S+',' ',t);t=_RE_EMJ.sub(' ',t)
    t=re.sub(r'(\d+\s?(gb|tb|mah|v|w|hz|fps))',r' ATTR_\1 ',t)
    t=re.sub(r'r\$\s*[\d.,]+',' VALOR ',t);t=re.sub(r'\b\d+%',' PCT ',t)
    t=re.sub(r'[^\w\s]',' ',t);t=re.sub(r'\s+',' ',t).strip()
    return ' '.join(sorted(w for w in t.split() if w not in _RUIDO_NORM and (len(w)>2 or "attr_" in w)))
def _sim(a,b):
    if not a or not b:return 0.0
    if min(len(a),len(b))/max(len(a),len(b))<0.7:return 0.0
    return SequenceMatcher(None,a,b).ratio()
def _reativacao(t:str)->bool:return bool(re.search(r'voltou|reativado|dispon[ií]vel novamente|estoque',t,re.I))

def deve_enviar(plat:str,cupom:str,texto:str,mapa_links:dict=None)->bool:
    try:
        mapa_links=mapa_links or {}
        cupons=_cupons(texto);alma=_alma(texto);benef=_benef(texto)
        asin=_extrair_asin_texto(texto,mapa_links) if plat=="amazon" else ""
        id_mgl=_extrair_id_magalu(texto,mapa_links) if plat=="magalu" else ""

        if _reativacao(texto):
            fp_re=_hash(f"reativ_{plat}_{'|'.join(sorted(cupons))}_{asin}_{id_mgl}")
            if db_get_dedupe(fp_re):log_dedup.info("🔁 [REATIVAÇÃO BLOQ]");return False
            db_set_dedupe(fp_re,plat,list(cupons),alma,"reativ",asin,id_mgl,list(benef));return True

        if asin:
            fp=f"amz_{asin}"
            if db_get_dedupe(fp):log_dedup.info(f"🔁 [AMZ-ASIN] {asin}");return False
            db_set_dedupe(fp,plat,list(cupons),alma,"amz",asin,"",list(benef));return True

        if id_mgl:
            fp=f"mgl_{id_mgl}"
            if db_get_dedupe(fp):log_dedup.info(f"🔁 [MGL-ID]");return False
            db_set_dedupe(fp,plat,list(cupons),alma,"mgl","",id_mgl,list(benef));return True

        if cupons:
            chave_prod=asin or id_mgl or "";fp_cup=_hash(f"{plat}|cup|{'|'.join(sorted(cupons))}|{chave_prod}")
            if db_get_dedupe(fp_cup):log_dedup.info(f"🔁 [CUPOM BLOQ] {plat}");return False

        if benef:
            chave_prod=asin or id_mgl or "";fp_ben=_hash(f"{plat}|ben|{'|'.join(sorted(benef))}|{chave_prod}")
            if db_get_dedupe(fp_ben):log_dedup.info("🔁 [BENEF BLOQ]");return False

        for e in db_buscar_dedupe_janela_rapida(plat):
            sim=_sim(alma,e.get("alma",""))
            if (cupons and cupons & set(e.get("cupons",[])) and sim>_SIM_MEDIO) or sim>_SIM_FORTE:
                log_dedup.info(f"🔁 [SIM {sim:.2f}] {plat}");return False

        fp_final=_hash(f"{plat}|{alma}|{'|'.join(sorted(cupons))}|{'|'.join(sorted(benef))}")
        db_set_dedupe(fp_final,plat,list(cupons),alma,"gen",asin,id_mgl,list(benef))
        if cupons:db_set_dedupe(fp_cup,plat,list(cupons),alma,"cup",asin,id_mgl,list(benef))
        if benef:db_set_dedupe(fp_ben,plat,list(cupons),alma,"ben",asin,id_mgl,list(benef))
        return True

    except Exception as e:
        log_dedup.error(f"❌ ERRO DEDUPE: {e}");return True


# ── MÓDULO 16: IMAGEM ─────────────────────────────────────────────────────────
# CRÍTICO: preparar_imagem recebe event.message INTEIRO (não .media)
async def preparar_imagem(fonte, eh_midia_telegram: bool) -> tuple:
    if eh_midia_telegram:
        try:
            buf = io.BytesIO()
            resultado = await client.download_media(fonte,file=buf)
            if resultado is None: log_img.warning("⚠️ download_media None"); return None,None
            buf.seek(0)
            if buf.getbuffer().nbytes < 500: return None,None
            buf.name = "imagem.jpg"; log_img.debug(f"✅ Mídia TG | {buf.getbuffer().nbytes:,}b"); return buf,None
        except Exception as e: log_img.warning(f"⚠️ download_media: {e}"); return None,None
    if isinstance(fonte,str):
        if fonte.startswith("http"):
            try:
                async with aiohttp.ClientSession(headers={"User-Agent":random.choice(USER_AGENTS)}) as s:
                    async with s.get(fonte,timeout=aiohttp.ClientTimeout(total=20),allow_redirects=True) as r:
                        if r.status == 200:
                            data = await r.read()
                            if len(data) < 1000: return None,None
                            buf = io.BytesIO(data); buf.name = "produto.jpg"; return buf,None
            except Exception as e: log_img.warning(f"⚠️ URL: {e}"); return None,None
        if os.path.exists(fonte): return fonte,None
    return None,None

async def buscar_imagem(url: str) -> Optional[str]:
    if not url or not url.startswith("http"): return None
    hdrs = {"User-Agent":random.choice(USER_AGENTS),"Accept":"text/html,*/*;q=0.9"}
    for t in range(1,4):
        try:
            async with aiohttp.ClientSession(headers=hdrs) as s:
                async with s.get(url,allow_redirects=True,timeout=aiohttp.ClientTimeout(total=15)) as r:
                    ct = r.headers.get("content-type","")
                    if "image" in ct: return str(r.url)
                    html = await r.text(errors="ignore"); soup = BeautifulSoup(html,"html.parser")
                    for attr in [{"property":"og:image"},{"property":"og:image:secure_url"},{"name":"twitter:image"}]:
                        tag = soup.find("meta",attrs=attr)
                        if not tag: continue
                        img_url = tag.get("content","")
                        if not img_url.startswith("http"): continue
                        img_url = re.sub(r'[?&](?:width|height|w|h|size|resize|fit|quality|q|maxwidth|maxheight|format|auto|compress|crop|scale)=[^&]+','',img_url).rstrip('?&')
                        log_img.info(f"✅ og:image t={t}: {img_url[:70]}"); return img_url
                    for scr in soup.find_all("script",type="application/ld+json"):
                        try:
                            data = json.loads(scr.string or ""); items = data if isinstance(data,list) else [data]
                            for item in items:
                                img = item.get("image")
                                if isinstance(img,str) and img.startswith("http"): return img
                                if isinstance(img,list) and img:
                                    c = img[0]
                                    if isinstance(c,str): return c
                                    if isinstance(c,dict):
                                        u = c.get("url","")
                                        if u.startswith("http"): return u
                        except Exception: pass
                    melhor_src = None; melhor_area = 0
                    for img_tag in soup.find_all("img",src=True):
                        src = img_tag.get("src","")
                        if not src.startswith("http"): continue
                        if any(x in src.lower() for x in ["icon","logo","avatar","badge","spinner"]): continue
                        try:
                            w = int(img_tag.get("width",0)); h = int(img_tag.get("height",0)); area = w*h
                            if area > melhor_area: melhor_area = area; melhor_src = src
                        except (ValueError,TypeError):
                            if any(x in src.lower() for x in ["product","produto","item","image","foto","zoom","large","xl","hd","original"]):
                                if not melhor_src: melhor_src = src
                    if melhor_src: return melhor_src
        except asyncio.TimeoutError: log_img.warning(f"⏱ Timeout t={t}/3")
        except Exception as e: log_img.warning(f"⚠️ t={t}/3: {e}")
        if t < 3: await asyncio.sleep(1.0)
    return None

# ── MÓDULO 17: RATE-LIMIT ─────────────────────────────────────────────────────
_RATE_LOCK: asyncio.Lock = None  # type: ignore
_ULTIMO_ENV_TS = 0.0

def _intervalo_atual() -> float: return 0.5 if 8 <= int(time.strftime("%H")) < 22 else 1.0

async def _rate_limit():
    global _ULTIMO_ENV_TS
    async with _RATE_LOCK:
        agora = time.monotonic(); espera = _intervalo_atual() - (agora - _ULTIMO_ENV_TS)
        if espera > 0: await asyncio.sleep(espera)
        _ULTIMO_ENV_TS = time.monotonic()

# ── MÓDULO 18: ANTI-LOOP ──────────────────────────────────────────────────────
_IDS_PROC: set = set()
_IDS_LOCK: asyncio.Lock = None  # type: ignore

async def _marcar(msg_id: int):
    async with _IDS_LOCK:
        _IDS_PROC.add(msg_id)
        if len(_IDS_PROC) > 5000:
            for _ in range(len(_IDS_PROC)-4000): _IDS_PROC.pop()

async def _foi_processado(msg_id: int) -> bool:
    async with _IDS_LOCK: return msg_id in _IDS_PROC

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO 19 ▸ ENVIO LIMPO
# Imagem Amazon (_IMG_AMZ): SOMENTE cupom exclusivo Amazon
# Isolamento por plataforma — zero contaminação cruzada
# ══════════════════════════════════════════════════════════════════════════════

def _tem_midia(media) -> bool:
    return media is not None and not isinstance(media, MessageMediaWebPage)


def _eh_cupom_texto(texto: str) -> bool:
    return bool(_KW_CUPOM.search(texto))


def _eh_cupom_exclusivo_amazon(texto: str, plat: str) -> bool:
    """
    Retorna True SOMENTE quando:
    - Plataforma é Amazon E
    - Texto é um cupom (tem 'cupom', 'código', 'off', etc.) E
    - NÃO menciona Shopee ou Magalu (garante exclusividade)
    """
    if plat != "amazon":
        return False
    if not _eh_cupom_texto(texto):
        return False
    # Garante que não é multi-plataforma
    tl = texto.lower()
    if "shopee" in tl or "magalu" in tl or "magazine" in tl:
        return False
    return True


async def _enviar(msg: str, img_obj) -> object:
    """Envio limpo. Nunca forward. Sempre constrói do zero."""
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

    return await client.send_message(
        GRUPO_DESTINO, msg,
        parse_mode="md", link_preview=True)

# ── MÓDULO 20: SQLITE ─────────────────────────────────────────────────────────
_DB_PATH = "foguetao.db"; _db_conn: Optional[sqlite3.Connection] = None; _db_lock = Lock()
TTL_SCHEDULER = 30*86400

def _init_db():
    global _db_conn
    _db_conn = sqlite3.connect(_DB_PATH,check_same_thread=False,timeout=10,isolation_level=None)
    for pragma in ["PRAGMA journal_mode=WAL","PRAGMA synchronous=NORMAL","PRAGMA cache_size=-16000","PRAGMA temp_store=MEMORY"]:
        _db_conn.execute(pragma)
    _db_conn.executescript("""
        CREATE TABLE IF NOT EXISTS links_cache(url_orig TEXT PRIMARY KEY,url_conv TEXT NOT NULL,plat TEXT NOT NULL,ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dedupe_temp(fp TEXT PRIMARY KEY,plat TEXT NOT NULL,cupons TEXT,alma TEXT,camp TEXT,asin TEXT,id_prod TEXT,benef TEXT,ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS saturacao(id INTEGER PRIMARY KEY AUTOINCREMENT,plat TEXT NOT NULL,sku TEXT,ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS scheduler(id INTEGER PRIMARY KEY AUTOINCREMENT,plat TEXT NOT NULL,hora INTEGER NOT NULL,score REAL DEFAULT 1.0,ts REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_lc_plat ON links_cache(plat);
        CREATE INDEX IF NOT EXISTS idx_dt_plat ON dedupe_temp(plat,ts);
        CREATE INDEX IF NOT EXISTS idx_dt_asin ON dedupe_temp(asin);
        CREATE INDEX IF NOT EXISTS idx_dt_id ON dedupe_temp(id_prod);
        CREATE INDEX IF NOT EXISTS idx_sat ON saturacao(plat,ts);
        CREATE INDEX IF NOT EXISTS idx_sch ON scheduler(plat,hora);
    """)
    for tabela,col,tipo in [("dedupe_temp","benef","TEXT"),("dedupe_temp","asin","TEXT"),("dedupe_temp","id_prod","TEXT")]:
        try: _db_conn.execute(f"ALTER TABLE {tabela} ADD COLUMN {col} {tipo}")
        except sqlite3.OperationalError: pass
    log_sys.info(f"🗄 DB ON | {_DB_PATH}")

@contextmanager
def _db():
    with _db_lock:
        try: yield _db_conn
        except sqlite3.Error as e: log_sys.error(f"❌ DB: {e}"); raise

def db_get_link(url_orig: str) -> Optional[str]:
    try:
        with _db() as db: row = db.execute("SELECT url_conv FROM links_cache WHERE url_orig=?",(url_orig,)).fetchone()
        return row[0] if row else None
    except Exception as e: log_sys.error(f"❌ db_get_link: {e}"); return None

def db_set_link(url_orig: str, url_conv: str, plat: str):
    try:
        with _db() as db: db.execute("INSERT OR REPLACE INTO links_cache(url_orig,url_conv,plat,ts)VALUES(?,?,?,?)",(url_orig,url_conv,plat,time.time()))
    except Exception as e: log_sys.error(f"❌ db_set_link: {e}")

def db_get_dedupe(fp: str) -> Optional[dict]:
    try:
        limite = time.time()-TTL_DEDUPE
        with _db() as db: row = db.execute("SELECT plat,cupons,alma,camp,asin,id_prod,benef,ts FROM dedupe_temp WHERE fp=? AND ts>=?",(fp,limite)).fetchone()
        if row: return {"plat":row[0],"cupons":json.loads(row[1] or "[]"),"alma":row[2],"camp":row[3],"asin":row[4] or "","id_prod":row[5] or "","benef":json.loads(row[6] or "[]"),"ts":row[7]}
    except Exception as e: log_sys.error(f"❌ db_get_dedupe: {e}")
    return None

def db_set_dedupe(fp: str, plat: str, cupons: list, alma: str, camp: str, asin: str="", id_prod: str="", benef: list=None):
    try:
        with _db() as db: db.execute("INSERT OR REPLACE INTO dedupe_temp(fp,plat,cupons,alma,camp,asin,id_prod,benef,ts)VALUES(?,?,?,?,?,?,?,?,?)",(fp,plat,json.dumps(cupons or []),alma or "",camp or "geral",asin or "",id_prod or "",json.dumps(benef or []),time.time()))
    except Exception as e: log_sys.error(f"❌ db_set_dedupe: {e}")

def db_buscar_dedupe_por_asin(asin: str, plat: str) -> list:
    if not asin: return []
    try:
        limite = time.time()-TTL_DEDUPE
        with _db() as db: rows = db.execute("SELECT fp,cupons,alma,ts FROM dedupe_temp WHERE asin=? AND plat=? AND ts>=? ORDER BY ts DESC LIMIT 10",(asin,plat,limite)).fetchall()
        return [{"fp":r[0],"cupons":json.loads(r[1] or "[]"),"alma":r[2] or "","ts":r[3]} for r in rows]
    except Exception as e: log_sys.error(f"❌ db_buscar_asin: {e}"); return []

def db_buscar_dedupe_por_id(id_prod: str, plat: str) -> list:
    if not id_prod: return []
    try:
        limite = time.time()-TTL_DEDUPE
        with _db() as db: rows = db.execute("SELECT fp,cupons,alma,ts FROM dedupe_temp WHERE id_prod=? AND plat=? AND ts>=? ORDER BY ts DESC LIMIT 10",(id_prod,plat,limite)).fetchall()
        return [{"fp":r[0],"cupons":json.loads(r[1] or "[]"),"alma":r[2] or "","ts":r[3]} for r in rows]
    except Exception as e: log_sys.error(f"❌ db_buscar_id: {e}"); return []

def db_buscar_dedupe_janela_rapida(plat: str) -> list:
    try:
        limite = time.time()-JANELA_RAPIDA
        with _db() as db: rows = db.execute("SELECT fp,cupons,alma,asin,id_prod,benef,ts FROM dedupe_temp WHERE plat=? AND ts>=? ORDER BY ts DESC",(plat,limite)).fetchall()
        return [{"fp":r[0],"cupons":json.loads(r[1] or "[]"),"alma":r[2] or "","asin":r[3] or "","id_prod":r[4] or "","benef":json.loads(r[5] or "[]"),"ts":r[6]} for r in rows]
    except Exception as e: log_sys.error(f"❌ db_janela: {e}"); return []

def db_registrar_sat(plat: str, sku: str=""):
    try:
        with _db() as db: db.execute("INSERT INTO saturacao(plat,sku,ts)VALUES(?,?,?)",(plat,sku or "",time.time()))
    except Exception as e: log_sys.error(f"❌ db_sat: {e}")

def db_count_sat(plat: str, janela: float=1800) -> int:
    try:
        limite = time.time()-janela
        with _db() as db: row = db.execute("SELECT COUNT(*) FROM saturacao WHERE plat=? AND ts>=?",(plat,limite)).fetchone()
        return row[0] if row else 0
    except Exception as e: log_sys.error(f"❌ db_count_sat: {e}"); return 0

def db_registrar_sch(plat: str, hora: int, score: float=1.0):
    try:
        with _db() as db: db.execute("INSERT INTO scheduler(plat,hora,score,ts)VALUES(?,?,?,?)",(plat,hora,score,time.time()))
    except Exception as e: log_sys.error(f"❌ db_sch: {e}")

def db_score_hora(plat: str, hora: int, dias: int=7) -> float:
    try:
        limite = time.time()-dias*86400
        with _db() as db: row = db.execute("SELECT AVG(score) FROM scheduler WHERE plat=? AND hora=? AND ts>=?",(plat,hora,limite)).fetchone()
        return float(row[0] or 1.0)
    except Exception as e: log_sys.error(f"❌ db_score: {e}"); return 1.0

def db_limpar():
    """Apaga SOMENTE dados temporários. links_cache NUNCA é apagado automaticamente."""
    try:
        agora = time.time()
        with _db() as db:
            # links_cache: NUNCA apagado — economiza chamadas de API
            db.execute("DELETE FROM dedupe_temp WHERE ts<?",(agora-TTL_DEDUPE,))
            db.execute("DELETE FROM saturacao WHERE ts<?",(agora-TTL_DEDUPE,))
            db.execute("DELETE FROM scheduler WHERE ts<?",(agora-TTL_SCHEDULER,))
        log_sys.debug("🗑 Limpeza temp OK (links_cache preservado)")
    except Exception as e: log_sys.error(f"❌ db_limpar: {e}")

# ── MÓDULO 21: PARSER DE LINKS ────────────────────────────────────────────────
@dataclass
class ParsedLink:
    url_original: str; url_limpa: str; plat: str; tipo: str; sku: str; valido: bool; motivo: str

_TRACKING = frozenset({"tag","ref","ref_","smid","sprefix","sr","spla","dchild","linkcode","linkid","camp","creative","pf_rd_p","pf_rd_r","pd_rd_wg","pd_rd_w","content-id","pd_rd_r","pd_rd_i","ie","qid","_encoding","dib","dib_tag","m","th","psc","ingress","visitid","s","ascsubtag","btn_ref","af_siteid","pid","af_click_lookback","is_retargeting","deep_link_value","af_dp","utm_source","utm_medium","utm_campaign","utm_term","utm_content","partner_id","promoter_id","af_force_deeplink","c","isretargeting","fbclid","gclid","msclkid","mc_eid","_ga","_gl"})
_P_SHP = [re.compile(r'/product/(\d+)/(\d+)'),re.compile(r'/i\.(\d+)\.(\d+)')]
_P_MGL = re.compile(r'/(?:[^/]+/)?p/([a-z0-9]{6,})/?',re.I)

def _sem_tracking(p) -> str:
    params = {k:v[0] for k,v in parse_qs(p.query,keep_blank_values=False).items() if k.lower() not in _TRACKING}
    return urlunparse(p._replace(query=urlencode(params) if params else "",fragment=""))

def parse_link(url: str) -> ParsedLink:
    url = url.strip().rstrip('.,;)>').replace(' ','%20')
    url = re.sub(r'%25([0-9A-Fa-f]{2})',r'%\1',url)
    if not url.startswith(("http://","https://")): return ParsedLink(url,url,"desconhecido","desconhecido","",False,"sem esquema")
    try: p = urlparse(url)
    except Exception as e: return ParsedLink(url,url,"desconhecido","desconhecido","",False,str(e))
    nl = p.netloc.lower().replace("www.",""); plat = classificar(url)
    if plat in (None,"preservar"): return ParsedLink(url,url,plat or "desconhecido","desconhecido","",plat=="preservar","")
    if plat == "expandir": return ParsedLink(url,_sem_tracking(p),"expandir","encurtado","",True,"")
    url_limpa = _sem_tracking(p)
    if plat == "amazon":
        asin = _extrair_asin(url)
        tipo = "produto" if asin else "busca" if re.search(r'/s[/?]|/deals|/b[/?]',p.path) else "evento" if re.search(r'/events/|/stores/',p.path) else "desconhecido"
        return ParsedLink(url,url_limpa,plat,tipo,asin or "",True,"")
    if plat == "shopee":
        sku = ""
        for pat in _P_SHP:
            m = pat.search(p.path+"?"+p.query)
            if m: sku = f"{m.group(1)}.{m.group(2)}"; break
        return ParsedLink(url,url_limpa,plat,"produto" if sku else "busca",sku,True,"")
    if plat == "magalu":
        sku = ""; m = _P_MGL.search(p.path)
        if m: sku = m.group(1)
        if "sacola" in nl and not p.path.strip("/"): return ParsedLink(url,url_limpa,plat,"invalido",sku,False,"sacola")
        tipo = "produto" if sku else "lista" if "/l/" in p.path else "selecao" if "/selecao/" in p.path else "desconhecido"
        return ParsedLink(url,url_limpa,plat,tipo,sku,True,"")
    return ParsedLink(url,url_limpa,"desconhecido","desconhecido","",False,"plat não mapeada")

def parse_links_bulk(urls: List[str]) -> List[ParsedLink]:
    res = [parse_link(u) for u in urls]; validos = [r for r in res if r.valido]
    log_lnk.info(f"🔍 Parser {len(validos)}/{len(urls)}"); return validos

# ── MÓDULO 22: ANTI-SATURAÇÃO ─────────────────────────────────────────────────
_SAT_MAX_PLAT = 10; _SAT_BURST_LIM = 6; _SAT_BURST_JAN = 60
_burst: List[float] = []
_burst_lock: asyncio.Lock = None  # type: ignore

async def _burst_add():
    async with _burst_lock:
        agora = time.monotonic(); _burst.append(agora)
        while _burst and agora-_burst[0] > _SAT_BURST_JAN: _burst.pop(0)

async def _burst_count() -> int:
    async with _burst_lock: agora = time.monotonic(); return sum(1 for t in _burst if agora-t <= _SAT_BURST_JAN)

async def antisaturacao_gate(plat: str, texto: str) -> float:
    if _KW_EVENTO.search(texto): return 0.0
    delay = 0.0
    if db_count_sat(plat) >= _SAT_MAX_PLAT: delay += 6.0
    if await _burst_count() >= _SAT_BURST_LIM: delay += 4.0
    return delay

def antisaturacao_ok(plat: str, sku: str): db_registrar_sat(plat,sku)

# ── MÓDULO 23: ORCHESTRATOR + PIPELINE ───────────────────────────────────────
# _init_globals() definida aqui no escopo global — ANTES de qualquer uso.
# Resolve definitivamente o NameError '_init_globals is not defined'.
_WORKERS_MAX = 4; _FILA_MAX = 200; _COALESCE_MS = 800
_buf:      list             = []
_buf_lck:  asyncio.Lock     = None  # type: ignore
_buf_evt:  asyncio.Event    = None  # type: ignore
_w_ativos: int              = 0
_w_lck:    asyncio.Lock     = None  # type: ignore
_coal:     dict             = {}

def _init_globals():
    """Cria objetos asyncio no event loop correto. DEVE ser chamada como primeira linha de _run()."""
    global _buf_lck,_buf_evt,_w_lck,_buf,_coal,_w_ativos
    global _RATE_LOCK,_IDS_LOCK,_sch_cnt_lock,_burst_lock
    _buf = []; _coal = {}; _w_ativos = 0
    _buf_lck = asyncio.Lock(); _buf_evt = asyncio.Event(); _w_lck = asyncio.Lock()
    _RATE_LOCK = asyncio.Lock(); _IDS_LOCK = asyncio.Lock()
    _sch_cnt_lock = asyncio.Lock(); _burst_lock = asyncio.Lock()
    log_sys.debug("🔧 _init_globals OK")

def _prio(texto: str) -> int:
    tl = texto.lower()
    if "amazon" in tl: return 1
    if "shopee" in tl: return 2
    if "magalu" in tl: return 3
    return 9

def _fp_r(texto: str) -> str: return hashlib.sha256(re.sub(r'\s+','',texto.lower())[:80].encode()).hexdigest()[:12]

def _tem_contexto(texto: str) -> bool:
    linhas = [l.strip() for l in texto.splitlines() if l.strip() and not re.match(r'https?://',l.strip())]
    if not linhas: return False
    total = " ".join(linhas)
    for ind in [r'off',r'%',r'r\$',r'cupom',r'desconto',r'promoção',r'oferta',r'grátis',r'evento',r'live',r'relâmpago',r'flash',r'volta',r'normalizou',r'a\s+partir',r'ativo',r'pix']:
        if re.search(ind,total,re.I): return True
    return len(total) > 20

async def _enfileirar(event, is_edit: bool):
    texto = event.message.text or ""
    if not texto.strip(): return
    fp = _fp_r(texto); agora = time.monotonic()
    async with _buf_lck:
        if not is_edit and agora-_coal.get(fp,0.0) < _COALESCE_MS/1000: return
        _coal[fp] = agora
        if len(_buf) >= _FILA_MAX: log_sys.warning(f"⚠️ Fila cheia | id={event.message.id}"); return
        heapq.heappush(_buf,(0 if is_edit else _prio(texto),agora,event,is_edit))
    _buf_evt.set()

async def _worker_loop():
    global _w_ativos
    while True:
        await _buf_evt.wait()
        while True:
            item = None
            async with _buf_lck:
                if _buf: item = heapq.heappop(_buf)
                else: _buf_evt.clear(); break
            if item is None: break
            prio,ts,event,is_edit = item
            async with _w_lck:
                if _w_ativos >= _WORKERS_MAX:
                    async with _buf_lck: heapq.heappush(_buf,item); _buf_evt.set()
                    await asyncio.sleep(0.2); break
                _w_ativos += 1
            try:
                if time.monotonic()-ts > 60: log_sys.warning(f"⏱ Expirado | id={event.message.id}"); continue
                await _pipeline(event,is_edit)
            except Exception as e: log_sys.error(f"❌ Worker: {e}",exc_info=True)
            finally:
                async with _w_lck: _w_ativos -= 1

async def _pipeline(event, is_edit: bool = False):
    msg_id = event.message.id; texto = event.message.text or ""
    try: chat = await event.get_chat(); uname = (chat.username or str(event.chat_id)).lower()
    except Exception as e: log_sys.error(f"❌ get_chat: {e}"); return
    log_tg.info(f"{'✏️' if is_edit else '📩'} @{uname} | id={msg_id} | {len(texto)}c | q={len(_buf)} w={_w_ativos}")
    if not texto.strip(): return
    try:
        if not is_edit:
            if await _foi_processado(msg_id): return
        else:
            loop = asyncio.get_event_loop()
            mapa_c = await loop.run_in_executor(_EXECUTOR,ler_mapa)
            if str(msg_id) not in mapa_c: return
    except Exception as e: log_sys.error(f"❌ anti-loop: {e}"); return
    try:
        if texto_bloqueado(texto): return
    except Exception as e: log_sys.error(f"❌ filtro: {e}"); return
    try: tc = limpar_ruido_textual(texto)
    except Exception as e: log_sys.error(f"❌ limpeza: {e}"); tc = texto
    if not _tem_contexto(tc): return
    try:
        links_c,links_p = extrair_links(tc); parsed = parse_links_bulk(links_c)
        diretos     = [r.url_limpa for r in parsed if r.plat not in ("expandir","desconhecido")]
        expandir_lst = [r.url_limpa for r in parsed if r.plat == "expandir"]
    except Exception as e: log_sys.error(f"❌ extração: {e}"); return
    if not diretos and not expandir_lst and not links_p:
        if "fadadoscupons" not in uname: return
    try: mapa,plat = await converter_links(diretos+expandir_lst)
    except Exception as e: log_sys.error(f"❌ converter: {e}"); mapa,plat = {},"amazon"
    if links_c and not mapa and not links_p: log_sys.warning(f"🚫 Zero links | @{uname}"); return
    try:
        sku = next((f"{r.plat[:3]}_{r.sku}" for r in parsed if r.sku), "") or _extrair_sku(tc, mapa)
        cup = _extrair_cupom(tc)
except Exception as e:
        log_sys.error(f"❌ sku: {e}")
        sku, cup = "", ""

    if not is_edit:
        try:
            delay_sat = await antisaturacao_gate(plat, tc)
        except Exception as e:
            log_sys.error(f"❌ antisaturacao: {e}")
            delay_sat = 0.0

        try:
            if not deve_enviar(plat, cup, tc, mapa): return
        except Exception as e:
            log_sys.error(f"❌ deve_enviar: {e}")
            return

        # Ajuste do scheduler removido
        delay_sch = 0.0
        total = delay_sat + delay_sch
        if total > 0:
            await asyncio.sleep(total)

    try:
        msg_final = renderizar(tc, mapa, links_p, plat)
    except Exception as e:
        log_sys.error(f"❌ renderizar: {e}")
        return

    # ══════════════════════════════════════════════════════════════════════
    # IMAGEM — ordem de prioridade + isolamento por plataforma
    # ══════════════════════════════════════════════════════════════════════
    tem_img = _tem_midia(event.message.media)
    img     = None
    eh_cup  = _eh_cupom_texto(tc)

    # 1. Imagem original da mensagem (sempre prioridade máxima)
    if tem_img:
        try:
            img, _ = await preparar_imagem(event.message, True)
            if img:
                log_img.debug("✅ Imagem original")
            else:
                log_img.warning("⚠️ download_media retornou None")
        except Exception as e:
            log_img.warning(f"⚠️ Imagem original: {e}")
            img = None

    # 2. Sem imagem original → busca ou fallback
    if img is None:

        if mapa and not eh_cup:
            # Produto sem imagem → busca na página
            try:
                img_url = await buscar_imagem(list(mapa.values())[0])
                if img_url:
                    img, _ = await preparar_imagem(img_url, False)
            except Exception as e:
                log_img.warning(f"⚠️ Busca imagem: {e}")

        if img is None and eh_cup:
            # Fallback exclusivo por plataforma
            # _IMG_AMZ: SOMENTE cupom exclusivo Amazon
            if plat == "shopee" and os.path.exists(_IMG_SHP):
                img = _IMG_SHP
                log_img.debug("🟣 Fallback Shopee")
            elif _eh_cupom_exclusivo_amazon(tc, plat) and os.path.exists(_IMG_AMZ):
                img = _IMG_AMZ
                log_img.debug("🟠 Fallback Amazon cupom exclusivo")
            elif plat == "magalu" and os.path.exists(_IMG_MGL):
                img = _IMG_MGL
                log_img.debug("🔵 Fallback Magalu")

    await _rate_limit()
    async with _SEM_ENVIO:
        loop = asyncio.get_event_loop()
        try: mp = await loop.run_in_executor(_EXECUTOR,ler_mapa)
        except Exception: mp = {}
        try:
            if is_edit:
                id_d = mp.get(str(msg_id))
                if not id_d: return
                for t in range(1,4):
                    try: await client.edit_message(GRUPO_DESTINO,id_d,msg_final,parse_mode="md"); log_tg.info(f"✏️ Editado | {id_d}"); break
                    except MessageNotModifiedError: break
                    except FloodWaitError as e: await asyncio.sleep(e.seconds)
                    except Exception as e:
                        log_tg.error(f"❌ edit t={t}: {e}")
                        if t < 3: await asyncio.sleep(2**t)
                return
            sent = None
            for t in range(1,4):
                try: sent = await _enviar(msg_final,img); break
                except FloodWaitError as e: await asyncio.sleep(e.seconds)
                except Exception as e:
                    log_tg.error(f"❌ envio t={t}: {e}")
                    if t == 1: img = None
                    elif t < 3: await asyncio.sleep(2**t)
            if sent:
                mp[str(msg_id)] = sent.id
                try: 
                    await loop.run_in_executor(_EXECUTOR, salvar_mapa, mp)
                except Exception as e: 
                    log_sys.error(f"❌ salvar_mapa: {e}")
                
                await _marcar(msg_id)  # Marcar como processado
                
                # O scheduler foi removido, então não tem nada aqui.

                try: 
                    antisaturacao_ok(plat, sku)
                except Exception: 
                    pass
                
                try: 
                    await _burst_add()
                except Exception: 
                    pass
                
                if plat == "magalu" and mapa:
                    for url_orig, url_conv in mapa.items():
                    
                        if "partner_id" in url_conv and "cutt.ly" not in url_conv:
                            try: await _agendar_edicao_magalu(url_conv,msg_id)
                            except Exception: pass
                log_sys.info(f"🚀 [OK] @{uname}→{GRUPO_DESTINO} | {msg_id}→{sent.id} | {plat.upper()} sku={sku}")
            else: log_sys.error(f"❌ Envio falhou | @{uname}")
        except Exception as e: log_sys.error(f"❌ CRÍTICO: {e}",exc_info=True)

async def processar(event, is_edit: bool = False): await _enfileirar(event,is_edit)

async def _iniciar_orchestrator():
    log_sys.info(f"🎛 Orchestrator | workers={_WORKERS_MAX} fila={_FILA_MAX} coalesce={_COALESCE_MS}ms")
    asyncio.create_task(_worker_loop())

# ── MÓDULO 24: HEALTH CHECK ───────────────────────────────────────────────────
async def _health_check():
    while True:
        await asyncio.sleep(300)
        try:
            db_limpar()
            try:
                with _db() as db:
                    n_links = db.execute("SELECT COUNT(*) FROM links_cache").fetchone()[0]
                    n_dedup = db.execute("SELECT COUNT(*) FROM dedupe_temp").fetchone()[0]
                    n_sat   = db.execute("SELECT COUNT(*) FROM saturacao").fetchone()[0]
            except Exception: n_links = n_dedup = n_sat = "?"
            log_hc.info(f"💚 links={n_links}(perm) | dedupe={n_dedup} | sat={n_sat} | anti-loop={len(_IDS_PROC)} | fila={len(_buf)} w={_w_ativos} | PIL={'OK' if _PIL_OK else 'OFF'}")
        except Exception as e: log_hc.error(f"❌ Health: {e}",exc_info=True)

# ── MÓDULO 25: INICIALIZAÇÃO ──────────────────────────────────────────────────
client = TelegramClient(StringSession(SESSION_STRING),API_ID,API_HASH)

async def _run():
    _init_globals()  # 1. SEMPRE PRIMEIRO — cria locks no event loop correto
    _init_db()       # 2. SQLite
    log_sys.info("🔌 Conectando...")
    await client.connect()
    if not await client.is_user_authorized(): log_sys.error("❌ Sessão inválida"); return False
    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 {GRUPOS_ORIGEM} → {GRUPO_DESTINO}")
    log_sys.info(f"🟠 Amazon: {_AMZ_TAG} | 🟣 Shopee: {_SHP_APP_ID} | 🔵 Magalu: {_MGL_PROMOTER}/{_MGL_SLUG}")
    log_sys.info(f"🖼  Pillow: {'OK' if _PIL_OK else 'OFF'}")
    log_sys.info("🚀 FOGUETÃO v75.0 — ONLINE")

    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def on_new(event):
        try: await processar(event,is_edit=False)
        except Exception as e: log_sys.error(f"❌ on_new: {e}",exc_info=True)

    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def on_edit(event):
        try: await processar(event,is_edit=True)
        except Exception as e: log_sys.error(f"❌ on_edit: {e}",exc_info=True)

    asyncio.create_task(_health_check())
    await _iniciar_orchestrator()
    await client.run_until_disconnected()
    return True

async def main():
    while True:
        try: await _run()
        except (AuthKeyUnregisteredError,SessionPasswordNeededError) as e: log_sys.error(f"❌ Auth fatal: {e}"); break
        except Exception as e:
            log_sys.error(f"💥 Caiu: {e} — restart 15s",exc_info=True)
            try: await client.disconnect()
            except Exception: pass
            await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())

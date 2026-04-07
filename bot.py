import re, asyncio, hashlib, json, time, os, aiohttp
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaWebPage
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

# ============================================================
# 🔹 CONFIGURAÇÕES GERAIS
# ============================================================
API_ID = 33768893
API_HASH = '7959ea0392ff7f91b4f7e207e75a1813'
SESSION = '/tmp/sessao_leo_v28' # Railway safe

GRUPOS_OFERTAS = ['fumotom', 'promotom']
GRUPO_CUPONS_EXCLUSIVO = 'fadadoscupons'
GRUPOS_ORIGEM = GRUPOS_OFERTAS + [GRUPO_CUPONS_EXCLUSIVO]
GRUPO_DESTINO = '@ofertap'

# TAGS AFILIADOS
AMAZON_TAG      = "leo21073-20"
MAGALU_PARTNER  = "3440"
MAGALU_PROMOTER = "5479317"
ML_SOURCE       = "silvaleo20230518163534"
SHOPEE_APP_ID   = "18348480261"
SHOPEE_SECRET   = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"
BITLY_TOKEN     = "69cdfdea70096c9cf42a5eac20cb55b17668ede9"

# ARQUIVOS E LINKS FIXOS
MEU_LINK_SOCIAL_ML = "https://mercadolivre.com/sec/23NpLSc"
IMG_ML_FIXA = "mercado_livre_c1a918503a.jpg" 
ARQUIVO_CACHE = "cache_ofertas.json"

# SEMÁFORO (Bypass de Flood - Máximo 2 mensagens simultâneas)
envio_lock = asyncio.Semaphore(2)

# FILTRO COMPLETO
FILTRO = [
    "Monitor Samsung", "Fonte Mancer", "Placa de video", "Placa de Vídeo",
    "Monitor LG", "PC home Essential", "Suporte articulado",
    "Gabinetes em oferta", "Monitor Safe", "gabinete atx",
    "gabinete micro atx", "mid tower", "mini tower", "mesh",
    "airflow", "TGT", "Acegeek", "Reactor II VE",
    "Core Reactor VE", "Sledger", "Monitor gamer",
    "Gabinete Gamer", "Fonte Cooler", "Placa mãe",
    "VHAGAR", "Superframe", "AM5", "AM4",
    "water cooler", "GTX", "CL18", "CL16",
    "CL32", "MT/s", "MHz", "SO-DIMM",
    "DIMM", "DDR5", "DDR4", "Dram"
]

# ============================================================
# 🔹 SISTEMA DE CACHE (SUA LÓGICA DE 48H / 24H)
# ============================================================
TTL_OFERTA = 48 * 60 * 60
TTL_CUPOM = 24 * 60 * 60

def carregar_cache():
    if not os.path.exists(ARQUIVO_CACHE): return {"ofertas": {}, "cupons": {}, "mapeamento": {}}
    try:
        with open(ARQUIVO_CACHE, "r", encoding="utf-8") as f: return json.load(f)
    except: return {"ofertas": {}, "cupons": {}, "mapeamento": {}}

def salvar_cache(cache):
    with open(ARQUIVO_CACHE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def ja_foi_enviado(plataforma, produto_id, preco, cupom=""):
    cache = carregar_cache()
    agora = time.time()
    cache["ofertas"] = {k: v for k, v in cache.get("ofertas", {}).items() if agora - v < TTL_OFERTA}
    cache["cupons"] = {k: v for k, v in cache.get("cupons", {}).items() if agora - v < TTL_CUPOM}
    
    hash_o = hashlib.md5(f"{plataforma}|{produto_id}|{preco}|{cupom}".encode()).hexdigest()
    chave_c = f"{plataforma}|{cupom}".lower().strip()
    
    if hash_o in cache["ofertas"]: return True
    if cupom and chave_c in cache["cupons"]: return True
    return False

def marcar_como_enviado(plataforma, produto_id, preco, cupom=""):
    cache = carregar_cache()
    hash_o = hashlib.md5(f"{plataforma}|{produto_id}|{preco}|{cupom}".encode()).hexdigest()
    cache["ofertas"][hash_o] = time.time()
    if cupom: cache["cupons"][f"{plataforma}|{cupom}".lower().strip()] = time.time()
    salvar_cache(cache)

# ============================================================
# 🔹 UTILITÁRIOS (DESCURTADOR + IMAGEM 3X + BITLY)
# ============================================================

async def expandir_url(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, allow_redirects=True, timeout=12) as r:
                return str(r.url)
    except: return url

async def encurtar_bitly(url):
    async with aiohttp.ClientSession() as session:
        h = {"Authorization": f"Bearer {BITLY_TOKEN}"}
        try:
            async with session.post("https://api-ssl.bitly.com/v4/shorten", json={"long_url": url}, headers=h) as r:
                d = await r.json()
                return d.get("link", url)
        except: return url

async def buscar_imagem(url):
    """Busca imagem 3x ou retorna None para ativar preview."""
    async with aiohttp.ClientSession() as session:
        for _ in range(3):
            try:
                async with session.get(url, timeout=7) as r:
                    soup = BeautifulSoup(await r.text(), 'html.parser')
                    img = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "twitter:image"})
                    if img: return img['content']
            except: await asyncio.sleep(1)
    return None

# ============================================================
# 🔹 CONVERSORES (MULTILINK + LIMPEZA LIXO)
# ============================================================

async def converter_shopee(url):
    ts = str(int(time.time()))
    query = f'mutation {{ generateShortLink(input: {{ originUrl: "{url}" }}) {{ shortLink }} }}'
    payload = json.dumps({"query": query}, separators=(",", ":"))
    sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
    headers = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as s:
        try:
            async with s.post("https://open-api.affiliate.shopee.com.br/graphql", data=payload, headers=headers) as r:
                res = await r.json()
                return res["data"]["generateShortLink"]["shortLink"] if "data" in res and res["data"] else url
        except: return url

async def converter_link(url):
    url_l = url.lower()
    
    if "mercadolivre" in url_l or "meli.la" in url_l:
        if any(x in url_l for x in ["/sec/", "/lista/", "/lists/", "/social/"]): 
            return MEU_LINK_SOCIAL_ML, "ml", True
        url_ex = await expandir_url(url)
        q = parse_qs(urlparse(url_ex).query)
        q.update({"matt_tool": ["afiliados"], "matt_source": [ML_SOURCE], "matt_campaign": ["ofertap"]})
        return urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True))), "ml", False

    elif "amazon" in url_l or "amzn" in url_l:
        url_ex = await expandir_url(url)
        q = parse_qs(urlparse(url_ex).query)
        q.update({"tag": [AMAZON_TAG]})
        return urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True))), "amazon", False

    elif "magazineluiza" in url_l or "magalu" in url_l:
        q = parse_qs(urlparse(url).query)
        q.update({"partner_id": [MAGALU_PARTNER], "promoter_id": [MAGALU_PROMOTER]})
        l_url = urlunparse(urlparse(url)._replace(query=urlencode(q, doseq=True)))
        return await encurtar_bitly(l_url), "magalu", False

    elif "shopee" in url_l:
        return await converter_shopee(url), "shopee", False

    # LIMPEZA DE LIXO: Se não for loja, apaga o link, exceto se for cadastro/ganhe
    if any(x in url_l for x in ["cadastro", "ganhe", "promo", "formulario", "inscricao"]):
        return url, "info", False
        
    return None, None, False

# ============================================================
# 🔹 FORMATAÇÃO (EMOJIS ✅🔥🎟 + CRASES)
# ============================================================

def formatar_oferta(texto, links_conv):
    preco_m = re.search(r'R\$\s?\d+[.,\d]*', texto)
    preco = preco_m.group(0) if preco_m else ""
    cupom_m = re.search(r'(?i)cupom[:\s]+([A-Z0-9]{4,20})', texto)
    cupom = cupom_m.group(1) if cupom_m else ""
    
    linhas = texto.split('\n')
    titulo = re.sub(r'[✅🔥🎟🛒🚨]|https?://\S+', '', linhas[0]).strip()
    
    # Monta padrão limpo
    msg = f"✅ Produto: {titulo}\n"
    if preco: msg += f"🔥 Preço: {preco}\n"
    if cupom: msg += f"🎟 Cupom: `{cupom.upper()}`\n"
    msg += "\n" + "\n".join(links_conv)
    return msg, preco, cupom

# ============================================================
# 🔹 MOTOR DE PROCESSAMENTO (MENSAGEM E EDIÇÃO)
# ============================================================

async def processar(event, is_edit=False):
    raw_text = event.message.text or ""
    if not raw_text.strip(): return
    if any(p.lower() in raw_text.lower() for p in FILTRO): return

    chat = await event.get_chat()
    username = (chat.username or "").lower()
    links_brutos = re.findall(r'https?://\S+', raw_text)
    
    # Regra exclusiva fadadoscupons para mensagens de texto (sem link)
    if not links_brutos and username != GRUPO_CUPONS_EXCLUSIVO: return

    links_conv, usar_img_f, plat_p, p_id = [], False, "outro", "0"

    for link in links_brutos:
        novo, plat, img_f = await converter_link(link)
        if novo:
            links_conv.append(novo)
            if plat != "info": 
                plat_p = plat
                p_id = novo[-12:]
            if img_f: usar_img_f = True
    
    final_msg, preco, cupom = formatar_oferta(raw_text, links_conv)

    if not is_edit:
        if ja_foi_enviado(plat_p, p_id, preco, cupom): return
        marcar_como_enviado(plat_p, p_id, preco, cupom)

    # GESTÃO DE IMAGEM (ML Social/Lista, Mídia Original ou Raspagem)
    imagem = None
    if usar_img_f and os.path.exists(IMG_ML_FIXA): imagem = IMG_ML_FIXA
    elif event.message.media and not isinstance(event.message.media, MessageMediaWebPage): imagem = event.message.media
    elif links_conv: imagem = await buscar_imagem(links_conv[0])

    async with envio_lock: # BYPASS DE FLOOD
        try:
            cache = carregar_cache()
            mapping = cache.get("mapeamento", {})
            if is_edit and str(event.message.id) in mapping:
                await client.edit_message(GRUPO_DESTINO, mapping[str(event.message.id)], final_msg)
            else:
                if imagem:
                    if len(final_msg) > 1024:
                        f = await client.send_file(GRUPO_DESTINO, imagem)
                        sent = await client.send_message(GRUPO_DESTINO, final_msg, reply_to=f.id)
                    else: sent = await client.send_file(GRUPO_DESTINO, imagem, caption=final_msg)
                else: sent = await client.send_message(GRUPO_DESTINO, final_msg)
                
                if sent:
                    cache = carregar_cache()
                    cache["mapeamento"][str(event.message.id)] = sent.id
                    salvar_cache(cache)
        except Exception as e: print(f"❌ Erro: {e}")

# ============================================================
# 🔹 START
# ============================================================
client = TelegramClient(SESSION, API_ID, API_HASH)
@client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
async def h1(e): await processar(e)
@client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
async def h2(e): await processar(e, is_edit=True)

print("🚀 BOT DEFINITIVO v28.0 ONLINE!")
client.start()
client.run_until_disconnected()

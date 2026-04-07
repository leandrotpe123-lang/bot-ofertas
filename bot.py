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
SESSION = 'sessao_leo_v22'

# Canais de Origem e Destino
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
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

# 🔹 FILTRO DE PALAVRAS BLOQUEADAS (Recolocado!)
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
# 🔹 SUA LÓGICA DE CACHE (EXATAMENTE COMO VOCÊ MANDOU)
# ============================================================
ARQUIVO_CACHE = "cache_ofertas.json"
TTL_OFERTA = 48 * 60 * 60   # 48 horas
TTL_CUPOM = 24 * 60 * 60    # 24 horas

def carregar_cache():
    if not os.path.exists(ARQUIVO_CACHE): return {"ofertas": {}, "cupons": {}}
    try:
        with open(ARQUIVO_CACHE, "r", encoding="utf-8") as f: return json.load(f)
    except: return {"ofertas": {}, "cupons": {}}

def salvar_cache(cache):
    with open(ARQUIVO_CACHE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def limpar_expirados(cache):
    agora = time.time()
    cache["ofertas"] = {k: v for k, v in cache["ofertas"].items() if agora - v < TTL_OFERTA}
    cache["cupons"] = {k: v for k, v in cache["cupons"].items() if agora - v < TTL_CUPOM}
    return cache

def gerar_hash(plataforma, produto_id, preco, cupom=""):
    chave = f"{plataforma}|{produto_id}|{preco}|{cupom}"
    return hashlib.md5(chave.encode()).hexdigest()

def ja_foi_enviado(plataforma, produto_id, preco, cupom=""):
    cache = carregar_cache()
    cache = limpar_expirados(cache)
    hash_oferta = gerar_hash(plataforma, produto_id, preco, cupom)
    chave_cupom = f"{plataforma}|{cupom}".lower().strip()
    if hash_oferta in cache["ofertas"]: return True
    if cupom and chave_cupom in cache["cupons"]: return True
    return False

def marcar_como_enviado(plataforma, produto_id, preco, cupom=""):
    cache = carregar_cache()
    cache = limpar_expirados(cache)
    agora = time.time()
    hash_oferta = gerar_hash(plataforma, produto_id, preco, cupom)
    cache["ofertas"][hash_oferta] = agora
    if cupom:
        chave_cupom = f"{plataforma}|{cupom}".lower().strip()
        cache["cupons"][chave_cupom] = agora
    salvar_cache(cache)

# ============================================================
# 🔹 UTILITÁRIOS (ASYNC, IMAGEM, EDIÇÃO)
# ============================================================

async def expandir_url(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.get(url, allow_redirects=True, timeout=12) as r:
                return str(r.url)
        except: return url

def extrair_id(url):
    url = url.split('?')[0]
    amz = re.search(r'/dp/([A-Z0-9]{10})', url)
    if amz: return amz.group(1)
    ml = re.search(r'(MLB\d+)', url.replace("-", ""))
    if ml: return ml.group(1)
    return url[-12:]

async def encurtar_bitly(url):
    async with aiohttp.ClientSession() as session:
        h = {"Authorization": f"Bearer {BITLY_TOKEN}"}
        try:
            async with session.post("https://api-ssl.bitly.com/v4/shorten", json={"long_url": url}, headers=h) as r:
                d = await resp.json()
                return d.get("link", url)
        except: return url

async def buscar_imagem(url):
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
# 🔹 CONVERSORES E FORMATAÇÃO
# ============================================================

def formatar_texto(texto, links_conv):
    preco_m = re.search(r'R\$\s?\d+[.,\d]*', texto)
    preco = preco_m.group(0) if preco_m else ""
    cupom_m = re.search(r'(?i)cupom[:\s]+([A-Z0-9]{4,20})', texto)
    cupom = cupom_m.group(1) if cupom_m else ""
    
    linhas = texto.split('\n')
    titulo = re.sub(r'[✅🔥🎟🛒🚨]|https?://\S+', '', linhas[0]).strip() if linhas else "Produto"

    msg = f"✅ Produto: {titulo}\n"
    if preco: msg += f"🔥 Preço: {preco}\n"
    if cupom: msg += f"🎟 Cupom: `{cupom.upper()}`\n"
    msg += "\n" + "\n".join(links_conv)
    return msg, preco, cupom

async def converter_link(url):
    url_l = url.lower()
    
    if "mercadolivre" in url_l or "meli.la" in url_l:
        if "/sec/" in url_l or "/lista/" in url_l or "/lists/" in url_l: return MEU_LINK_SOCIAL_ML, "ml", True
        url_ex = await expandir_url(url)
        p = {"matt_tool": "afiliados", "matt_source": ML_SOURCE, "matt_campaign": "ofertap"}
        return urlunparse(urlparse(url_ex)._replace(query=urlencode(parse_qs(urlparse(url_ex).query) | {k:[v] for k,v in p.items()}, doseq=True))), "ml", False
    
    elif "amazon" in url_l or "amzn" in url_l:
        url_ex = await expandir_url(url)
        return urlunparse(urlparse(url_ex)._replace(query=urlencode(parse_qs(urlparse(url_ex).query) | {'tag': [AMAZON_TAG]}, doseq=True))), "amazon", False
    
    elif "magazineluiza" in url_l or "magalu" in url_l:
        p = {"utm_source": "divulgador", "partner_id": MAGALU_PARTNER, "promoter_id": MAGALU_PROMOTER}
        l_url = urlunparse(urlparse(url)._replace(query=urlencode(parse_qs(urlparse(url).query) | {k:[v] for k,v in p.items()}, doseq=True)))
        return await encurtar_bitly(l_url), "magalu", False
    
    elif "shopee" in url_l:
        ts = str(int(time.time()))
        payload = json.dumps({"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{url}" }}) {{ shortLink }} }}'}, separators=(",", ":"))
        sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
        headers = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post("https://open-api.affiliate.shopee.com.br/graphql", data=payload, headers=headers) as resp:
                    res = await resp.json()
                    return res["data"]["generateShortLink"]["shortLink"], "shopee", False
            except: return url, "shopee", False

    if any(x in url_l for x in ["cadastro", "ganhe", "promo", "formulario"]): return url, "info", False
    return None, None, False

# ============================================================
# 🔹 MOTOR DE PROCESSAMENTO
# ============================================================

async def processar(event, is_edit=False):
    texto_orig = event.message.text or ""
    if not texto_orig.strip(): return

    # 🔹 FILTRO DE PALAVRAS BLOQUEADAS (Aplicado aqui)
    if any(p.lower() in texto_orig.lower() for p in FILTRO):
        print(f"🚫 Bloqueado pelo filtro: {texto_orig[:30]}...")
        return

    chat = await event.get_chat()
    username = (chat.username or "").lower()
    links_brutos = re.findall(r'https?://\S+', texto_orig)
    
    # Regra: Mensagem sem link só passa se for do fadadoscupons
    if not links_brutos and username != GRUPO_CUPONS_EXCLUSIVO: return

    links_conv = []
    usar_img_f = False
    plat_p, p_id = "outro", "0"

    for link in links_brutos:
        novo, plat, img_f = await converter_link(link)
        if novo:
            links_conv.append(novo)
            if plat != "info": 
                plat_p, p_id = plat, extrair_id(novo)
            if img_f: usar_img_f = True

    final_msg, preco, cupom = formatar_texto(texto_orig, links_conv)

    # LÓGICA DE CACHE
    if not is_edit:
        if ja_foi_enviado(plat_p, p_id, preco, cupom): return
        marcar_como_enviado(plat_p, p_id, preco, cupom)

    # Imagem
    imagem = None
    if usar_img_f and os.path.exists(IMG_ML_FIXA): imagem = IMG_ML_FIXA
    elif event.message.media and not isinstance(event.message.media, MessageMediaWebPage): imagem = event.message.media
    elif links_conv: imagem = await buscar_imagem(links_conv[0])

    try:
        mapping = carregar_cache().get("mapeamento", {})
        if is_edit and str(event.message.id) in mapping:
            await client.edit_message(GRUPO_DESTINO, mapping[str(event.message.id)], final_msg)
        else:
            sent = None
            if imagem:
                if len(final_msg) > 1024:
                    photo = await client.send_file(GRUPO_DESTINO, imagem)
                    sent = await client.send_message(GRUPO_DESTINO, final_msg, reply_to=photo.id)
                else:
                    sent = await client.send_file(GRUPO_DESTINO, imagem, caption=final_msg)
            else:
                sent = await client.send_message(GRUPO_DESTINO, final_msg)
            
            if sent:
                cache = carregar_cache()
                if "mapeamento" not in cache: cache["mapeamento"] = {}
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

print("🚀 BOT BLACK OPS v22.0 ONLINE - FILTRO ATIVO!")
client.start()
client.run_until_disconnected()

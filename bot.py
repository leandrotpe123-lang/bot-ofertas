import re, asyncio, hashlib, json, time, os, aiohttp, random, unicodedata
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

# ============================================================
# 🔹 CONFIGURAÇÕES GERAIS
# ============================================================
API_ID = 33768893
API_HASH = '7959ea0392ff7f91b4f7e207e75a1813'
SESSION_STRING = os.environ.get("TELEGRAM_SESSION")

if not SESSION_STRING:
    print("❌ ERRO: Configure a variável TELEGRAM_SESSION no Railway!")
    exit(1)

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

# ARQUIVOS E IMAGENS
MEU_LINK_SOCIAL_ML = "https://mercadolivre.com/sec/23NpLSc"
IMG_FIXA = "mercado_livre_c1a918503a.jpg" 
ARQUIVO_DEDUP = "deduplicacao_profissional.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

envio_lock = asyncio.Semaphore(2)

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
    "DIMM", "DDR4", "DDR5", "Dram"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
]

# ============================================================
# 🔹 DEDUPLICAÇÃO PROFISSIONAL (SUA LÓGICA)
# ============================================================
PALAVRAS_RUIDO = {"promo", "promocao", "promoção", "oferta", "imperdivel", "imperdível", "corre", "aproveita", "desconto", "cupom", "frete", "gratis", "grátis", "urgente", "relampago", "relâmpago", "hoje", "agora"}

def remover_acentos(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def normalizar_texto_raiz(texto):
    if not texto: return ""
    texto = remover_acentos(texto.lower())
    texto = re.sub(r"[^\w\s]", " ", texto)
    texto = re.sub(r"http\S+|www\S+", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return " ".join([p for p in texto.split() if p not in PALAVRAS_RUIDO])

def deve_enviar_oferta(plataforma, produto_id, preco, cupom="", texto=""):
    try:
        cache = json.load(open(ARQUIVO_DEDUP, "r")) if os.path.exists(ARQUIVO_DEDUP) else {}
    except: cache = {}
    
    agora = time.time()
    cache = {k: v for k, v in cache.items() if agora - v < 172800} # 48h

    t_raiz = normalizar_texto_raiz(texto)
    chave = f"{plataforma}|{produto_id}|{str(preco).strip()}|{str(cupom).strip().lower()}|{t_raiz}"
    fp = hashlib.md5(chave.encode("utf-8")).hexdigest()

    if fp in cache: return False
    cache[fp] = agora
    json.dump(cache, open(ARQUIVO_DEDUP, "w"), indent=2)
    return True

# ============================================================
# 🔹 UTILITÁRIOS ELITE (LINK & IMAGEM)
# ============================================================

async def expandir_url_elite(url):
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": random.choice(USER_AGENTS)}) as s:
            async with s.get(url, allow_redirects=True, timeout=12) as r:
                return str(r.url)
    except: return url

async def encurtar_bitly(url):
    async with aiohttp.ClientSession() as s:
        h = {"Authorization": f"Bearer {BITLY_TOKEN}"}
        try:
            async with s.post("https://api-ssl.bitly.com/v4/shorten", json={"long_url": url}, headers=h) as r:
                d = await r.json()
                return d.get("link", url)
        except: return url

async def buscar_imagem_3x(url):
    async with aiohttp.ClientSession(headers={"User-Agent": random.choice(USER_AGENTS)}) as s:
        for _ in range(3):
            try:
                async with s.get(url, timeout=8) as r:
                    soup = BeautifulSoup(await r.text(), 'html.parser')
                    img = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "twitter:image"})
                    if img: return img['content']
            except: await asyncio.sleep(1)
    return None

# ============================================================
# 🔹 CONVERSORES (MULTILINK ATÉ 50)
# ============================================================

async def converter_shopee(url):
    ts = str(int(time.time()))
    payload = json.dumps({"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{url}" }}) {{ shortLink }} }}'}, separators=(",", ":"))
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
    # Mercado Livre
    if "mercadolivre" in url_l or "meli.la" in url_l:
        if any(x in url_l for x in ["/sec/", "/lista/", "/social/", "/lists/"]):
            return MEU_LINK_SOCIAL_ML, "ml", True
        url_ex = await expandir_url_elite(url)
        q = parse_qs(urlparse(url_ex).query); q.update({"matt_tool": ["afiliados"], "matt_source": [ML_SOURCE], "matt_campaign": ["ofertap"]})
        return urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True))), "ml", False
    # Amazon
    elif "amazon" in url_l or "amzn" in url_l:
        url_ex = await expandir_url_elite(url)
        q = parse_qs(urlparse(url_ex).query); q.update({"tag": [AMAZON_TAG]})
        return urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True))), "amazon", False
    # Magalu
    elif "magazineluiza" in url_l or "magalu" in url_l:
        url_ex = await expandir_url_elite(url)
        q = parse_qs(urlparse(url_ex).query); q.update({"partner_id": [MAGALU_PARTNER], "promoter_id": [MAGALU_PROMOTER]})
        l_url = urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True)))
        return await encurtar_bitly(l_url), "magalu", False
    # Shopee
    elif "shopee" in url_l:
        return await converter_shopee(url), "shopee", False
    # Cadastro
    if any(x in url_l for x in ["cadastro", "ganhe", "promo", "formulario", "inscricao"]): return url, "info", False
    return None, None, False

# ============================================================
# 🔹 FORMATAÇÃO (LIMPANDO LABELS + EMOJIS)
# ============================================================

def formatar_texto_final(texto, links_conv):
    # 1. Remove "Produto:", "Preço:", etc.
    texto = re.sub(r'(?i)(produto|preço|cupom|valor):\s*', '', texto)
    # 2. Crases nos Cupons
    texto = re.sub(r'\b([A-Z0-9]{4,20})\b', r'`\1`', texto)
    
    linhas = texto.split('\n')
    novas_linhas = []
    for i, linha in enumerate(linhas):
        linha = linha.strip()
        if not linha: continue
        # Emoji Inteligente
        if not re.match(r'[^\w\s]', linha):
            if i == 0: linha = "✅ " + linha
            elif "R$" in linha: linha = "🔥 " + linha
            elif "`" in linha: linha = "🎟 " + linha
        novas_linhas.append(linha)
    
    texto_corpo = re.sub(r'https?://\S+', '', "\n".join(novas_linhas)).strip()
    return texto_corpo + "\n\n" + "\n".join(links_conv)

# ============================================================
# 🔹 MOTOR DE PROCESSAMENTO
# ============================================================

async def processar_evento(event, is_edit=False):
    raw_text = event.message.text or ""
    if not raw_text.strip(): return
    if any(p.lower() in raw_text.lower() for p in FILTRO): return

    chat = await event.get_chat()
    username = (chat.username or "").lower()
    links_detectados = re.findall(r'https?://\S+', raw_text)
    
    if not links_detectados and username != GRUPO_CUPONS_EXCLUSIVO: return

    links_convertidos, force_img_ml, plat_p, prod_id = [], False, "outro", "0"
    for link in links_detectados[:50]:
        novo, plat, force_img = await converter_link(link)
        if novo:
            links_convertidos.append(novo)
            if plat != "info": 
                plat_p = plat
                prod_id = re.search(r'/(?:dp|MLB|product|i\.)/([A-Z0-9.\-_]+)', novo).group(1) if re.search(r'/(?:dp|MLB|product|i\.)/([A-Z0-9.\-_]+)', novo) else novo[-15:]
            if force_img: force_img_ml = True

    if links_detectados and not links_convertidos: return

    preco = re.search(r'R\$\s?\d+[.,\d]*', raw_text).group(0) if re.search(r'R\$\s?\d+[.,\d]*', raw_text) else "0"
    cupom = re.search(r'\b([A-Z0-9]{4,20})\b', raw_text).group(1) if re.search(r'\b([A-Z0-9]{4,20})\b', raw_text) else ""

    if not is_edit:
        if not deve_enviar_oferta(plat_p, prod_id, preco, cupom, raw_text): return

    final_msg = formatar_texto_final(raw_text, links_convertidos)

    # 🔹 LÓGICA DE IMAGEM BLINDADA
    imagem = None
    tem_midia = event.message.media and not isinstance(event.message.media, MessageMediaWebPage)

    if force_img_ml: imagem = IMG_FIXA # ML Lista/Social SEMPRE FIXA
    elif username == GRUPO_CUPONS_EXCLUSIVO:
        imagem = event.message.media if tem_midia else IMG_FIXA # Cupons Shopee/Amazon
    elif tem_midia:
        imagem = event.message.media # Oferta normal com foto
    elif links_convertidos:
        imagem = await buscar_imagem_3x(links_convertidos[0]) # Busca 3x se sem foto

    async with envio_lock:
        try:
            mapping = json.load(open(ARQUIVO_MAPEAMENTO, "r")) if os.path.exists(ARQUIVO_MAPEAMENTO) else {}
            if is_edit and str(event.message.id) in mapping:
                await client.edit_message(GRUPO_DESTINO, mapping[str(event.message.id)], final_msg)
            else:
                sent = None
                if imagem:
                    if len(final_msg) > 1024:
                        photo = await client.send_file(GRUPO_DESTINO, imagem)
                        sent = await client.send_message(GRUPO_DESTINO, final_msg, reply_to=photo.id)
                    else: sent = await client.send_file(GRUPO_DESTINO, imagem, caption=final_msg)
                else: sent = await client.send_message(GRUPO_DESTINO, final_msg)
                
                if sent:
                    mapping[str(event.message.id)] = sent.id
                    json.dump(mapping, open(ARQUIVO_MAPEAMENTO, "w"))
                    print("✅ Oferta enviada escorregando!")
        except Exception as e: print(f"❌ Erro: {e}")

# ============================================================
# 🔹 START
# ============================================================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

async def main():
    await client.connect()
    if not await client.is_user_authorized(): return
    print("🚀 BOT MASTER v40.0 ONLINE!")
    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def n_h(e): await processar_evento(e)
    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def e_h(e): await processar_evento(e, is_edit=True)
    await client.run_until_disconnected()

if __name__ == '__main__': asyncio.run(main())

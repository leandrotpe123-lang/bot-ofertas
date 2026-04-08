import re, asyncio, hashlib, json, time, os, aiohttp, random, unicodedata
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage
from telethon.errors import MessageNotModifiedError
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

# ============================================================
# 🔹 CONFIGURAÇÕES GERAIS
# ============================================================
API_ID = 33768893
API_HASH = '7959ea0392ff7f91b4f7e207e75a1813'
SESSION_STRING = os.environ.get("TELEGRAM_SESSION")

if not SESSION_STRING:
    print("❌ ERRO: A variável TELEGRAM_SESSION não foi encontrada no Railway!")
    exit(1)

# CANAIS
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

# ARQUIVOS FIXOS
MEU_LINK_SOCIAL_ML = "https://mercadolivre.com/sec/23NpLSc"
IMG_FIXA = "mercado_livre_c1a918503a.jpg" 
ARQUIVO_DEDUP = "deduplicacao_profissional.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

# SEMÁFORO (Bypass de Flood)
envio_lock = asyncio.Semaphore(2)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
]

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

# ============================================================
# 🔹 SISTEMA DE DEDUPLICAÇÃO PROFISSIONAL
# ============================================================
PALAVRAS_RUIDO = {"promo", "promocao", "promoção", "oferta", "imperdivel", "corre", "aproveita", "desconto", "cupom", "frete", "gratis"}

def normalizar_texto_raiz(texto):
    if not texto: return ""
    texto = "".join(c for c in unicodedata.normalize('NFD', texto.lower()) if unicodedata.category(c) != 'Mn')
    texto = re.sub(r"[^\w\s]", " ", texto)
    texto = re.sub(r"http\S+|www\S+", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return " ".join([p for p in texto.split() if p not in PALAVRAS_RUIDO])

def deve_enviar_oferta(plataforma, produto_id, preco, cupom="", texto=""):
    try:
        cache = json.load(open(ARQUIVO_DEDUP, "r")) if os.path.exists(ARQUIVO_DEDUP) else {}
    except: cache = {}
    agora = time.time()
    cache = {k: v for k, v in cache.items() if agora - v < 172800} # 48 horas
    t_raiz = normalizar_texto_raiz(texto)
    fp = hashlib.md5(f"{plataforma}|{produto_id}|{preco}|{cupom}|{t_raiz}".encode()).hexdigest()
    if fp in cache: return False
    cache[fp] = agora
    json.dump(cache, open(ARQUIVO_DEDUP, "w"), indent=2)
    return True

# ============================================================
# 🔹 UTILITÁRIOS ELITE (LINKS E IMAGENS)
# ============================================================
async def expandir_url(url):
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

async def buscar_imagem_scrape(url):
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
    h = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as s:
        try:
            async with s.post("https://open-api.affiliate.shopee.com.br/graphql", data=payload, headers=h) as r:
                res = await r.json()
                return res["data"]["generateShortLink"]["shortLink"] if "data" in res and res["data"] else url
        except: return url

async def converter_link(url):
    url_l = url.lower()
    if "mercadolivre" in url_l or "meli.la" in url_l:
        if any(x in url_l for x in ["/sec/", "/lista/", "/social/", "/lists/"]):
            return MEU_LINK_SOCIAL_ML, "ml", True
        url_ex = await expandir_url(url)
        q = parse_qs(urlparse(url_ex).query); q.update({"matt_tool": ["afiliados"], "matt_source": [ML_SOURCE], "matt_campaign": ["ofertap"]})
        return urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True))), "ml", False
    elif "amazon" in url_l or "amzn" in url_l:
        url_ex = await expandir_url(url)
        q = parse_qs(urlparse(url_ex).query); q.update({"tag": [AMAZON_TAG]})
        return urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True))), "amazon", False
    elif "magazineluiza" in url_l or "magalu" in url_l:
        url_ex = await expandir_url(url)
        q = parse_qs(urlparse(url_ex).query); q.update({"partner_id": [MAGALU_PARTNER], "promoter_id": [MAGALU_PROMOTER]})
        l_url = urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True)))
        return await encurtar_bitly(l_url), "magalu", False
    elif "shopee" in url_l:
        return await converter_shopee(url), "shopee", False
    if any(x in url_l for x in ["cadastro", "ganhe", "promo", "formulario", "inscricao", "sorteio"]):
        return url, "info", False
    return None, None, False

# ============================================================
# 🔹 FORMATAÇÃO FINAL
# ============================================================
def formatar_texto_final(texto, links_conv):
    texto = re.sub(r'(?i)(produto|preço|cupom|valor):\s*', '', texto)
    texto = re.sub(r'\b([A-Z0-9]{4,20})\b', r'`\1`', texto)
    linhas = [l.strip() for l in texto.split('\n') if l.strip()]
    novas = []
    for i, linha in enumerate(linhas):
        if not re.match(r'[^\w\s]', linha):
            if i == 0: linha = "✅ " + linha
            elif "R$" in linha: linha = "🔥 " + linha
            elif "`" in linha: linha = "🎟 " + linha
        novas.append(linha)
    corpo = re.sub(r'https?://\S+', '', "\n".join(novas)).strip()
    return corpo + "\n\n" + "\n".join(links_conv)

# ============================================================
# 🔹 MOTOR DE PROCESSAMENTO
# ============================================================
async def processar_evento(event, is_edit=False):
    texto_bruto = event.message.text or ""
    if not texto_bruto.strip() or any(p.lower() in texto_bruto.lower() for p in FILTRO): return
    
    chat = await event.get_chat()
    username = (chat.username or "").lower()
    links_raw = re.findall(r'https?://\S+', texto_bruto)
    
    if not links_raw and username != GRUPO_CUPONS_EXCLUSIVO: return

    links_conv, forcar_fixa, plat_p, prod_id = [], False, "outro", "0"
    for l in links_raw[:50]:
        novo, plat, force = await converter_link(l)
        if novo:
            links_conv.append(novo)
            if plat != "info":
                plat_p = plat
                m = re.search(r'/(?:dp|MLB|product|i\.)/([A-Z0-9.\-_]+)', novo)
                prod_id = m.group(1) if m else novo[-15:]
            if force: forcar_fixa = True

    if links_raw and not links_conv: return
    
    preco = re.search(r'R\$\s?\d+[.,\d]*', texto_bruto).group(0) if re.search(r'R\$\s?\d+[.,\d]*', texto_bruto) else "0"
    cupom = re.search(r'\b([A-Z0-9]{4,20})\b', texto_bruto).group(1) if re.search(r'\b([A-Z0-9]{4,20})\b', texto_bruto) else ""

    if not is_edit and not deve_enviar_oferta(plat_p, prod_id, preco, cupom, texto_bruto): return

    final_msg = formatar_texto_final(texto_bruto, links_conv)
    imagem = None
    tem_midia = event.message.media and not isinstance(event.message.media, MessageMediaWebPage)

    if forcar_fixa: imagem = IMG_FIXA
    elif username == GRUPO_CUPONS_EXCLUSIVO: imagem = event.message.media if tem_midia else IMG_FIXA
    elif tem_midia: imagem = event.message.media
    elif links_conv: imagem = await buscar_imagem_scrape(links_conv[0])

    async with envio_lock:
        try:
            mapa = json.load(open(ARQUIVO_MAPEAMENTO, "r")) if os.path.exists(ARQUIVO_MAPEAMENTO) else {}
            if is_edit and str(event.message.id) in mapa:
                try: await client.edit_message(GRUPO_DESTINO, mapa[str(event.message.id)], final_msg)
                except MessageNotModifiedError: pass
            else:
                if imagem:
                    if len(final_msg) > 1024:
                        p = await client.send_file(GRUPO_DESTINO, imagem)
                        sent = await client.send_message(GRUPO_DESTINO, final_msg, reply_to=p.id)
                    else: sent = await client.send_file(GRUPO_DESTINO, imagem, caption=final_msg)
                else: sent = await client.send_message(GRUPO_DESTINO, final_msg)
                
                if sent:
                    mapa[str(event.message.id)] = sent.id
                    json.dump(mapa, open(ARQUIVO_MAPEAMENTO, "w"))
                    print("✅ Oferta enviada!")
        except Exception as e: print(f"❌ Erro: {e}")

# ============================================================
# 🔹 INICIALIZAÇÃO
# ============================================================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

async def main():
    await client.connect()
    if not await client.is_user_authorized():
        print("❌ Sessão Inválida!"); return
    print("🚀 BOT MASTER v43.0 ONLINE!")
    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def n(e): await processar_evento(e)
    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def ed(e): await processar_evento(e, is_edit=True)
    await client.run_until_disconnected()

if __name__ == '__main__': asyncio.run(main())

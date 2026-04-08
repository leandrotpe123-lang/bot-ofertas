import re, asyncio, hashlib, json, time, os, aiohttp, random, unicodedata, requests
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage
from telethon.errors import MessageNotModifiedError
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from difflib import SequenceMatcher
from threading import Lock

# ============================================================
# 🔹 CONFIGURAÇÕES GERAIS
# ============================================================
API_ID = 33768893
API_HASH = '7959ea0392ff7f91b4f7e207e75a1813'
SESSION_STRING = os.environ.get("TELEGRAM_SESSION")

GRUPOS_ORIGEM = ['fumotom', 'promotom', 'fadadoscupons', 'botofera']
GRUPO_DESTINO = '@ofertap'

# TAGS AFILIADOS
AMAZON_TAG      = "leo21073-20"
MAGALU_PARTNER  = "3440"
MAGALU_PROMOTER = "5479317"
ML_SOURCE       = "silvaleo20230518163534"
SHOPEE_APP_ID   = "18348480261"
SHOPEE_SECRET   = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"
BITLY_TOKEN     = "69cdfdea70096c9cf42a5eac20cb55b17668ede9"

# ARQUIVOS E IMAGENS FIXAS
IMG_ML_FIXA = "mercado_livre_c1a918503a.jpg"
IMG_SHOPEE_FIXA = "shopee_fixa.jpg"
IMG_AMAZON_FIXA = "amazon_fixa.jpg"
ARQUIVO_CACHE = "cache_dedup_profissional.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

# LÓGICA DE DEDUPLICAÇÃO
LOCK = Lock()
TTL_MINUTOS = 120
TTL_SEGUNDOS = TTL_MINUTOS * 60
JANELA_ANTISPAM_SEGUNDOS = 900
SIMILARIDADE_MINIMA = 0.90
PALAVRAS_RUIDO = {"promo", "promocao", "promoção", "oferta", "desconto", "cupom", "corre", "aproveita", "urgente", "gratis", "grátis", "frete", "hoje", "agora", "relampago", "relâmpago"}

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
    "DIMM", "DDR5", "DDR4", "Dram"
]

# ============================================================
# 🔹 SISTEMA DE DEDUPLICAÇÃO PROFISSIONAL
# ============================================================

def remover_acentos(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def normalizar_texto(texto):
    if not texto: return ""
    texto = remover_acentos(texto.lower())
    texto = re.sub(r"http\S+|www\S+", " ", texto)
    texto = re.sub(r"[^\w\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    tokens = [t for t in texto.split() if t not in PALAVRAS_RUIDO]
    tokens.sort()
    return " ".join(tokens)

def similaridade_texto(t1, t2):
    return SequenceMatcher(None, t1, t2).ratio()

def carregar_cache():
    if not os.path.exists(ARQUIVO_CACHE): return {}
    try:
        with open(ARQUIVO_CACHE, "r", encoding="utf-8") as f: return json.load(f)
    except: return {}

def salvar_cache(cache):
    with open(ARQUIVO_CACHE, "w", encoding="utf-8") as f: json.dump(cache, f, ensure_ascii=False, indent=2)

def deve_enviar_oferta(plataforma, produto_id, preco, cupom="", texto=""):
    with LOCK:
        cache = carregar_cache()
        agora = time.time()
        cache = {k: v for k, v in cache.items() if agora - v["timestamp"] < TTL_SEGUNDOS}
        texto_norm = normalizar_texto(texto)
        hash_exato = hashlib.sha256(f"{plataforma}|{produto_id}|{preco}|{cupom.lower()}|{texto_norm}".encode()).hexdigest()
        if hash_exato in cache: return False
        for _, oferta in cache.items():
            if str(oferta["produto_id"]) == str(produto_id) and str(oferta["preco"]) == str(preco) and oferta["cupom"] == str(cupom).lower():
                if agora - oferta["timestamp"] < JANELA_ANTISPAM_SEGUNDOS and similaridade_texto(texto_norm, oferta["texto"]) >= SIMILARIDADE_MINIMA:
                    return False
        cache[hash_exato] = {"produto_id": str(produto_id), "preco": str(preco), "cupom": str(cupom).lower(), "texto": texto_norm, "timestamp": agora}
        salvar_cache(cache)
        return True

# ============================================================
# 🔹 CONVERSORES ELITE
# ============================================================

async def expandir_url(url):
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as s:
            async with s.get(url, allow_redirects=True, timeout=10) as r:
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

async def converter_link(url):
    url_l = url.lower()
    # Mercado Livre
    if "mercadolivre" in url_l or "meli.la" in url_l:
        if "/sec/" in url_l or "/social/" in url_l or "/lista/" in url_l:
            # Troca link social pelo seu e marca imagem fixa
            return "https://mercadolivre.com/sec/23NpLSc", "ml", True
        url_ex = await expandir_url(url)
        q = parse_qs(urlparse(url_ex).query); q.update({"matt_tool": ["afiliados"], "matt_source": [ML_SOURCE], "matt_campaign": ["ofertap"]})
        return urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True))), "ml", False

    # Amazon (Sua Lógica Descascada Solicitada)
    elif "amazon" in url_l or "amzn" in url_l:
        url_ex = await expandir_url(url)
        base = url_ex.split("?")[0].split("#")[0]
        node = re.search(r'node=([^&]+)', url_ex)
        url_final = f"{base}?node={node.group(1)}&tag={AMAZON_TAG}" if node else f"{base}?tag={AMAZON_TAG}"
        return url_final, "amazon", False

    # Magalu
    elif "magazineluiza" in url_l or "magalu" in url_l:
        url_ex = await expandir_url(url)
        q = parse_qs(urlparse(url_ex).query); q.update({"partner_id": [MAGALU_PARTNER], "promoter_id": [MAGALU_PROMOTER]})
        l_url = urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True)))
        return await encurtar_bitly(l_url), "magalu", False

    # Shopee
    elif "shopee" in url_l:
        ts = str(int(time.time()))
        payload = json.dumps({"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{url}" }}) {{ shortLink }} }}'}, separators=(",", ":"))
        sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
        h = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession() as s:
            try:
                async with s.post("https://open-api.affiliate.shopee.com.br/graphql", data=payload, headers=h) as r:
                    res = await r.json()
                    return res["data"]["generateShortLink"]["shortLink"], "shopee", False
            except: return url, "shopee", False

    # Cadastro / Promoção
    if any(x in url_l for x in ["cadastro", "ganhe", "promo", "formulario", "inscricao"]): return url, "info", False
    return None, None, False

# ============================================================
# 🔹 FORMATAÇÃO DE TEXTO
# ============================================================

def formatar_texto_final(texto, links_conv):
    # Remove nomes chatos: Produto, Preço, etc.
    texto = re.sub(r'(?i)(produto|preço|cupom|valor):\s*', '', texto)
    # Craser nos cupons
    texto = re.sub(r'\b([A-Z0-9]{5,25})\b', r'`\1`', texto)
    
    linhas = texto.split('\n')
    novas_linhas = []
    for i, linha in enumerate(linhas):
        linha = linha.strip()
        if not linha: continue
        if not re.match(r'[^\w\s]', linha): # Se não tiver emoji
            if i == 0: linha = "🔥 " + linha
            elif "R$" in linha: linha = "💵 " + linha
            elif "`" in linha: linha = "🎟 " + linha
        novas_linhas.append(linha)
    
    corpo = re.sub(r'https?://\S+', '', "\n".join(novas_linhas)).strip()
    return corpo + "\n\n" + "\n".join(links_conv)

# ============================================================
# 🔹 MOTOR DE PROCESSAMENTO
# ============================================================

async def processar_evento(event, is_edit=False):
    texto_bruto = event.message.text or ""
    if not texto_bruto.strip() or any(p.lower() in texto_bruto.lower() for p in FILTRO): return

    links_detectados = re.findall(r'https?://\S+', texto_bruto)
    chat = await event.get_chat()
    username = (chat.username or "").lower()

    # Bloqueia sem link (exceto grupo de cupons)
    if not links_detectados and username != "fadadoscupons": return

    links_conv, force_img_ml, plat_p, prod_id = [], False, "outro", "0"
    for link in links_detectados[:50]: # Multi-link até 50
        novo, plat, img_f = await converter_link(link)
        if novo:
            links_conv.append(novo)
            if plat != "info": 
                plat_p = plat
                id_m = re.search(r'/(?:dp|MLB|product|i\.)/([A-Z0-9.\-_]+)', novo)
                prod_id = id_m.group(1) if id_m else novo[-15:]
            if img_f: force_img_ml = True

    if links_detectados and not links_conv: return # Link estranho detectado

    preco = re.search(r'R\$\s?\d+[.,\d]*', texto_bruto).group(0) if re.search(r'R\$\s?\d+[.,\d]*', texto_bruto) else "0"
    cupom = re.search(r'\b([A-Z0-9]{5,25})\b', texto_bruto).group(1) if re.search(r'\b([A-Z0-9]{5,25})\b', texto_bruto) else ""

    if not is_edit and not deve_enviar_oferta(plat_p, prod_id, preco, cupom, texto_bruto): return

    final_msg = formatar_texto_final(texto_bruto, links_conv)

    # Lógica de Imagem
    imagem = None
    tem_media = event.message.media and not isinstance(event.message.media, MessageMediaWebPage)

    if force_img_ml: imagem = IMG_ML_FIXA
    elif username == "fadadoscupons":
        if tem_media: imagem = event.message.media
        else:
            if "shopee" in str(links_conv).lower(): imagem = IMG_SHOPEE_FIXA if os.path.exists(IMG_SHOPEE_FIXA) else IMG_ML_FIXA
            else: imagem = IMG_AMAZON_FIXA if os.path.exists(IMG_AMAZON_FIXA) else IMG_ML_FIXA
    elif tem_media: imagem = event.message.media
    else:
        # Scrape 3x
        async with aiohttp.ClientSession() as s:
            for _ in range(3):
                try:
                    async with s.get(links_conv[0], timeout=5) as r:
                        soup = BeautifulSoup(await r.text(), 'html.parser')
                        img = soup.find("meta", property="og:image")
                        if img: imagem = img['content']; break
                except: await asyncio.sleep(1)

    async with envio_lock:
        try:
            mapping = json.load(open(ARQUIVO_MAPEAMENTO, "r")) if os.path.exists(ARQUIVO_MAPEAMENTO) else {}
            if is_edit and str(event.message.id) in mapping:
                try: await client.edit_message(GRUPO_DESTINO, mapping[str(event.message.id)], final_msg)
                except MessageNotModifiedError: pass
            else:
                sent = None
                if imagem:
                    if len(final_msg) > 1024:
                        p = await client.send_file(GRUPO_DESTINO, imagem)
                        sent = await client.send_message(GRUPO_DESTINO, final_msg, reply_to=p.id)
                    else: sent = await client.send_file(GRUPO_DESTINO, imagem, caption=final_msg)
                else: sent = await client.send_message(GRUPO_DESTINO, final_msg)
                
                if sent:
                    mapping[str(event.message.id)] = sent.id
                    json.dump(mapping, open(ARQUIVO_MAPEAMENTO, "w"))
        except Exception as e: print(f"❌ Erro: {e}")

# ============================================================
# 🔹 START
# ============================================================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

async def main():
    await client.connect()
    if not await client.is_user_authorized(): return
    print("🚀 BOT v42.0 ONLINE!")
    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def n_h(e): await processar_evento(e)
    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def e_h(e): await processar_evento(e, is_edit=True)
    await client.run_until_disconnected()

if __name__ == '__main__': asyncio.run(main())

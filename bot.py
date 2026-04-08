import os, re, time, json, asyncio, aiohttp, hashlib, random, unicodedata, logging
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage
from telethon.errors import MessageNotModifiedError, FloodWaitError
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from difflib import SequenceMatcher
from threading import Lock

# ============================================================
# 🔹 MÓDULO 1: DEBUG & LOGGING INDIVIDUAL
# ============================================================
def get_custom_logger(name, color_code):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(f'\033[{color_code}m[%(name)s]\033[0m %(asctime)s - %(message)s', datefmt='%H:%M:%S')
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

log_amz = get_custom_logger('AMAZON', '1;33') 
log_shp = get_custom_logger('SHOPEE', '1;38;5;208') 
log_sys = get_custom_logger('SISTEMA', '1;32') 

# ============================================================
# 🔹 CONFIGURAÇÕES GERAIS
# ============================================================
API_ID = 33768893
API_HASH = '7959ea0392ff7f91b4f7e207e75a1813'
SESSION_STRING = os.environ.get("TELEGRAM_SESSION")

GRUPOS_PRODUTOS = ['promotom', 'botofera', 'fumotom'] 
GRUPO_CUPONS_ONLY = ['fadadoscupons','botofera']
GRUPOS_ORIGEM = GRUPOS_PRODUTOS + [GRUPO_CUPONS_ONLY]
GRUPO_DESTINO = '@ofertap'

AMAZON_TAG = "leo21073-20"
SHOPEE_APP_ID = "18348480261"
SHOPEE_SECRET = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"

IMG_AMAZON = "cupom-amazon.jpg" 
IMG_SHOPEE = "IMG_20260404_180150.jpg"

ARQUIVO_CACHE = "cache_dedup_profissional.json"
ARQUIVO_MAPEAMENTO = "map_mensagens_edicao.json"

envio_lock = asyncio.Semaphore(3)

FILTRO = ["Monitor Samsung", "Fonte Mancer", "Placa de video", "Monitor LG", "VHAGAR", "Superframe", "Dram", "Monitor Safe", "Monitor Redragon"]

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"]

# ============================================================
# 🔹 MÓDULO 2: DEDUPLICAÇÃO PROFISSIONAL (SUA LÓGICA)
# ============================================================
LOCK_DEDUP = Lock()
TTL_SEGUNDOS = 120 * 60
SIMILARIDADE_BLOQUEIO = 0.90
PALAVRAS_RUIDO = {"promo", "promocao", "promoção", "oferta", "desconto", "cupom", "corre", "aproveita", "urgente", "gratis"}

def normalizar_texto(texto):
    if not texto: return ""
    texto = "".join(c for c in unicodedata.normalize('NFD', texto.lower()) if unicodedata.category(c) != 'Mn')
    texto = re.sub(r"http\S+|www\S+|[^\w\s]", " ", texto)
    tokens = sorted([p for p in texto.split() if p not in PALAVRAS_RUIDO])
    return " ".join(tokens)

def deve_enviar_oferta(plataforma, produto_id, preco, cupom="", texto=""):
    with LOCK_DEDUP:
        try: cache = json.load(open(ARQUIVO_CACHE, "r")) if os.path.exists(ARQUIVO_CACHE) else {}
        except: cache = {}
        agora = time.time()
        cache = {k: v for k, v in cache.items() if agora - v["timestamp"] < TTL_SEGUNDOS}
        texto_norm = normalizar_texto(texto)
        h = hashlib.sha256(f"{plataforma}|{produto_id}|{preco}|{cupom.lower()}|{texto_norm}".encode()).hexdigest()
        if h in cache: return False
        for oferta in cache.values():
            if str(oferta["produto_id"]) == str(produto_id) and str(oferta["preco"]) == str(preco) and \
               SequenceMatcher(None, texto_norm, oferta["texto"]).ratio() >= SIMILARIDADE_BLOQUEIO:
                return False
        cache[h] = {"produto_id": str(produto_id), "preco": str(preco), "cupom": str(cupom).lower(), 
                    "texto": texto_norm, "timestamp": agora, "plataforma": plataforma}
        json.dump(cache, open(ARQUIVO_CACHE, "w"), indent=2)
        return True

# ============================================================
# 🔹 MÓDULO 3: MOTORES DE CONVERSÃO (MASSIVO 50 LINKS)
# ============================================================
async def motor_amazon(url):
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": random.choice(USER_AGENTS)}) as s:
            async with s.get(url, allow_redirects=True, timeout=15) as r:
                final = str(r.url)
                p = urlparse(final); q = parse_qs(p.query)
                q.update({"tag": [AMAZON_TAG]})
                return urlunparse(p._replace(query=urlencode(q, doseq=True)))
    except: return url

async def motor_shopee(url):
    ts = str(int(time.time()))
    payload = json.dumps({"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{url}" }}) {{ shortLink }} }}'}, separators=(",", ":"))
    sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
    h = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post("https://open-api.affiliate.shopee.com.br/graphql", data=payload, headers=h, timeout=12) as r:
                res = await r.json()
                if "data" in res and res["data"]: return res["data"]["generateShortLink"]["shortLink"]
                return url
    except: return url

async def converter_link_massivo(url):
    url_l = url.lower()
    if "amazon.com" in url_l or "amzn.to" in url_l: return await motor_amazon(url), "amazon"
    if "shopee.com" in url_l or "s.shopee" in url_l: return await motor_shopee(url), "shopee"
    if any(x in url_l for x in ["cadastro", "ganhe", "promo", "formulario"]): return url, "info"
    return None, None

# ============================================================
# 🔹 MÓDULO 4: FORMATAÇÃO (CÓPIA FIEL + DETECÇÃO DE CUPOM)
# ============================================================
def formatar_texto_lamborghini(texto_original, links_conv):
    # Remove qualquer label manual antiga e links originais do texto
    texto = re.sub(r'(?i)(produto|preço|cupom|valor|oferta):\s*', '', texto_original)
    
    keywords_cupom = ["cupom", "off", "resgate", "carrinho", "ganhe"]
    eh_cupom_post = any(x in texto.lower() for x in keywords_cupom)

    linhas = texto.split('\n')
    novas_linhas = []
    links_originais = re.findall(r'https?://\S+', texto)

    for i, linha in enumerate(linhas):
        if any(link in linha for link in links_originais): continue
            
        conteudo = linha.strip()
        if not conteudo:
            novas_linhas.append(linha)
            continue

        # Lógica de Emoji para cada linha (Respeita traços no início)
        # Se a linha começa com Letra, Número ou Traço (-)
        if conteudo[0].isalnum() or conteudo.startswith('-'):
            # PRIORIDADE 1: Se a linha fala de CUPOM ou DESCONTO (Independente de onde vier)
            if any(x in conteudo.lower() for x in ["cupom", "off", "resgate"]):
                linha = "🎟" + linha
            # PRIORIDADE 2: Título da Oferta
            elif i == 0:
                linha = ("🔥" if eh_cupom_post else "✅") + linha
            # PRIORIDADE 3: Preço
            elif "R$" in conteudo:
                linha = ("💵" if eh_cupom_post else "🔥") + linha
        
        novas_linhas.append(linha)

    corpo_limpo = "\n".join(novas_linhas).rstrip()
    return corpo_limpo + "\n\n" + "\n".join(links_conv)

async def buscar_imagem_3x(url):
    for _ in range(3):
        try:
            async with aiohttp.ClientSession(headers={"User-Agent": random.choice(USER_AGENTS)}) as s:
                async with s.get(url, timeout=10) as r:
                    soup = BeautifulSoup(await r.text(), 'html.parser')
                    img = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "twitter:image"})
                    if img: return img['content']
        except: await asyncio.sleep(1)
    return None

# ============================================================
# 🔹 MÓDULO 5: MOTOR DE EVENTOS
# ============================================================
async def processar_evento(event, is_edit=False):
    texto = event.message.text or ""
    if not texto.strip() or any(p.lower() in texto.lower() for p in FILTRO): return
    
    chat = await event.get_chat()
    username = (chat.username or "").lower()
    links_raw = re.findall(r'https?://\S+', texto)
    if not links_raw and username != GRUPO_CUPONS_ONLY: return

    # Conversão Paralela de até 50 links
    tarefas = [converter_link_massivo(l) for l in links_raw[:50]]
    resultados = await asyncio.gather(*tarefas)
    links_conv = [r[0] for r in resultados if r[0]]
    plataforma_p = "shopee" if any(r[1] == "shopee" for r in resultados) else "amazon"
    
    if links_raw and not links_conv: return 

    prod_id = re.search(r'/(?:dp|product|i\.)/([A-Z0-9.\-_]+)', links_conv[0]).group(1) if links_conv and re.search(r'/(?:dp|product|i\.)/([A-Z0-9.\-_]+)', links_conv[0]) else "0"
    preco = re.search(r'R\$\s?\d+[.,\d]*', texto).group(0) if re.search(r'R\$\s?\d+[.,\d]*', texto) else "0"
    cupom = re.search(r'\b([A-Z0-9]{4,20})\b', texto).group(1) if re.search(r'\b([A-Z0-9]{4,20})\b', texto) else ""

    if not is_edit and not deve_enviar_oferta(plataforma_p, prod_id, preco, cupom, texto): return

    final_msg = formatar_texto_lamborghini(texto, links_conv)
    tem_media = event.message.media and not isinstance(event.message.media, MessageMediaWebPage)
    
    if username == GRUPO_CUPONS_ONLY:
        imagem = event.message.media if tem_media else (IMG_AMAZON if plataforma_p == "amazon" else IMG_SHOPEE)
    elif tem_media: imagem = event.message.media
    elif links_conv: imagem = await buscar_imagem_3x(links_conv[0])

    async with envio_lock:
        try:
            mapa = json.load(open(ARQUIVO_MAPEAMENTO, "r")) if os.path.exists(ARQUIVO_MAPEAMENTO) else {}
            if is_edit and str(event.message.id) in mapa:
                try: await client.edit_message(GRUPO_DESTINO, mapa[str(event.message.id)], final_msg)
                except MessageNotModifiedError: pass
            else:
                sent = None
                if imagem:
                    if len(final_msg) > 1024:
                        f = await client.send_file(GRUPO_DESTINO, imagem)
                        sent = await client.send_message(GRUPO_DESTINO, final_msg, reply_to=f.id)
                    else: sent = await client.send_file(GRUPO_DESTINO, imagem, caption=final_msg)
                else: sent = await client.send_message(GRUPO_DESTINO, final_msg)
                if sent:
                    mapa[str(event.message.id)] = sent.id
                    json.dump(mapa, open(ARQUIVO_MAPEAMENTO, "w"))
        except Exception as e: log_sys.error(f"Erro: {e}")

# ============================================================
# 🔹 START
# ============================================================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
async def main():
    await client.connect()
    if not await client.is_user_authorized(): return
    log_sys.info("🚀 FOGUETÃO v10.0 ONLINE - CUPONS DETECTADOS!")
    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def n_h(e): await processar_evento(e)
    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def e_h(e): await processar_evento(e, is_edit=True)
    await client.run_until_disconnected()
if __name__ == '__main__': asyncio.run(main())

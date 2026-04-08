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
# 🔹 MÓDULO 1: DEBUG & LOGGING PROFISSIONAL
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

GRUPOS_ORIGEM = ['promotom', 'fumotom', 'botofera', 'fadadoscupons']
GRUPO_DESTINO = '@ofertap'

AMAZON_TAG = "leo21073-20"
SHOPEE_APP_ID = "18348480261"
SHOPEE_SECRET = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"

IMG_AMAZON = "cupom-amazon.jpg" 
IMG_SHOPEE = "IMG_20260404_180150.jpg"

ARQUIVO_CACHE = "cache_dedup_profissional.json"
ARQUIVO_MAPEAMENTO = "map_mensagens_edicao.json"

envio_lock = asyncio.Semaphore(5)

FILTRO = [
    "Monitor Samsung", "Fonte Mancer", "Placa de video", "Monitor LG", "PC home Essential", 
    "Suporte articulado", "Gabinetes em oferta", "VHAGAR", "Superframe", "AM5", "AM4", 
    "GTX", "DDR5", "DDR4", "Dram", "Monitor Safe", "Monitor Redragon", "CL18", "CL16", "CL32",
    "MT/s", "MHz", "RX 580", "Ryzen", "Placa Mãe", "Gabinete Gamer", "Water Cooler"
]

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"]

# ============================================================
# 🔹 MÓDULO 2: DEDUPLICAÇÃO PROFISSIONAL (SUA LÓGICA)
# ============================================================
LOCK_DEDUP = Lock()
TTL_SEGUNDOS = 120 * 60
JANELA_ANTISPAM_SEGUNDOS = 900
SIMILARIDADE_MINIMA = 0.90
PALAVRAS_RUIDO = {"promo", "promocao", "promoção", "oferta", "desconto", "cupom", "corre", "aproveita", "urgente", "gratis", "frete", "hoje", "agora"}

def remover_acentos(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def normalizar_texto_raiz(texto):
    if not texto: return ""
    texto = remover_acentos(texto.lower())
    texto = re.sub(r"http\S+|www\S+|[^\w\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return " ".join(sorted([p for p in texto.split() if p not in PALAVRAS_RUIDO]))

def deve_enviar_oferta(plataforma, produto_id, preco, cupom="", texto=""):
    with LOCK_DEDUP:
        try: cache = json.load(open(ARQUIVO_CACHE, "r")) if os.path.exists(ARQUIVO_CACHE) else {}
        except: cache = {}
        agora = time.time()
        cache = {k: v for k, v in cache.items() if agora - v["timestamp"] < TTL_SEGUNDOS}
        t_raiz = normalizar_texto_raiz(texto)
        h_exato = hashlib.sha256(f"{plataforma}|{produto_id}|{preco}|{cupom.lower()}|{t_raiz}".encode()).hexdigest()
        if h_exato in cache: return False
        for oferta in cache.values():
            if str(oferta["produto_id"]) == str(produto_id) and str(oferta["preco"]) == str(preco) and \
               oferta["cupom"] == str(cupom).lower() and SequenceMatcher(None, t_raiz, oferta["texto"]).ratio() >= SIMILARIDADE_MINIMA:
                return False
        cache[h_exato] = {"produto_id": str(produto_id), "preco": str(preco), "cupom": str(cupom).lower(), "texto": t_raiz, "timestamp": agora, "plataforma": plataforma}
        json.dump(cache, open(ARQUIVO_CACHE, "w"), indent=2)
        return True

# ============================================================
# 🔹 MÓDULO 3: MOTORES DE CONVERSÃO
# ============================================================
async def motor_amazon(url):
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": random.choice(USER_AGENTS)}) as s:
            async with s.get(url, allow_redirects=True, timeout=15) as r:
                html = await r.text()
                soup = BeautifulSoup(html, 'html.parser')
                refresh = soup.find('meta', attrs={'http-equiv': 'refresh'})
                url_f = str(r.url)
                if refresh:
                    match = re.search(r'url=(.*)', refresh['content'], re.I)
                    if match: url_f = match.group(1)
                p = urlparse(url_f); q = parse_qs(p.query)
                q.update({"tag": [AMAZON_TAG]})
                log_amz.info(f"✅ Amazon Descascada")
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
                link = res["data"]["generateShortLink"]["shortLink"]
                log_shp.info(f"✅ Shopee Convertida")
                return link
    except: return url

async def converter_geral(url):
    url_l = url.lower()
    if "amazon.com" in url_l or "amzn.to" in url_l: return await motor_amazon(url), "amazon"
    if "shopee.com" in url_l or "s.shopee" in url_l: return await motor_shopee(url), "shopee"
    if any(x in url_l for x in ["cadastro", "ganhe", "promo", "formulario"]): return url, "info"
    return None, None

# ============================================================
# 🔹 MÓDULO 4: FORMATAÇÃO (O SEGREDO DA CÓPIA FIEL)
# ============================================================
def formatar_texto_fiel(texto_original, map_links, eh_cupom):
    # 1. Remove labels (Produto:, Preço:, etc) preservando o texto à frente
    texto = re.sub(r'(?i)^(produto|preço|cupom|valor|oferta|link|resgate)[:\s-]*', '', texto_original, flags=re.MULTILINE)
    
    # 2. Substitui os links velhos pelos novos NO LUGAR ORIGINAL
    for link_velho, link_novo in map_links.items():
        texto = texto.replace(link_velho, link_novo)

    # 3. Injeção de Emojis mantendo a estrutura original
    linhas = texto.split('\n')
    novas_linhas = []
    for i, linha in enumerate(linhas):
        cont = linha.strip()
        if not cont:
            novas_linhas.append("")
            continue
        # Se a linha começa com texto/número/traço e não tem emoji, coloca um
        if re.match(r'^[a-zA-Z0-9\-]', cont):
            if i == 0: linha = ("🔥 " if eh_cupom else "✅ ") + cont
            elif "R$" in cont: linha = ("💵 " if eh_cupom else "🔥 ") + cont
            elif any(x in cont.lower() for x in ["cupom", "off", "resgate"]): linha = "🎟 " + cont
        novas_linhas.append(linha)

    return "\n".join(novas_linhas).strip()

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
# 🔹 MÓDULO 5: PROCESSAMENTO E EDIÇÃO
# ============================================================
async def processar_evento(event, is_edit=False):
    texto_bruto = event.message.text or ""
    if not texto_bruto.strip() or any(p.lower() in texto_bruto.lower() for p in FILTRO): return
    
    chat = await event.get_chat(); username = (chat.username or "").lower()
    links_raw = re.findall(r'https?://\S+', texto_bruto)
    if not links_raw and username != GRUPO_CUPONS_EXCLUSIVO: return

    # Conversão Paralela Massiva (Fiel ao lugar original)
    map_links = {}
    plat_p = "amazon"
    tarefas = [converter_geral(l) for l in links_raw[:50]]
    resultados = await asyncio.gather(*tarefas)
    
    for i, (novo, plat) in enumerate(resultados):
        if novo:
            map_links[links_raw[i]] = novo
            if plat != "info": plat_p = plat

    if links_raw and not map_links: return

    # Extração Fingerprint
    prod_id = "0"
    if map_links:
        first = list(map_links.values())[0]
        m = re.search(r'/(?:dp|product|i\.)/([A-Z0-9.\-_]+)', first)
        prod_id = m.group(1) if m else first[-15:]

    preco = re.search(r'R\$\s?\d+[.,\d]*', texto_bruto).group(0) if re.search(r'R\$\s?\d+[.,\d]*', texto_bruto) else "0"
    cupom = re.search(r'\b([A-Z0-9]{4,20})\b', texto_bruto).group(1) if re.search(r'\b([A-Z0-9]{4,20})\b', texto_bruto) else ""

    if not is_edit and not deve_enviar_oferta(plat_p, prod_id, preco, cupom, texto_bruto): return

    eh_cupom = any(x in texto_bruto.lower() for x in ["cupom", "off", "resgate", "carrinho"])
    final_msg = formatar_texto_fiel(texto_bruto, map_links, eh_cupom)
    
    tem_media = event.message.media and not isinstance(event.message.media, MessageMediaWebPage)
    imagem = None
    if username == GRUPO_CUPONS_EXCLUSIVO:
        imagem = event.message.media if tem_media else (IMG_AMAZON if plat_p == "amazon" else IMG_SHOPEE)
    elif tem_media: imagem = event.message.media
    elif map_links: imagem = await buscar_imagem_3x(list(map_links.values())[0])

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
                        f = await client.send_file(GRUPO_DESTINO, imagem); sent = await client.send_message(GRUPO_DESTINO, final_msg, reply_to=f.id)
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
    log_sys.info("🚀 FOGUETÃO v44.0 ONLINE - CÓPIA FIEL ATIVADA!")
    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def n_h(e): await processar_evento(e)
    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def e_h(e): await processar_evento(e, is_edit=True)
    await client.run_until_disconnected()
if __name__ == '__main__': asyncio.run(main())

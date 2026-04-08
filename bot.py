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
# 🔹 MÓDULO 1: SISTEMA DE LOGS (DEBUG INDIVIDUAL)
# ============================================================
def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(f'[%(name)s] %(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

log_amz = get_logger('AMAZON')
log_shp = get_logger('SHOPEE')
log_sys = get_logger('SISTEMA')

# ============================================================
# 🔹 CONFIGURAÇÕES E VARIÁVEIS
# ============================================================
API_ID = 33768893
API_HASH = '7959ea0392ff7f91b4f7e207e75a1813'
SESSION_STRING = os.environ.get("TELEGRAM_SESSION")

# CANAIS ORIGEM POR TIPO
GRUPOS_OFERTAS = ['promotom', 'fumotom']
GRUPO_CUPONS = 'fadadoscupons'
GRUPOS_ORIGEM = GRUPOS_OFERTAS + [GRUPO_CUPONS]
GRUPO_DESTINO = '@ofertap'

# AFILIADOS
AMAZON_TAG = "leo21073-20"
SHOPEE_APP_ID = "18348480261"
SHOPEE_SECRET = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"

# ARQUIVOS E IMAGENS
IMG_FIXA_ML = "mercado_livre_c1a918503a.jpg"
ARQUIVO_CACHE = "cache_dedup_profissional.json"
ARQUIVO_MAPEAMENTO = "map_mensagens_edicao.json"

# FILTRO BLACKLIST
FILTRO = ["Monitor Samsung", "VHAGAR", "Monitor Safe", "Monitor Redragon", "Dram"]

# BYPASS DE FLOOD (Semáforo de processamento)
envio_lock = asyncio.Semaphore(3)

# ============================================================
# 🔹 MÓDULO 2: DEDUPLICAÇÃO PROFISSIONAL (SUA LÓGICA)
# ============================================================
LOCK = Lock()
TTL_SEGUNDOS = 120 * 60
JANELA_ANTISPAM = 900
SIMILARIDADE_MINIMA = 0.90
PALAVRAS_RUIDO = {"promo", "promocao", "promoção", "oferta", "desconto", "cupom", "corre", "aproveita", "urgente", "gratis", "frete", "hoje", "agora"}

def normalizar_texto(texto):
    if not texto: return ""
    texto = "".join(c for c in unicodedata.normalize('NFD', texto.lower()) if unicodedata.category(c) != 'Mn')
    texto = re.sub(r"http\S+|www\S+|[^\w\s]", " ", texto)
    tokens = sorted([p for p in texto.split() if p not in PALAVRAS_RUIDO])
    return " ".join(tokens)

def deve_enviar_oferta(plataforma, produto_id, preco, cupom="", texto=""):
    with LOCK:
        if not os.path.exists(ARQUIVO_CACHE): cache = {}
        else:
            try: cache = json.load(open(ARQUIVO_CACHE, "r"))
            except: cache = {}
        
        agora = time.time()
        cache = {k: v for k, v in cache.items() if agora - v["timestamp"] < TTL_SEGUNDOS}
        texto_norm = normalizar_texto(texto)
        
        h = hashlib.sha256(f"{plataforma}|{produto_id}|{preco}|{cupom.lower()}|{texto_norm}".encode()).hexdigest()
        if h in cache: return False

        for oferta in cache.values():
            if str(oferta["produto_id"]) == str(produto_id) and str(oferta["preco"]) == str(preco) and \
               oferta["cupom"] == str(cupom).lower() and (agora - oferta["timestamp"] < JANELA_ANTISPAM) and \
               SequenceMatcher(None, texto_norm, oferta["texto"]).ratio() >= SIMILARIDADE_MINIMA:
                return False

        cache[h] = {"produto_id": str(produto_id), "preco": str(preco), "cupom": str(cupom).lower(), 
                    "texto": texto_norm, "timestamp": agora, "plataforma": plataforma}
        json.dump(cache, open(ARQUIVO_CACHE, "w"), indent=2)
        return True

# ============================================================
# 🔹 MÓDULO 3: MOTORES DE CONVERSÃO (ULTRA FAST)
# ============================================================

async def motor_amazon(url):
    """Simula Redirect JavaScript e descasca o link até o osso."""
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as s:
            async with s.get(url, allow_redirects=True, timeout=12) as r:
                url_final = str(r.url)
                log_amz.info(f"Link Descascado: {url_final}")
                parsed = urlparse(url_final)
                qs = parse_qs(parsed.query)
                qs.update({"tag": [AMAZON_TAG]})
                return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
    except Exception as e:
        log_amz.error(f"Erro no motor Amazon: {e}")
        return url

async def motor_shopee(url):
    """API GraphQL com Assinatura e Debug Seletivo."""
    ts = str(int(time.time()))
    payload = json.dumps({"query": f'mutation {{ generateShortLink(input: {{ originUrl: "{url}" }}) {{ shortLink }} }}'}, separators=(",", ":"))
    sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
    h = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post("https://open-api.affiliate.shopee.com.br/graphql", data=payload, headers=h, timeout=10) as r:
                res = await r.json()
                if "data" in res and res["data"]:
                    link = res["data"]["generateShortLink"]["shortLink"]
                    log_shp.info(f"Convertido: {link}")
                    return link
                log_shp.warning(f"Resposta inválida da API: {res}")
                return url
    except Exception as e:
        log_shp.error(f"Erro no motor Shopee: {e}")
        return url

async def converter_geral(url):
    """Filtra links massivos. Só aceita Shopee, Amazon ou Cadastro."""
    url_l = url.lower()
    if "amazon" in url_l or "amzn" in url_l: return await motor_amazon(url), "amazon"
    if "shopee" in url_l: return await motor_shopee(url), "shopee"
    if any(x in url_l for x in ["cadastro", "ganhe", "promo", "formulario"]): return url, "info"
    return None, None

# ============================================================
# 🔹 MÓDULO 4: FORMATAÇÃO E MÍDIA
# ============================================================

def formatar_texto(texto, links_conv):
    # Limpa nomes Produto/Preço/etc
    texto = re.sub(r'(?i)(produto|preço|cupom|valor):\s*', '', texto)
    # Craser nos cupons
    texto = re.sub(r'\b([A-Z0-9]{4,20})\b', r'`\1`', texto)
    linhas = [l.strip() for l in texto.split('\n') if l.strip()]
    novas = []
    for i, linha in enumerate(linhas):
        if not re.match(r'[^\w\s]', linha): # Se não tem emoji coloca
            if i == 0: linha = "✅ " + linha
            elif "R$" in linha: linha = "🔥 " + linha
            elif "`" in linha: linha = "🎟 " + linha
        novas.append(linha)
    corpo = re.sub(r'https?://\S+', '', "\n".join(novas)).strip()
    return corpo + "\n\n" + "\n".join(links_conv)

async def buscar_imagem_3x(url):
    for _ in range(3):
        try:
            async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as s:
                async with s.get(url, timeout=8) as r:
                    soup = BeautifulSoup(await r.text(), 'html.parser')
                    img = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "twitter:image"})
                    if img: return img['content']
        except: await asyncio.sleep(1)
    return None

# ============================================================
# 🔹 MÓDULO 5: PROCESSAMENTO E EDIÇÃO
# ============================================================

async def gerenciar_evento(event, is_edit=False):
    texto = event.message.text or ""
    if not texto.strip() or any(p.lower() in texto.lower() for p in FILTRO): return
    
    chat = await event.get_chat()
    username = (chat.username or "").lower()
    links_detectados = re.findall(r'https?://\S+', texto)
    
    # Bloqueio de segurança: oferta sem link (exceto grupo cupons)
    if not links_detectados and username != GRUPO_CUPONS: return

    # Conversão de Multilinks paralela (Até 50 links escorregando)
    tarefas = [converter_geral(l) for l in links_detectados[:50]]
    resultados = await asyncio.gather(*tarefas)
    
    links_conv = [r[0] for r in resultados if r[0]]
    plataforma_p = "shopee" if any(r[1] == "shopee" for r in resultados) else "amazon"
    
    # Se tinha links mas nenhum prestava, deleta o post
    if links_detectados and not links_conv: return

    # Extração de IDs para deduplicação profissional
    prod_id = "0"
    if links_conv:
        m = re.search(r'/(?:dp|product|i\.)/([A-Z0-9.\-_]+)', links_conv[0])
        prod_id = m.group(1) if m else links_conv[0][-15:]

    preco = re.search(r'R\$\s?\d+[.,\d]*', texto).group(0) if re.search(r'R\$\s?\d+[.,\d]*', texto) else "0"
    cupom = re.search(r'\b([A-Z0-9]{4,20})\b', texto).group(1) if re.search(r'\b([A-Z0-9]{4,20})\b', texto) else ""

    if not is_edit and not deve_enviar_oferta(plataforma_p, prod_id, preco, cupom, texto):
        log_sys.info(f"🚫 Duplicado: {prod_id}")
        return

    final_msg = formatar_texto(texto, links_conv)
    
    # Regra de Imagem Lamborghini
    imagem = None
    tem_midia = event.message.media and not isinstance(event.message.media, MessageMediaWebPage)
    
    if username == GRUPO_CUPONS:
        imagem = event.message.media if tem_midia else IMG_FIXA_ML
    elif tem_midia:
        imagem = event.message.media
    elif links_conv:
        imagem = await buscar_imagem_3x(links_conv[0])

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
                    log_sys.info("✅ Oferta enviada escorregando!")
        except Exception as e: log_sys.error(f"Erro envio: {e}")

# ============================================================
# 🔹 START
# ============================================================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

async def main():
    await client.connect()
    if not await client.is_user_authorized(): return
    log_sys.info("🚀 LAMBORGHINI v2.0 ONLINE - ACELERA LEO!")

    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def n_h(e): await gerenciar_evento(e)

    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def e_h(e): await gerenciar_evento(e, is_edit=True)

    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())

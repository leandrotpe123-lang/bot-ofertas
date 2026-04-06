import os, re, requests, json, time, hashlib, sqlite3, logging
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from bs4 import BeautifulSoup
from datetime import datetime

# --- LOGS DE SISTEMA ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
log_sys = logging.getLogger(__name__)

# --- CONFIGURAÇÕES TELEGRAM ---
api_id = 33768893
api_hash = '7959ea0392ff7f91b4f7e207e75a1813'
# StringSession para não deslogar na nuvem
session_str = os.environ.get('SESSION_STRING', '') 
client = TelegramClient(StringSession(session_str), api_id, api_hash)

GRUPO_DESTINO = '@ofertap'
GRUPOS_ORIGEM = [
    'https://t.me/botofera', 'https://t.me/promotom', 'https://t.me/ninjaofertas',
    'https://t.me/fadadoscupons', 'https://t.me/SamuelF3lipePromo',
    'https://t.me/fumotom', 'https://t.me/fadapromos', 'https://t.me/paraseubaby'
]

# --- CREDENCIAIS (SEUS DADOS ORIGINAIS) ---
AMAZON_TAG      = "leo21073-20"
SHOPEE_APP_ID   = "18348480261"
SHOPEE_SECRET   = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"

# --- IMAGENS DE CUPOM (CONFORME SEU PEDIDO) ---
IMG_CUPOM_AMAZON = "https://files.catbox.moe/myf04b.jpg"
IMG_CUPOM_SHOPEE = "https://files.catbox.moe/u1ebbh.jpg"

# --- FILTRO DE PALAVRAS COMPLETO ---
filtro_palavras = [
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
    "DIMM", "DDR5", "DDR4", "Dram",
    "shopee video", "shopee vídeo", "somente nos vídeos"
]

# --- BANCO DE DADOS (DEDUPLICAÇÃO BLINDADA) ---
def init_db():
    conn = sqlite3.connect('tanque_de_guerra.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS ofertas 
                 (hash TEXT PRIMARY KEY, data TEXT)''')
    conn.commit()
    return conn
db = init_db()

# --- FUNÇÕES DE CONVERSÃO ORIGINAIS (FILÉ) ---

def converter_amazon(url):
    try:
        url = url.strip()
        if "amzn.to" in url.lower():
            r = requests.get(url, allow_redirects=True, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            url = r.url
        url = url.split("#")[0]
        base = url.split("?", 1)[0]
        node = re.search(r'node=([^&]+)', url)
        # Lógica original preservada
        return f"{base}?node={node.group(1)}&tag={AMAZON_TAG}" if node else f"{base}?tag={AMAZON_TAG}"
    except: return url

def converter_shopee(url):
    try:
        url_real = url.strip()
        query = f'mutation {{ generateShortLink(input: {{ originUrl: "{url_real}" }}) {{ shortLink }} }}'
        payload = json.dumps({"query": query}, separators=(",", ":"))
        ts = str(int(time.time()))
        # Assinatura SHA256 Original
        sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
        headers = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
        r = requests.post("https://open-api.affiliate.shopee.com.br/graphql", headers=headers, data=payload, timeout=10).json()
        return r['data']['generateShortLink']['shortLink']
    except: return url

# --- BUSCA DE IMAGEM (3 TENTATIVAS) ---
def buscar_foto_no_site(texto, tentativa=1):
    links = re.findall(r'https?://[^\s\)\]\>\"]+', texto)
    for l in links:
        try:
            r = requests.get(l, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(r.text, 'html.parser')
            og = soup.find("meta", property="og:image")
            if og: return og["content"]
        except:
            if tentativa < 3:
                time.sleep(1)
                return buscar_foto_no_site(texto, tentativa + 1)
    return None

# --- DEDUPLICAÇÃO (IDENTIDADE ÚNICA) ---
def gerar_hash_blindado(texto):
    # Ignora emojis, links e quem enviou para comparar apenas o produto e preço
    t = re.sub(r'https?://\S+', '', texto).lower()
    t = re.sub(r'^.*?:', '', t).strip()
    t = re.sub(r'[^a-z0-9]', '', t)
    preco = "".join(re.findall(r'\d+', t)[:2])
    # Tenta cupom
    cp = ""
    match = re.search(r'(?:CUPOM|🏷️|CODIGO):\s*([A-Z0-9]{4,15})', texto.upper())
    if match: cp = match.group(1)
    return hashlib.md5(f"{t[:40]}_{preco}_{cp}".encode()).hexdigest()

# ============================================================
# 🔹 EVENTO PRINCIPAL (ZERO ERROS)
# ============================================================

@client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
async def handler(event):
    raw_text = event.message.text
    if not raw_text or not raw_text.strip(): return

    # 1. FILTRO DE PALAVRAS BLOQUEADAS
    if any(p.lower() in raw_text.lower() for p in filtro_palavras):
        print(f"DEBUG | Bloqueado pelo filtro: {raw_text[:20]}...")
        return

    # 2. IDENTIFICA PLATAFORMA
    plat = "AMZ" if ("amazon" in raw_text.lower() or "amzn" in raw_text.lower()) else "SHP" if "shopee" in raw_text.lower() else None
    if not plat: return

    # 3. DEDUPLICAÇÃO (SQLITE)
    h = gerar_hash_blindado(raw_text)
    cursor = db.cursor()
    cursor.execute('SELECT hash FROM ofertas WHERE hash=?', (h,))
    if cursor.fetchone(): return # Já enviado

    # 4. CONVERSÃO DE LINKS E CUPOM
    links = re.findall(r'https?://[^\s\)\]\>\"]+', raw_text)
    texto_final = raw_text
    for l in links:
        novo = converter_amazon(l) if plat == "AMZ" else converter_shopee(l)
        texto_final = texto_final.replace(l, novo)
    
    # Aplicar Crases no Cupom
    match_cp = re.search(r'(?:CUPOM|🏷️|CODIGO):\s*([A-Z0-9]{4,15})', texto_final.upper())
    if match_cp:
        cp = match_cp.group(1)
        if f'`{cp}`' not in texto_final:
            texto_final = re.sub(re.escape(cp), f"`{cp}`", texto_final, flags=re.I, count=1)

    # 5. LÓGICA DE ENVIO DE IMAGENS
    is_cupom = any(x in raw_text.lower() for x in ['cupom', '🏷️', '🎟️', 'vale', 'off'])
    try:
        if event.message.photo: # 1ª Prioridade: Foto original
            await client.send_file(GRUPO_DESTINO, event.message.photo, caption=texto_final)
        elif not is_cupom: # 2ª Prioridade (Oferta): Busca site
            img = buscar_foto_no_site(raw_text)
            if img: await client.send_file(GRUPO_DESTINO, img, caption=texto_final)
            else: await client.send_message(GRUPO_DESTINO, texto_final) # Fallback: Link Preview
        else: # 3ª Prioridade (Cupom): Imagem Fixa (Sua ordem correta)
            img_fixa = IMG_CUPOM_AMAZON if plat == "AMZ" else IMG_CUPOM_SHOPEE
            await client.send_file(GRUPO_DESTINO, img_fixa, caption=texto_final)

        # SALVA NO BANCO PARA DEDUP PERMANENTE
        cursor.execute('INSERT INTO ofertas (hash, data) VALUES (?, ?)', (h, datetime.now().isoformat()))
        db.commit()
        print(f"✅ Enviado com Sucesso: {plat}")

    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")

# INÍCIO DO ROBÔ
print("🚀 BOT TANQUE DE GUERRA INICIADO - AMAZON E SHOPEE ONLINE")
client.start()
client.run_until_disconnected()

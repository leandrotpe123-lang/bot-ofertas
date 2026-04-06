import re, requests, json, time, hashlib, os
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from datetime import datetime
from bs4 import BeautifulSoup

# ============================================================
# 🔹 CONFIGURAÇÕES GERAIS
# ============================================================
api_id = 33768893
api_hash = '7959ea0392ff7f91b4f7e207e75a1813'
session_string = os.environ.get('SESSION_STRING', '')
client = TelegramClient(StringSession(session_string), api_id, api_hash)

grupo_destino = '@ofertap'
grupos_origem = [
    'https://t.me/botofera', 'https://t.me/promotom',
    'https://t.me/ninjaofertas', 'https://t.me/fadadoscupons',
    'https://t.me/SamuelF3lipePromo', 'https://t.me/fumotom',
    'https://t.me/fadapromos', 'https://t.me/paraseubaby'
]

# IMAGENS FIXAS PARA CUPONS (SEUS LINKS)
IMG_CUPOM_SHOPEE = "https://files.catbox.moe/myf04b.jpg"
IMG_CUPOM_AMAZON = "https://files.catbox.moe/u1ebbh.jpg"

# AFILIADOS
AMAZON_TAG      = "leo21073-20"
SHOPEE_APP_ID   = "18348480261"
SHOPEE_SECRET   = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"

DB_FILE = 'db_v6.json'
if os.path.exists(DB_FILE):
    with open(DB_FILE, 'r') as f: enviados = json.load(f)
else: enviados = {}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ============================================================
# 🔹 FILTROS E UTILITÁRIOS
# ============================================================
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
    "DIMM", "DDR5", "DDR4", "Dram"
]

def eh_cupom(texto):
    # Identifica se a mensagem é um cupom genérico ou oferta de produto
    palavras_cupom = ['cupom', 'resgate', '🏷️', '🎟️', 'vale', 'off em compras']
    return any(p in texto.lower() for p in palavras_cupom)

def gerar_hash(texto):
    t = re.sub(r'https?://\S+', '', texto) # Tira links
    t = re.sub(r'[^\w\s]', '', t).lower() # Tira emojis/pontos
    
    precos = re.findall(r'\d+', t)
    preco = precos[0] if precos else "0"
    
    # Pega código do cupom se houver
    cupom = "0"
    match = re.search(r'(?:CUPOM|🏷️|CODIGO):\s*([A-Z0-9]+)', texto.upper())
    if match: cupom = match.group(1)
    
    palavras = t.split()
    nome = "".join(palavras[:4]) if len(palavras) >= 4 else "item"
    
    return hashlib.md5(f"{nome}_{preco}_{cupom}".encode()).hexdigest()

# ============================================================
# 🔹 BUSCA DE IMAGEM ORIGINAL (OFERTAS)
# ============================================================
def buscar_imagem_produto(texto, tentativa=1):
    links = re.findall(r'https?://[^\s\)\]\>\"]+', texto)
    for link in links:
        try:
            r = requests.get(link, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(r.text, 'html.parser')
            og = soup.find("meta", property="og:image")
            if og: return og["content"]
        except:
            if tentativa < 3:
                time.sleep(1)
                return buscar_imagem_produto(texto, tentativa + 1)
    return None

# ============================================================
# 🔹 CONVERSORES
# ============================================================
def conv_shopee(url):
    try:
        query = f'mutation {{generateShortLink(input: {{originUrl: "{url.strip()}"}}) {{shortLink}}}}'
        payload = json.dumps({"query": query}, separators=(",", ":"))
        ts = str(int(time.time()))
        sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
        headers = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
        r = requests.post("https://open-api.affiliate.shopee.com.br/graphql", headers=headers, data=payload, timeout=10).json()
        return r['data']['generateShortLink']['shortLink']
    except: return url

def conv_amazon(url):
    try:
        if "amzn.to" in url.lower():
            r = requests.get(url, allow_redirects=True, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            url = r.url
        base = url.split("?")[0].split("#")[0]
        return f"{base}?tag={AMAZON_TAG}"
    except: return url

# ============================================================
# 🔹 EVENTO DE MENSAGEM
# ============================================================

@client.on(events.NewMessage(chats=grupos_origem))
async def handler(event):
    texto = event.message.text or ""
    if not texto.strip() or any(p.lower() in texto.lower() for p in filtro_palavras): return

    plat = "shopee" if "shopee" in texto.lower() else "amazon" if ("amazon" in texto.lower() or "amzn" in texto.lower()) else None
    if not plat: return

    # Deduplicação
    h = gerar_hash(texto)
    if h in enviados and (time.time() - enviados[h] < 21600): return

    # Converter Links
    links = re.findall(r'https?://[^\s\)\]\>\"]+', texto)
    texto_final = texto
    for link in links:
        novo = conv_shopee(link) if plat == "shopee" else conv_amazon(link)
        texto_final = texto_final.replace(link, novo)

    # Lógica de Envio de Imagens
    is_cupom = eh_cupom(texto)
    try:
        if is_cupom:
            # LÓGICA CUPOM: Foto original ou sua imagem fixa
            foto_enviar = event.message.photo if event.message.photo else (IMG_CUPOM_SHOPEE if plat == "shopee" else IMG_CUPOM_AMAZON)
            await client.send_file(grupo_destino, foto_enviar, caption=texto_final)
        else:
            # LÓGICA OFERTA: Foto original ou busca no link (3 tentativas) ou Link Preview
            if event.message.photo:
                await client.send_file(grupo_destino, event.message.photo, caption=texto_final)
            else:
                img_busca = buscar_imagem_produto(texto)
                if img_busca:
                    await client.send_file(grupo_destino, img_busca, caption=texto_final)
                else:
                    # Se falhar tudo, manda link preview (texto puro)
                    await client.send_message(grupo_destino, texto_final)
        
        enviados[h] = time.time()
        with open(DB_FILE, 'w') as f: json.dump(enviados, f)
        log(f"✅ Enviado: {plat} | Tipo: {'Cupom' if is_cupom else 'Oferta'}")
        
    except Exception as e:
        log(f"❌ Erro: {e}")

log("🤖 Bot Online! Lógica de Imagens e Cupons configurada.")
client.start()
client.run_until_disconnected()

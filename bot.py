import os, re, requests, json, time, hashlib, sqlite3, logging
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from bs4 import BeautifulSoup
from datetime import datetime

# ============================================================
# 🔹 1. CONFIGURAÇÕES E LOGS
# ============================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
api_id = 33768893
api_hash = '7959ea0392ff7f91b4f7e207e75a1813'
# StringSession para Nuvem (Configurar na variável de ambiente SESSION_STRING)
session_str = os.environ.get('SESSION_STRING', '') 
client = TelegramClient(StringSession(session_str), api_id, api_hash)

GRUPO_DESTINO = '@ofertap'
GRUPOS_ORIGEM = [
    'https://t.me/botofera', 'https://t.me/promotom', 'https://t.me/ninjaofertas',
    'https://t.me/fadadoscupons', 'https://t.me/SamuelF3lipePromo',
    'https://t.me/fumotom', 'https://t.me/fadapromos', 'https://t.me/paraseubaby'
]

AMAZON_TAG      = "leo21073-20"
SHOPEE_APP_ID   = "18348480261"
SHOPEE_SECRET   = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"

# IMAGENS PARA CUPONS (SEUS LINKS CORRIGIDOS)
IMG_CUPOM_AMAZON = "https://files.catbox.moe/myf04b.jpg" # Foto do bolo/tag
IMG_CUPOM_SHOPEE = "https://files.catbox.moe/u1ebbh.jpg" # Foto shopee

# ============================================================
# 🔹 2. FILTRO DE PALAVRAS COMPLETO (RESTAURADO)
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
    "DIMM", "DDR5", "DDR4", "Dram",
    "shopee video", "shopee vídeo", "somente nos vídeos"
]

# ============================================================
# 🔹 3. BANCO DE DADOS (SQLITE - MEMÓRIA DE 24H)
# ============================================================
def iniciar_banco():
    conn = sqlite3.connect('tanque_de_guerra.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS ofertas 
                     (id_hash TEXT PRIMARY KEY, timestamp REAL)''')
    conn.commit()
    return conn

conexao_db = iniciar_banco()

def limpar_banco():
    """Apaga registros com mais de 24 horas"""
    limite = time.time() - 86400
    cursor = conexao_db.cursor()
    cursor.execute('DELETE FROM ofertas WHERE timestamp < ?', (limite,))
    conexao_db.commit()

# ============================================================
# 🔹 4. FUNÇÕES DE CONVERSÃO ORIGINAIS (FILÉ)
# ============================================================

def converter_amazon(url_original):
    try:
        url = url_original.strip()
        if "amzn.to" in url.lower():
            r = requests.get(url, allow_redirects=True, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            url = r.url
        base = url.split("#")[0].split("?", 1)[0]
        node = re.search(r'node=([^&]+)', url)
        # Mantém tag e node conforme seu script original
        return f"{base}?node={node.group(1)}&tag={AMAZON_TAG}" if node else f"{base}?tag={AMAZON_TAG}"
    except: return url_original

def converter_shopee(url_original):
    try:
        url_real = url_original.strip()
        query = f'mutation {{ generateShortLink(input: {{ originUrl: "{url_real}" }}) {{ shortLink }} }}'
        payload = json.dumps({"query": query}, separators=(",", ":"))
        ts = str(int(time.time()))
        # Assinatura SHA256 Profissional
        sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
        headers = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
        r = requests.post("https://open-api.affiliate.shopee.com.br/graphql", headers=headers, data=payload, timeout=10).json()
        return r["data"]["generateShortLink"]["shortLink"]
    except: return url_original

# ============================================================
# 🔹 5. UTILITÁRIOS (IMAGENS, CRASES E HASH)
# ============================================================

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
                time.sleep(1); return buscar_foto_no_site(texto, tentativa + 1)
    return None

def aplicar_crases(texto):
    """Detecta cupom e coloca crases automaticamente"""
    match = re.search(r'(?:CUPOM|🏷️|CODIGO):\s*([A-Z0-9]{4,15})', texto.upper())
    if match:
        cp = match.group(1)
        if f'`{cp}`' not in texto:
            return re.sub(re.escape(cp), f"`{cp}`", texto, flags=re.I, count=1)
    return texto

def gerar_hash_sensivel(texto):
    """Cria um ID baseado no texto. Se o texto mudar (ex: 'Ainda ativo'), o ID muda."""
    # Remove apenas os links (porque mudam de grupo para grupo)
    t = re.sub(r'https?://\S+', '', texto)
    # Remove emojis e mantém apenas letras/números para comparar o conteúdo real
    t = re.sub(r'[^\w\s]', '', t).lower().strip()
    return hashlib.md5(t.encode()).hexdigest()

# ============================================================
# 🔹 6. EVENTO PRINCIPAL (TANQUE DE GUERRA)
# ============================================================

@client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
async def handler(event):
    raw_text = event.message.text
    if not raw_text or not raw_text.strip(): return

    # A) Filtro de Palavras
    if any(p.lower() in raw_text.lower() for p in filtro_palavras):
        return

    # B) Plataforma
    plat = "AMAZON" if "amazon" in raw_text.lower() or "amzn" in raw_text.lower() else "SHOPEE" if "shopee" in raw_text.lower() else None
    if not plat: return

    # C) Deduplicação Sensível (Permite repostar se o texto mudar)
    limpar_banco()
    h = gerar_hash_sensivel(raw_text)
    
    cursor = conexao_db.cursor()
    cursor.execute('SELECT timestamp FROM ofertas WHERE id_hash=?', (h,))
    if cursor.fetchone():
        print("DEBUG | Oferta 100% idêntica ignorada.")
        return

    # D) Conversão de Links e Cupom
    links = re.findall(r'https?://[^\s\)\]\>\"]+', raw_text)
    texto_final = raw_text
    for l in links:
        novo = converter_amazon(l) if plat == "AMAZON" else converter_shopee(l)
        texto_final = texto_final.replace(l, novo)
    
    texto_final = aplicar_crases(texto_final)
    is_cupom = any(x in raw_text.lower() for x in ['cupom', '🏷️', '🎟️', 'vale', 'off'])

    # E) Lógica de Envio de Imagens (Ordem correta)
    try:
        if event.message.photo: # 1. Foto do grupo
            await client.send_file(GRUPO_DESTINO, event.message.photo, caption=texto_final)
        elif not is_cupom: # 2. Oferta: Busca foto no site
            img = buscar_foto_no_site(raw_text)
            if img: await client.send_file(GRUPO_DESTINO, img, caption=texto_final)
            else: await client.send_message(GRUPO_DESTINO, texto_final)
        else: # 3. Cupom: Imagem Fixa (Amazon: Bolo | Shopee: Shopee)
            img_fixa = IMG_CUPOM_AMAZON if plat == "AMAZON" else IMG_CUPOM_SHOPEE
            await client.send_file(GRUPO_DESTINO, img_fixa, caption=texto_final)

        # F) Grava no Banco para evitar repetição do MESMO texto
        cursor.execute('INSERT INTO ofertas (id_hash, timestamp) VALUES (?, ?)', (h, time.time()))
        conexao_db.commit()
        print(f"✅ Enviado: {plat} | Texto Novo detectado.")

    except Exception as e:
        print(f"❌ Erro: {e}")

# --- START ---
print("🚀 BOT TANQUE DE GUERRA ONLINE!")
print("Deduplicação Inteligente: Aceita reposts com textos diferentes.")
client.start()
client.run_until_disconnected()

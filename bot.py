# ============================================================
# BOT DE OFERTAS - TELEGRAM
# Plataformas: Amazon | Shopee
# Versão Profissional
# ============================================================

from telethon import TelegramClient, events
from telethon.sessions import StringSession
import re, requests, json, time, hashlib, os
from datetime import datetime
from bs4 import BeautifulSoup

api_id = 33768893
api_hash = '7959ea0392ff7f91b4f7e207e75a1813'
session = StringSession(os.environ.get('SESSION_STRING', ''))
client = TelegramClient(session, api_id, api_hash)

grupos_origem = [
    'https://t.me/botofera',
    'https://t.me/promotom',
    'https://t.me/ninjaofertas',
    'https://t.me/fadadoscupons',
    'https://t.me/SamuelF3lipePromo',
    'https://t.me/fumotom',
    'https://t.me/fadapromos',
    'https://t.me/paraseubaby',
]
grupo_destino = '@ofertap'

ARQUIVO_WHATSAPP = '/storage/emulated/0/oferta_whatsapp.txt'

AMAZON_TAG    = "leo21073-20"
SHOPEE_APP_ID = "18348480261"
SHOPEE_SECRET = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"

IMAGEM_SHOPEE = "https://files.catbox.moe/myf04b.jpg"
IMAGEM_AMAZON = "https://files.catbox.moe/u1ebbh.jpg"

enviados       = {}  # hash -> timestamp
mensagens_map  = {}  # msg_id_original -> msg_id_destino

TEMPO_DEDUP = 7200   # 2 horas
MAX_RETRIES = 3

# ============================================================
# LOG
# ============================================================
def log(msg):
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f"[{agora}] {msg}")

# ============================================================
# FILTROS
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

filtro_exclusivo = [
    "shopee video", "shopee vídeo", "somente nos vídeos",
    "vídeos da shopee", "videos da shopee",
    "cupom exclusivo", "link exclusivo", "exclusivo no link",
    "exclusivo pelo link", "válido somente nos vídeos",
    "valido somente nos videos"
]

def deve_bloquear(texto):
    texto_lower = texto.lower()
    for p in filtro_palavras:
        if p.lower() in texto_lower:
            log(f"Bloqueado pelo filtro: {p}")
            return True
    for p in filtro_exclusivo:
        if p in texto_lower:
            log(f"Bloqueado por exclusivo/video: {p}")
            return True
    return False

# ============================================================
# UTILITARIOS
# ============================================================
def extrair_links(texto):
    return re.findall(r'https?://[^\s\)\]\>\"]+', texto)

def tem_link_valido(texto):
    for link in extrair_links(texto):
        if "amazon" in link.lower() or "amzn" in link.lower():
            return True
        if "shopee" in link.lower():
            return True
    log("Bloqueado: nenhum link Amazon ou Shopee.")
    return False

def detectar_plataforma(texto):
    for link in extrair_links(texto):
        if "amazon" in link.lower() or "amzn" in link.lower():
            return "amazon"
        if "shopee" in link.lower():
            return "shopee"
    return None

def tem_emojis(texto):
    return bool(re.search(
        r'[\U00010000-\U0010ffff\U0001F600-\U0001F64F'
        r'\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF'
        r'\U0001F1E0-\U0001F1FF\u2600-\u26FF\u2700-\u27BF]+',
        texto
    ))

def remover_emojis(texto):
    return re.sub(
        r'[\U00010000-\U0010ffff\U0001F600-\U0001F64F'
        r'\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF'
        r'\U0001F1E0-\U0001F1FF\u2600-\u26FF\u2700-\u27BF'
        r'\u200d\uFE0F\u20E3\u3030]+',
        '', texto
    )

def normalizar(texto):
    # Remove emojis, links, pontuacao especial, espacos extras, maiusculas
    texto = remover_emojis(texto)
    texto = re.sub(r'https?://\S+', '', texto)
    texto = re.sub(r'[-*_~`#•|]', '', texto)
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip().lower()

def extrair_cupom(texto):
    # Formato monoespaco `CUPOM`
    mono = re.findall(r'`([A-Za-z0-9]{4,20})`', texto)
    if mono:
        return mono[0].upper()
    # Depois de ":" no final da linha
    palavras_chave = ['off', 'limite', 'acima', 'r$', 'cashback', 'desconto', 'cupom']
    for linha in texto.split('\n'):
        if any(p in linha.lower() for p in palavras_chave):
            partes = linha.split(':')
            if len(partes) > 1:
                cupom = re.sub(r'[^A-Za-z0-9]', '', partes[-1].strip())
                if 4 <= len(cupom) <= 20:
                    return cupom.upper()
    return None

def gerar_hash(texto, cupom=None):
    texto_norm = normalizar(texto)
    cupom_str = cupom.upper() if cupom else ""
    return hashlib.md5(f"{texto_norm}|{cupom_str}".encode()).hexdigest()

def ja_enviado(hash_val):
    if hash_val in enviados:
        if time.time() - enviados[hash_val] < TEMPO_DEDUP:
            return True
    return False

def limpar_memoria():
    agora = time.time()
    for k in list(enviados.keys()):
        if agora - enviados[k] > TEMPO_DEDUP * 2:
            del enviados[k]

def eh_cupom(texto):
    mono = re.findall(r'`([A-Za-z0-9]{4,20})`', texto)
    if mono:
        return True
    if any(p in texto.lower() for p in ['cupom', 'resgate']):
        for linha in texto.split('\n'):
            partes = linha.split(':')
            if len(partes) > 1:
                possivel = re.sub(r'[^A-Za-z0-9]', '', partes[-1].strip())
                if 4 <= len(possivel) <= 20 and possivel.upper() == possivel:
                    return True
    return False

def adicionar_crases(texto, codigo):
    if not codigo:
        return texto
    if f'`{codigo}`' in texto:
        return texto
    # Tenta substituir o código exato
    padrao = re.compile(r'(?<![`\w])' + re.escape(codigo) + r'(?![`\w])')
    return padrao.sub(f'`{codigo}`', texto, count=1)

def adicionar_emojis(texto, plataforma, is_cupom):
    linhas = texto.strip().split('\n')
    novas = []
    primeira_linha_processada = False

    for linha in linhas:
        s = linha.strip()
        if not s:
            novas.append('')
            continue

        # Primeira linha não vazia - nome do produto ou titulo
        if not primeira_linha_processada and not s.startswith('http'):
            if not tem_emojis(s):
                emoji = "🚨" if (plataforma == "shopee" or is_cupom) else "✅"
                s = f"{emoji} {s}"
            primeira_linha_processada = True
            novas.append(s)
            continue

        # Linha de preco
        if re.search(r'r\$\s*\d+', s.lower()) and not tem_emojis(s):
            novas.append(f"🔥 {s}")
            continue

        # Linha de cupom/codigo
        if any(p in s.lower() for p in ['cupom', 'código', 'codigo']) and not tem_emojis(s):
            novas.append(f"🏷️ {s}")
            continue

        # Linha de desconto com OFF
        if 'off' in s.lower() and is_cupom and not tem_emojis(s):
            novas.append(f"🎟 {s}")
            continue

        # Linha de resgate
        if 'resgate' in s.lower() and not tem_emojis(s):
            novas.append(f"✅ {s}")
            continue

        # Linha de carrinho
        if 'carrinho' in s.lower() and not tem_emojis(s):
            novas.append(f"🛒 {s}")
            continue

        novas.append(linha)

    return '\n'.join(novas)

# ============================================================
# AMAZON
# ============================================================
def trocar_tag_amazon(url, tentativa=1):
    try:
        url = url.strip()
        if "amzn.to" in url.lower():
            r = requests.get(url, allow_redirects=True, timeout=10,
                           headers={"User-Agent": "Mozilla/5.0"})
            url = r.url
            log(f"AMAZON | Desencurtado: {url}")
        url = url.split("#")[0]
        base = url.split("?", 1)[0]
        node = re.search(r'node=([^&]+)', url)
        url_final = f"{base}?node={node.group(1)}&tag={AMAZON_TAG}" if node else f"{base}?tag={AMAZON_TAG}"
        log(f"AMAZON | Final: {url_final}")
        return url_final
    except Exception as e:
        log(f"AMAZON | Erro tentativa {tentativa}: {e}")
        if tentativa < MAX_RETRIES:
            time.sleep(2)
            return trocar_tag_amazon(url, tentativa + 1)
        return None

# ============================================================
# SHOPEE
# ============================================================
def ajustar_link_shopee(url, tentativa=1):
    try:
        url_real = url.strip()
        query = f'mutation {{generateShortLink(input: {{originUrl: "{url_real}"}}) {{shortLink}}}}'
        payload = json.dumps({"query": query}, separators=(",", ":"))
        ts = str(int(time.time()))
        sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
        headers = {
            "Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}",
            "Content-Type": "application/json"
        }
        r = requests.post(
            "https://open-api.affiliate.shopee.com.br/graphql",
            headers=headers, data=payload, timeout=10
        )
        dados = r.json()
        sl = dados.get("data", {}).get("generateShortLink", {}).get("shortLink")
        if sl:
            log(f"SHOPEE | Convertido: {sl}")
            return sl
        raise Exception("Sem shortLink na resposta")
    except Exception as e:
        log(f"SHOPEE | Erro tentativa {tentativa}: {e}")
        if tentativa < MAX_RETRIES:
            time.sleep(2)
            return ajustar_link_shopee(url, tentativa + 1)
        return None

def atualizar_links(texto):
    for link in extrair_links(texto):
        novo = None
        if "amazon" in link.lower() or "amzn" in link.lower():
            novo = trocar_tag_amazon(link)
            if novo is None:
                log("AMAZON | Falhou. Descartando oferta.")
                return None
        elif "shopee" in link.lower():
            novo = ajustar_link_shopee(link)
            if novo is None:
                log("SHOPEE | Falhou. Descartando oferta.")
                return None
        if novo and novo != link:
            texto = texto.replace(link, novo, 1)
    return texto

# ============================================================
# BUSCAR IMAGEM
# ============================================================
def buscar_imagem(texto, tentativa=1):
    for link in extrair_links(texto):
        try:
            r = requests.get(link, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(r.text, 'html.parser')
            og = soup.find("meta", property="og:image")
            if og and og.get("content"):
                log("IMAGEM | og:image encontrada")
                return og["content"]
            tw = soup.find("meta", attrs={"name": "twitter:image"})
            if tw and tw.get("content"):
                log("IMAGEM | twitter:image encontrada")
                return tw["content"]
            for img in soup.find_all("img", src=True):
                src = img["src"]
                if src.startswith("http") and any(
                    ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]
                ):
                    return src
        except Exception as e:
            log(f"Erro buscar imagem tentativa {tentativa}: {e}")
            if tentativa < MAX_RETRIES:
                time.sleep(1)
                return buscar_imagem(texto, tentativa + 1)
    return None

# ============================================================
# PROCESSAR
# ============================================================
def processar(texto, plataforma, is_cupom):
    texto_final = atualizar_links(texto)
    if texto_final is None:
        return None, None

    cupom = extrair_cupom(texto_final)

    if cupom:
        texto_final = adicionar_crases(texto_final, cupom)

    if not tem_emojis(texto_final):
        texto_final = adicionar_emojis(texto_final, plataforma, is_cupom)

    return texto_final, cupom

# ============================================================
# ENVIAR MENSAGEM
# ============================================================
async def enviar(texto_final, plataforma, is_cupom, foto_original=None):
    if is_cupom:
        if foto_original:
            log("IMAGEM | Foto original do cupom.")
            return await client.send_file(grupo_destino, foto_original, caption=texto_final, link_preview=False)
        else:
            imagem = IMAGEM_SHOPEE if plataforma == "shopee" else IMAGEM_AMAZON
            log(f"IMAGEM | Usando imagem {plataforma.upper()}")
            return await client.send_file(grupo_destino, imagem, caption=texto_final, link_preview=False)
    else:
        if foto_original:
            log("IMAGEM | Foto original da oferta.")
            return await client.send_file(grupo_destino, foto_original, caption=texto_final, link_preview=False)
        else:
            imagem = buscar_imagem(texto_final)
            if imagem:
                log("IMAGEM | Imagem do produto encontrada.")
                return await client.send_file(grupo_destino, imagem, caption=texto_final, link_preview=False)
            else:
                log("IMAGEM | Sem imagem, enviando com preview.")
                return await client.send_message(grupo_destino, texto_final)

# ============================================================
# EVENTO - NOVA MENSAGEM
# ============================================================
@client.on(events.NewMessage(chats=grupos_origem))
async def handler(event):
    texto = event.message.text or ""
    if not texto.strip():
        return

    if deve_bloquear(texto):
        return

    if not tem_link_valido(texto):
        return

    plataforma = detectar_plataforma(texto)
    is_cupom = eh_cupom(texto)
    cupom_raw = extrair_cupom(texto)

    hash_val = gerar_hash(texto, cupom_raw)
    if ja_enviado(hash_val):
        log("Duplicado, ignorando.")
        return

    texto_final, cupom = processar(texto, plataforma, is_cupom)
    if texto_final is None:
        return

    try:
        foto = event.message.photo if event.message.photo else None
        msg = await enviar(texto_final, plataforma, is_cupom, foto)
        mensagens_map[event.message.id] = msg.id
        enviados[hash_val] = time.time()
        limpar_memoria()
        log(f"Enviado! Total: {len(enviados)}")
    except Exception as e:
        log(f"Erro ao enviar: {e}")

# ============================================================
# EVENTO - MENSAGEM EDITADA
# ============================================================
@client.on(events.MessageEdited(chats=grupos_origem))
async def handler_edited(event):
    texto = event.message.text or ""
    if not texto.strip():
        return

    if event.message.id not in mensagens_map:
        return

    if deve_bloquear(texto):
        return

    plataforma = detectar_plataforma(texto)
    is_cupom = eh_cupom(texto)

    texto_final, cupom = processar(texto, plataforma, is_cupom)
    if texto_final is None:
        return

    try:
        msg_id = mensagens_map[event.message.id]
        await client.edit_message(grupo_destino, msg_id, texto_final)
        log(f"Editado! ID: {msg_id}")
    except Exception as e:
        log(f"Erro ao editar: {e}")

# ============================================================
# INICIA O BOT
# ============================================================
log("Bot iniciado! Monitorando ofertas...")
client.start()
client.run_until_disconnected()

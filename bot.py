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

# IMAGEM SHOPEE e AMAZON para cupons sem imagem
IMAGEM_SHOPEE = "https://files.catbox.moe/myf04b.jpg"
IMAGEM_AMAZON = "https://files.catbox.moe/u1ebbh.jpg"

ofertas_enviadas   = {}  # hash -> timestamp
mensagens_enviadas = {}  # msg_id_original -> msg_id_destino

TEMPO_DEDUP = 7200   # 2 horas
MAX_RETRIES = 3

# ============================================================
# LOG COM DATA/HORA
# ============================================================
def log(msg):
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f"[{agora}] {msg}")

# ============================================================
# FILTROS DE BLOQUEIO
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
    links = extrair_links(texto)
    for link in links:
        if "amazon" in link.lower() or "amzn" in link.lower():
            return True
        if "shopee" in link.lower():
            return True
    log("Bloqueado: nenhum link Amazon ou Shopee.")
    return False

def detectar_plataforma(texto):
    links = extrair_links(texto)
    for link in links:
        if "amazon" in link.lower() or "amzn" in link.lower():
            return "amazon"
        if "shopee" in link.lower():
            return "shopee"
    return None

def tem_emojis(texto):
    padrao = re.compile(
        r'[\U00010000-\U0010ffff'
        r'\U0001F600-\U0001F64F'
        r'\U0001F300-\U0001F5FF'
        r'\U0001F680-\U0001F6FF'
        r'\U0001F1E0-\U0001F1FF'
        r'\u2600-\u26FF'
        r'\u2700-\u27BF]+'
    )
    return bool(padrao.search(texto))

def remover_emojis(texto):
    return re.sub(
        r'[\U00010000-\U0010ffff'
        r'\U0001F600-\U0001F64F'
        r'\U0001F300-\U0001F5FF'
        r'\U0001F680-\U0001F6FF'
        r'\U0001F1E0-\U0001F1FF'
        r'\u2600-\u26FF'
        r'\u2700-\u27BF'
        r'\u200d\uFE0F\u20E3\u3030]+',
        '', texto
    )

def normalizar_para_hash(texto):
    texto = remover_emojis(texto)
    texto = re.sub(r'https?://\S+', '', texto)
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip().lower()

def extrair_cupom(texto):
    # Formato monoespaco `CUPOM`
    mono = re.findall(r'`([A-Za-z0-9]{4,20})`', texto)
    if mono:
        return mono[0]
    # Linha com palavras chave - pega depois do ultimo ":"
    palavras_chave = ['off', 'limite', 'acima', 'r$', 'cashback', 'desconto', 'cupom']
    for linha in texto.split('\n'):
        if any(p in linha.lower() for p in palavras_chave):
            partes = linha.split(':')
            if len(partes) > 1:
                cupom = re.sub(r'[^A-Za-z0-9]', '', partes[-1].strip())
                if 4 <= len(cupom) <= 20:
                    return cupom
    return None

def gerar_hash(texto, cupom=None):
    normalizado = normalizar_para_hash(texto)
    cupom_str = cupom.upper() if cupom else ""
    return hashlib.md5(f"{normalizado}_{cupom_str}".encode()).hexdigest()

def ja_enviado(hash_val):
    if hash_val in ofertas_enviadas:
        if time.time() - ofertas_enviadas[hash_val] < TEMPO_DEDUP:
            return True
    return False

def limpar_memoria():
    agora = time.time()
    for k in list(ofertas_enviadas.keys()):
        if agora - ofertas_enviadas[k] > TEMPO_DEDUP * 2:
            del ofertas_enviadas[k]

def eh_cupom(texto):
    mono = re.findall(r'`([A-Za-z0-9]{4,20})`', texto)
    if mono:
        return True
    palavras_cupom = ['cupom', 'resgate']
    if any(p in texto.lower() for p in palavras_cupom):
        for linha in texto.split('\n'):
            partes = linha.split(':')
            if len(partes) > 1:
                possivel = re.sub(r'[^A-Za-z0-9]', '', partes[-1].strip())
                if 4 <= len(possivel) <= 20 and possivel.upper() == possivel:
                    return True
    return False

def adicionar_crases_cupom(texto, codigo):
    if not codigo:
        return texto
    if f'`{codigo}`' in texto:
        return texto
    return texto.replace(codigo, f'`{codigo}`', 1)

def adicionar_emojis(texto, plataforma, is_cupom):
    linhas = texto.strip().split('\n')
    novas_linhas = []

    for linha in linhas:
        linha_strip = linha.strip()
        if not linha_strip:
            novas_linhas.append(linha)
            continue

        # Linha de preço
        if re.search(r'r\$\s*\d+', linha_strip.lower()) and '🔥' not in linha_strip:
            novas_linhas.append(f"🔥 {linha_strip}")

        # Linha de resgate/resgate aqui
        elif 'resgate' in linha_strip.lower() and '✅' not in linha_strip:
            novas_linhas.append(f"✅ {linha_strip}")

        # Linha de carrinho
        elif 'carrinho' in linha_strip.lower() and '🛒' not in linha_strip:
            novas_linhas.append(f"🛒 {linha_strip}")

        # Linha de desconto com OFF
        elif 'off' in linha_strip.lower() and '🎟' not in linha_strip and is_cupom:
            novas_linhas.append(f"🎟 {linha_strip}")

        # Primeira linha sem emoji (nome do produto ou título)
        elif (
            novas_linhas == [] and
            not linha_strip.startswith('http') and
            not tem_emojis(linha_strip)
        ):
            emoji = "🚨" if plataforma == "shopee" or is_cupom else "✅"
            novas_linhas.append(f"{emoji} {linha_strip}")

        else:
            novas_linhas.append(linha)

    return '\n'.join(novas_linhas)

# ============================================================
# AMAZON
# ============================================================
def trocar_tag_amazon(url, tentativa=1):
    try:
        url = url.strip()
        if "amzn.to" in url.lower():
            resposta = requests.get(
                url, allow_redirects=True, timeout=10,
                headers={"User-Agent": "Mozilla/5.0"}
            )
            url = resposta.url
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
        log(f"SHOPEE | Original: {url_real}")
        query_string = (
            "mutation {generateShortLink(input: {originUrl: \""
            + url_real +
            "\"}) {shortLink}}"
        )
        payload = json.dumps({"query": query_string}, separators=(",", ":"))
        timestamp = str(int(time.time()))
        assinatura_base = f"{SHOPEE_APP_ID}{timestamp}{payload}{SHOPEE_SECRET}"
        signature = hashlib.sha256(assinatura_base.encode()).hexdigest()
        headers = {
            "Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={timestamp},Signature={signature}",
            "Content-Type": "application/json"
        }
        resposta = requests.post(
            "https://open-api.affiliate.shopee.com.br/graphql",
            headers=headers, data=payload, timeout=10
        )
        dados = resposta.json()
        if dados.get("data", {}).get("generateShortLink", {}).get("shortLink"):
            short_link = dados["data"]["generateShortLink"]["shortLink"]
            log(f"SHOPEE | Convertido: {short_link}")
            return short_link
        raise Exception("Resposta inválida da API Shopee")
    except Exception as e:
        log(f"SHOPEE | Erro tentativa {tentativa}: {e}")
        if tentativa < MAX_RETRIES:
            time.sleep(2)
            return ajustar_link_shopee(url, tentativa + 1)
        return None

def atualizar_links(texto):
    links = extrair_links(texto)
    for link in links:
        novo_link = None
        if "amazon" in link.lower() or "amzn" in link.lower():
            novo_link = trocar_tag_amazon(link)
            if novo_link is None:
                log("AMAZON | Falhou todas as tentativas. Descartando.")
                return None
        elif "shopee" in link.lower():
            novo_link = ajustar_link_shopee(link)
            if novo_link is None:
                log("SHOPEE | Falhou todas as tentativas. Descartando.")
                return None
        if novo_link and novo_link != link:
            texto = texto.replace(link, novo_link, 1)
    return texto

# ============================================================
# BUSCAR IMAGEM
# ============================================================
def buscar_melhor_imagem(texto):
    links = extrair_links(texto)
    for link in links:
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
            log(f"Erro ao buscar imagem: {e}")
    return None

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
    cupom = extrair_cupom(texto)

    # Verifica duplicata pelo texto + cupom
    hash_val = gerar_hash(texto, cupom)
    if ja_enviado(hash_val):
        log("Duplicado, ignorando.")
        return

    # Atualiza links
    texto_final = atualizar_links(texto)
    if texto_final is None:
        return

    # Adiciona crases no cupom se não tiver
    if cupom:
        texto_final = adicionar_crases_cupom(texto_final, cupom)

    # Adiciona emojis se não tiver
    if not tem_emojis(texto_final):
        texto_final = adicionar_emojis(texto_final, plataforma, is_cupom)

    # Envia mensagem
    try:
        if is_cupom:
            if event.message.photo:
                log("IMAGEM | Usando foto original do cupom.")
                msg_enviada = await client.send_file(
                    grupo_destino,
                    event.message.photo,
                    caption=texto_final
                )
            else:
                # Usa imagem correta baseada na plataforma
                imagem = IMAGEM_SHOPEE if plataforma == "shopee" else IMAGEM_AMAZON
                log(f"IMAGEM | Usando imagem {plataforma}")
                msg_enviada = await client.send_file(
                    grupo_destino,
                    imagem,
                    caption=texto_final
                )
        else:
            if event.message.photo:
                log("IMAGEM | Usando foto original da oferta.")
                msg_enviada = await client.send_file(
                    grupo_destino,
                    event.message.photo,
                    caption=texto_final
                )
            else:
                imagem = buscar_melhor_imagem(texto_final)
                if imagem:
                    msg_enviada = await client.send_file(
                        grupo_destino,
                        imagem,
                        caption=texto_final
                    )
                else:
                    msg_enviada = await client.send_message(
                        grupo_destino,
                        texto_final
                    )

        mensagens_enviadas[event.message.id] = msg_enviada.id
        ofertas_enviadas[hash_val] = time.time()
        limpar_memoria()

        # ============================================================
        # WHATSAPP - DESATIVADO POR ENQUANTO
        # ============================================================
        # try:
        #     conteudo = texto_final
        #     if cupom:
        #         conteudo += "\n\n" + cupom
        #     with open(ARQUIVO_WHATSAPP, 'w', encoding='utf-8') as f:
        #         f.write(conteudo)
        #     log("WHATSAPP | Arquivo salvo!")
        # except Exception as e:
        #     log(f"Erro ao salvar WhatsApp: {e}")

        log(f"Enviado! Total: {len(ofertas_enviadas)}")

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

    msg_id_original = event.message.id
    if msg_id_original not in mensagens_enviadas:
        return

    if deve_bloquear(texto):
        return

    plataforma = detectar_plataforma(texto)
    is_cupom = eh_cupom(texto)
    cupom = extrair_cupom(texto)

    texto_final = atualizar_links(texto)
    if texto_final is None:
        return

    if cupom:
        texto_final = adicionar_crases_cupom(texto_final, cupom)

    if not tem_emojis(texto_final):
        texto_final = adicionar_emojis(texto_final, plataforma, is_cupom)

    try:
        msg_id_destino = mensagens_enviadas[msg_id_original]
        await client.edit_message(
            grupo_destino,
            msg_id_destino,
            texto_final
        )
        log(f"Mensagem editada! ID: {msg_id_destino}")
    except Exception as e:
        log(f"Erro ao editar: {e}")

# ============================================================
# INICIA O BOT
# ============================================================
log("Bot iniciado! Monitorando ofertas...")
client.start()
client.run_until_disconnected()

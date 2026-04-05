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

ofertas_enviadas    = {}   # hash -> timestamp
cupons_enviados     = {}   # hash_texto -> timestamp
mensagens_enviadas  = {}   # msg_id_original -> msg_id_destino

TEMPO_DEDUP_OFERTA = 86400  # 24 horas
TEMPO_DEDUP_CUPOM  = 7200   # 2 horas
MAX_RETRIES        = 3

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
            log(f"Bloqueado pelo filtro de palavras: {p}")
            return True
    for p in filtro_exclusivo:
        if p in texto_lower:
            log(f"Bloqueado por ser exclusivo/video: {p}")
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
    log("Bloqueado: nenhum link Amazon ou Shopee encontrado.")
    return False

def detectar_plataforma(texto):
    links = extrair_links(texto)
    for link in links:
        if "amazon" in link.lower() or "amzn" in link.lower():
            return "amazon"
        if "shopee" in link.lower():
            return "shopee"
    return None

def remover_emojis(texto):
    return re.sub(
        r'[\U00010000-\U0010ffff'
        r'\U0001F600-\U0001F64F'
        r'\U0001F300-\U0001F5FF'
        r'\U0001F680-\U0001F6FF'
        r'\U0001F1E0-\U0001F1FF'
        r'\u2600-\u26FF'
        r'\u2700-\u27BF'
        r'\u200d'
        r'\uFE0F'
        r'\u20E3'
        r'\u3030'
        r']+',
        '', texto
    )

def normalizar_texto(texto):
    texto = remover_emojis(texto)
    texto = re.sub(r'https?://\S+', '', texto)
    texto = re.sub(r'\s+', ' ', texto)
    texto = texto.strip().lower()
    return texto

def gerar_hash_oferta(texto):
    normalizado = normalizar_texto(texto)
    preco = re.findall(r'r\$[\s]?\d+[,\.]?\d*', normalizado)
    preco = preco[0] if preco else ""
    primeira_linha = normalizado.split('\n')[0].strip()
    texto_limpo = re.sub(r'[^\w\s]', '', primeira_linha)
    return hashlib.md5(f"{texto_limpo}_{preco}".encode()).hexdigest()

def gerar_hash_cupom(texto):
    normalizado = normalizar_texto(texto)
    return hashlib.md5(normalizado.encode()).hexdigest()

def oferta_duplicada(hash_val):
    if hash_val in ofertas_enviadas:
        if time.time() - ofertas_enviadas[hash_val] < TEMPO_DEDUP_OFERTA:
            return True
    return False

def cupom_duplicado(hash_val):
    if hash_val in cupons_enviados:
        if time.time() - cupons_enviados[hash_val] < TEMPO_DEDUP_CUPOM:
            return True
    return False

def limpar_memoria():
    agora = time.time()
    for k in list(ofertas_enviadas.keys()):
        if agora - ofertas_enviadas[k] > TEMPO_DEDUP_OFERTA:
            del ofertas_enviadas[k]
    for k in list(cupons_enviados.keys()):
        if agora - cupons_enviados[k] > TEMPO_DEDUP_CUPOM:
            del cupons_enviados[k]

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

def extrair_cupom(texto):
    mono = re.findall(r'`([A-Za-z0-9]{4,20})`', texto)
    if mono:
        log(f"CUPOM | Monoespaco: {mono[0]}")
        return mono[0]
    palavras_chave = ['off', 'limite', 'acima', 'r$', 'cashback', 'desconto', 'cupom']
    for linha in texto.split('\n'):
        linha_lower = linha.lower()
        if any(p in linha_lower for p in palavras_chave):
            partes = linha.split(':')
            if len(partes) > 1:
                cupom = re.sub(r'[^A-Za-z0-9]', '', partes[-1].strip())
                if 4 <= len(cupom) <= 20:
                    log(f"CUPOM | Encontrado: {cupom}")
                    return cupom
    log("CUPOM | Nenhum cupom encontrado.")
    return None

# ============================================================
# AMAZON
# ============================================================
def trocar_tag_amazon(url, tentativa=1):
    try:
        url = url.strip()
        if "amzn.to" in url.lower():
            resposta = requests.get(
                url,
                allow_redirects=True,
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"}
            )
            url = resposta.url
            log(f"AMAZON | Desencurtado: {url}")
        url = url.split("#")[0]
        base = url.split("?", 1)[0]
        node = re.search(r'node=([^&]+)', url)
        if node:
            url_final = f"{base}?node={node.group(1)}&tag={AMAZON_TAG}"
        else:
            url_final = f"{base}?tag={AMAZON_TAG}"
        log(f"AMAZON | Final: {url_final}")
        return url_final
    except Exception as e:
        log(f"AMAZON | Erro tentativa {tentativa}: {e}")
        if tentativa < MAX_RETRIES:
            time.sleep(2)
            return trocar_tag_amazon(url, tentativa + 1)
        log("AMAZON | Falhou todas as tentativas. Descartando oferta.")
        return None

# ============================================================
# SHOPEE
# ============================================================
def ajustar_link_shopee(url, tentativa=1):
    try:
        url_real = url.strip()
        log(f"SHOPEE | Original: {url_real}")
        query_string = (
            "mutation {"
            "generateShortLink("
            "input: {"
            'originUrl: "' + url_real + '"'
            "}"
            ") {"
            "shortLink"
            "}"
            "}"
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
            headers=headers,
            data=payload,
            timeout=10
        )
        dados = resposta.json()
        if (
            "data" in dados and
            dados["data"] and
            dados["data"].get("generateShortLink")
        ):
            short_link = dados["data"]["generateShortLink"]["shortLink"]
            log(f"SHOPEE | Convertido: {short_link}")
            return short_link
        raise Exception("Resposta inválida da API Shopee")
    except Exception as e:
        log(f"SHOPEE | Erro tentativa {tentativa}: {e}")
        if tentativa < MAX_RETRIES:
            time.sleep(2)
            return ajustar_link_shopee(url, tentativa + 1)
        log("SHOPEE | Falhou todas as tentativas. Descartando oferta.")
        return None

def atualizar_texto(texto):
    links = extrair_links(texto)
    for link in links:
        novo_link = None
        if "amazon" in link.lower() or "amzn" in link.lower():
            novo_link = trocar_tag_amazon(link)
            if novo_link is None:
                return None
        elif "shopee" in link.lower():
            novo_link = ajustar_link_shopee(link)
            if novo_link is None:
                return None
        if novo_link and novo_link != link:
            texto = texto.replace(link, novo_link, 1)
    return texto

# ============================================================
# FORMATAR MENSAGEM
# ============================================================
def formatar_oferta(texto, plataforma, cupom=None):
    texto_limpo = remover_emojis(texto).strip()
    linhas = [l.strip() for l in texto_limpo.split('\n') if l.strip()]
    links = extrair_links(texto)

    nome = ""
    for linha in linhas:
        if not linha.startswith("http") and len(linha) > 3:
            nome = linha
            break

    preco = ""
    for linha in linhas:
        if "r$" in linha.lower():
            preco = linha.strip()
            break

    link_principal = links[0] if links else ""
    emoji_inicio = "🚨" if plataforma == "shopee" else "✅"

    msg = f"{emoji_inicio} *{nome}*\n\n"
    if preco:
        msg += f"🔥 *{preco}*\n"
    if cupom:
        msg += f"🏷️ *Cupom:* `{cupom}`\n"
    if link_principal:
        msg += f"\n*Link*: {link_principal}"

    return msg.strip()

def formatar_cupom(texto, plataforma, cupom=None):
    texto_limpo = remover_emojis(texto).strip()
    links = extrair_links(texto)

    if plataforma == "shopee":
        desconto = ""
        for linha in texto_limpo.split('\n'):
            if any(p in linha.lower() for p in ['off', 'r$', 'limite', 'acima']):
                desconto = linha.strip()
                break

        link_resgate = ""
        link_carrinho = ""
        for link in links:
            if "shopee" in link.lower():
                if not link_resgate:
                    link_resgate = link
                elif not link_carrinho:
                    link_carrinho = link

        msg = "🚨 *CUPOM SHOPEE* 🚨\n\n"
        if desconto:
            msg += f"🎟 {desconto}"
            if cupom:
                msg += f": `{cupom}`"
            msg += "\n\n"
        if link_resgate:
            msg += f"✅ Resgate aqui:\n{link_resgate}\n\n"
        if link_carrinho:
            msg += f"🛒 Carrinho:\n{link_carrinho}"
        return msg.strip()

    else:
        desconto = ""
        for linha in texto_limpo.split('\n'):
            if any(p in linha.lower() for p in ['off', 'r$', '%', 'limite', 'acima']):
                desconto = linha.strip()
                break

        link_principal = links[0] if links else ""

        msg = "🚨 *Cupom Amazon APP*\n\n"
        if desconto:
            msg += f"🎟 *{desconto}"
            if cupom:
                msg += f": `{cupom}`"
            msg += "*\n\n"
        if link_principal:
            msg += f"✅ *Resgate por esse link*:\n{link_principal}"
        return msg.strip()

def buscar_melhor_imagem(texto):
    links = extrair_links(texto)
    for link in links:
        try:
            r = requests.get(link, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(r.text, 'html.parser')
            og = soup.find("meta", property="og:image")
            if og and og.get("content"):
                log(f"IMAGEM | og:image: {og['content']}")
                return og["content"]
            tw = soup.find("meta", attrs={"name": "twitter:image"})
            if tw and tw.get("content"):
                log(f"IMAGEM | twitter:image: {tw['content']}")
                return tw["content"]
            for img in soup.find_all("img", src=True):
                src = img["src"]
                if (
                    src.startswith("http") and
                    any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"])
                ):
                    log(f"IMAGEM | img tag: {src}")
                    return src
        except Exception as e:
            log(f"Erro ao buscar imagem: {e}")
            continue
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

    if is_cupom:
        hash_val = gerar_hash_cupom(texto)
        if cupom_duplicado(hash_val):
            log("Cupom duplicado, ignorando.")
            return
    else:
        hash_val = gerar_hash_oferta(texto)
        if oferta_duplicada(hash_val):
            log("Oferta duplicada, ignorando.")
            return

    texto_atualizado = atualizar_texto(texto)
    if texto_atualizado is None:
        log("Conversão de links falhou. Descartando oferta.")
        return

    cupom = extrair_cupom(texto_atualizado)

    if is_cupom:
        mensagem_formatada = formatar_cupom(texto_atualizado, plataforma, cupom)
        if event.message.photo:
            log("IMAGEM | Usando foto original do cupom.")
            msg_enviada = await client.send_file(
                grupo_destino,
                event.message.photo,
                caption=mensagem_formatada,
                parse_mode='md'
            )
        else:
            imagem_cupom = IMAGEM_SHOPEE if plataforma == "shopee" else IMAGEM_AMAZON
            msg_enviada = await client.send_file(
                grupo_destino,
                imagem_cupom,
                caption=mensagem_formatada,
                parse_mode='md'
            )
        cupons_enviados[hash_val] = time.time()

    else:
        mensagem_formatada = formatar_oferta(texto_atualizado, plataforma, cupom)
        if event.message.photo:
            log("IMAGEM | Usando foto original da oferta.")
            msg_enviada = await client.send_file(
                grupo_destino,
                event.message.photo,
                caption=mensagem_formatada,
                parse_mode='md'
            )
        else:
            imagem = buscar_melhor_imagem(texto_atualizado)
            if imagem:
                msg_enviada = await client.send_file(
                    grupo_destino,
                    imagem,
                    caption=mensagem_formatada,
                    parse_mode='md'
                )
            else:
                msg_enviada = await client.send_message(
                    grupo_destino,
                    mensagem_formatada,
                    parse_mode='md'
                )
        ofertas_enviadas[hash_val] = time.time()

    mensagens_enviadas[event.message.id] = msg_enviada.id
    limpar_memoria()

    # ============================================================
    # WHATSAPP - DESATIVADO POR ENQUANTO
    # ============================================================
    # try:
    #     conteudo = mensagem_formatada
    #     if cupom:
    #         conteudo += "\n\n" + cupom
    #     with open(ARQUIVO_WHATSAPP, 'w', encoding='utf-8') as f:
    #         f.write(conteudo)
    #     log("WHATSAPP | Arquivo salvo!")
    # except Exception as e:
    #     log(f"Erro ao salvar arquivo WhatsApp: {e}")

    log(f"Oferta enviada! Total acumulado: {len(ofertas_enviadas) + len(cupons_enviados)}")

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

    texto_atualizado = atualizar_texto(texto)
    if texto_atualizado is None:
        log("Edição: conversão de links falhou. Ignorando.")
        return

    cupom = extrair_cupom(texto_atualizado)

    if is_cupom:
        mensagem_formatada = formatar_cupom(texto_atualizado, plataforma, cupom)
    else:
        mensagem_formatada = formatar_oferta(texto_atualizado, plataforma, cupom)

    try:
        msg_id_destino = mensagens_enviadas[msg_id_original]
        await client.edit_message(
            grupo_destino,
            msg_id_destino,
            mensagem_formatada,
            parse_mode='md'
        )
        log(f"Mensagem editada com sucesso! ID: {msg_id_destino}")
    except Exception as e:
        log(f"Erro ao editar mensagem: {e}")

# ============================================================
# INICIA O BOT
# ============================================================
print("Bot iniciado! Monitorando ofertas...")
client.start()
client.run_until_disconnected()

# ============================================================
# BOT DE OFERTAS - TELEGRAM
# Plataformas: Amazon | Shopee
# ============================================================

from telethon import TelegramClient, events
from telethon.sessions import StringSession
import re, requests, json, time, hashlib, os
from bs4 import BeautifulSoup

api_id = 33768893
api_hash = '7959ea0392ff7f91b4f7e207e75a1813'
session = StringSession(os.environ.get('SESSION_STRING', ''))
client = TelegramClient(session, api_id, api_hash)

grupos_origem = [
    'https://t.me/botofera',
]
grupo_destino = '@ofertap'

ARQUIVO_WHATSAPP = '/storage/emulated/0/oferta_whatsapp.txt'

AMAZON_TAG    = "leo21073-20"
SHOPEE_APP_ID = "18348480261"
SHOPEE_SECRET = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"

IMAGEM_SHOPEE = "https://i.imgur.com/Emr2aPR.jpeg"
IMAGEM_AMAZON = "https://i.imgur.com/43N938V.jpeg"

ofertas_enviadas = set()
cupons_enviados  = {}  # cupom -> timestamp

filtro = [
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
# VERIFICA SE TEM LINK AMAZON OU SHOPEE
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
    print("DEBUG | Bloqueado: nenhum link Amazon ou Shopee encontrado.")
    return False

def detectar_plataforma(texto):
    links = extrair_links(texto)
    for link in links:
        if "amazon" in link.lower() or "amzn" in link.lower():
            return "amazon"
        if "shopee" in link.lower():
            return "shopee"
    return None

# ============================================================
# AMAZON
# ============================================================
def trocar_tag_amazon(url):
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
            print("DEBUG AMAZON | Desencurtado:", url)
        url = url.split("#")[0]
        base = url.split("?", 1)[0]
        node = re.search(r'node=([^&]+)', url)
        if node:
            url_final = f"{base}?node={node.group(1)}&tag={AMAZON_TAG}"
        else:
            url_final = f"{base}?tag={AMAZON_TAG}"
        print("DEBUG AMAZON | Final:", url_final)
        return url_final
    except Exception as e:
        print(f"Erro Amazon: {e}")
        return url

# ============================================================
# SHOPEE
# ============================================================
def ajustar_link_shopee(url):
    try:
        url_real = url.strip()
        print("DEBUG SHOPEE | Original:", url_real)
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
        print("DEBUG SHOPEE | Resposta API:", dados)
        if (
            "data" in dados and
            dados["data"] and
            dados["data"].get("generateShortLink")
        ):
            short_link = dados["data"]["generateShortLink"]["shortLink"]
            print("DEBUG SHOPEE | Convertido:", short_link)
            return short_link
        return url_real
    except Exception as e:
        print(f"Erro Shopee: {e}")
        return url

# ============================================================
# UTILITARIOS
# ============================================================
def atualizar_texto(texto):
    links = extrair_links(texto)
    for link in links:
        novo_link = None
        if "amazon" in link.lower() or "amzn" in link.lower():
            novo_link = trocar_tag_amazon(link)
        elif "shopee" in link.lower():
            novo_link = ajustar_link_shopee(link)
        if novo_link and novo_link != link:
            texto = texto.replace(link, novo_link, 1)
    return texto

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

def gerar_hash_oferta(texto):
    texto_sem_links = re.sub(r'https?://\S+', '', texto)
    texto_sem_emojis = remover_emojis(texto_sem_links)
    preco = re.findall(r'R\$[\s]?\d+[,\.]?\d*', texto)
    preco = preco[0] if preco else ""
    primeira_linha = texto_sem_emojis.strip().split('\n')[0].strip().lower()
    texto_limpo = re.sub(r'[^\w\s]', '', primeira_linha)
    return hashlib.md5(
        f"{texto_limpo}_{preco}".encode()
    ).hexdigest()

def extrair_cupom(texto):
    mono = re.findall(r'`([A-Za-z0-9]{4,20})`', texto)
    if mono:
        print("DEBUG CUPOM | Monoespaco:", mono[0])
        return mono[0]
    palavras_chave = ['off', 'limite', 'acima', 'r$', 'cashback', 'desconto']
    for linha in texto.split('\n'):
        linha_lower = linha.lower()
        if any(p in linha_lower for p in palavras_chave):
            partes = linha.split(':')
            if len(partes) > 1:
                cupom = partes[-1].strip()
                cupom = re.sub(r'[^A-Za-z0-9]', '', cupom)
                if 4 <= len(cupom) <= 20:
                    print("DEBUG CUPOM | Encontrado:", cupom)
                    return cupom
    print("DEBUG CUPOM | Nenhum cupom encontrado.")
    return None

def cupom_duplicado(cupom):
    if cupom in cupons_enviados:
        ultimo = cupons_enviados[cupom]
        if time.time() - ultimo < 7200:  # 2 horas
            print(f"DEBUG CUPOM | Duplicado bloqueado: {cupom}")
            return True
    return False

def registrar_cupom(cupom):
    cupons_enviados[cupom] = time.time()

def formatar_mensagem(texto, plataforma, cupom=None):
    texto_sem_links = re.sub(r'https?://\S+', '', texto).strip()
    texto_sem_emojis = remover_emojis(texto_sem_links).strip()
    linhas = [l.strip() for l in texto_sem_emojis.split('\n') if l.strip()]

    links = extrair_links(texto)
    link_principal = links[0] if links else ""

    eh_cupom = any(p in texto.lower() for p in ['cupom', 'código', 'resgate'])

    if eh_cupom and plataforma == "shopee":
        desconto = ""
        for linha in linhas:
            if any(p in linha.lower() for p in ['off', 'limite', 'r$']):
                desconto = linha
                break
        link_resgate = ""
        link_carrinho = ""
        for link in links:
            if "shopee" in link.lower():
                if not link_resgate:
                    link_resgate = link
                elif not link_carrinho:
                    link_carrinho = link

        msg = f"🚨 *CUPOM SHOPEE* 🚨\n\n"
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

    elif eh_cupom and plataforma == "amazon":
        desconto = ""
        for linha in linhas:
            if any(p in linha.lower() for p in ['off', 'limite', 'r$', '%']):
                desconto = linha
                break
        msg = f"🚨 *Cupom Amazon APP*\n\n"
        if desconto:
            msg += f"🎟 *{desconto}"
            if cupom:
                msg += f": `{cupom}`"
            msg += "*\n\n"
        if link_principal:
            msg += f"✅ *Resgate por esse link*:\n{link_principal}"
        return msg.strip()

    else:
        nome = linhas[0] if linhas else "Produto"
        preco = ""
        for linha in linhas:
            if "r$" in linha.lower():
                preco = linha
                break

        msg = f"🚨 *{nome}*\n\n"
        if preco:
            msg += f"🔥 *{preco}*\n"
        if cupom:
            msg += f"🏷️ *Cupom:* `{cupom}`\n"
        msg += f"\n*Link*: {link_principal}"
        return msg.strip()

def buscar_melhor_imagem(texto):
    links = extrair_links(texto)
    for link in links:
        try:
            r = requests.get(link, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(r.text, 'html.parser')
            og = soup.find("meta", property="og:image")
            if og and og.get("content"):
                print("DEBUG IMAGEM | og:image:", og["content"])
                return og["content"]
            tw = soup.find("meta", attrs={"name": "twitter:image"})
            if tw and tw.get("content"):
                print("DEBUG IMAGEM | twitter:image:", tw["content"])
                return tw["content"]
            for img in soup.find_all("img", src=True):
                src = img["src"]
                if (
                    src.startswith("http") and
                    any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"])
                ):
                    print("DEBUG IMAGEM | img tag:", src)
                    return src
        except Exception as e:
            print(f"Erro ao buscar imagem: {e}")
            continue
    return None

def eh_cupom(texto):
    return any(p in texto.lower() for p in ['cupom', 'código', 'resgate', 'desconto'])

# ============================================================
# EVENTO TELEGRAM
# ============================================================
@client.on(events.NewMessage(chats=grupos_origem))
async def handler(event):
    texto = event.message.text or ""
    if not texto.strip():
        return

    if any(p.lower() in texto.lower() for p in filtro):
        print("DEBUG | Bloqueado pelo filtro.")
        return

    if not tem_link_valido(texto):
        return

    texto = atualizar_texto(texto)

    hash_atual = gerar_hash_oferta(texto)
    if hash_atual in ofertas_enviadas:
        print("DEBUG | Oferta duplicada, ignorando:", hash_atual)
        return
    ofertas_enviadas.add(hash_atual)

    cupom = extrair_cupom(texto)

    if cupom and cupom_duplicado(cupom):
        cupom = None

    plataforma = detectar_plataforma(texto)
    mensagem_formatada = formatar_mensagem(texto, plataforma, cupom)

    is_cupom = eh_cupom(texto)

    if is_cupom:
        imagem_cupom = IMAGEM_SHOPEE if plataforma == "shopee" else IMAGEM_AMAZON
        await client.send_file(
            grupo_destino,
            imagem_cupom,
            caption=mensagem_formatada,
            parse_mode='md'
        )
    elif event.message.photo:
        print("DEBUG IMAGEM | Usando foto da mensagem original.")
        await client.send_file(
            grupo_destino,
            event.message.photo,
            caption=mensagem_formatada,
            parse_mode='md'
        )
    else:
        imagem = buscar_melhor_imagem(texto)
        if imagem:
            await client.send_file(
                grupo_destino,
                imagem,
                caption=mensagem_formatada,
                parse_mode='md'
            )
        else:
            await client.send_message(
                grupo_destino,
                mensagem_formatada,
                parse_mode='md'
            )

    if cupom:
        registrar_cupom(cupom)

    # ============================================================
    # WHATSAPP - DESATIVADO POR ENQUANTO
    # ============================================================
    # try:
    #     conteudo = mensagem_formatada
    #     if cupom:
    #         conteudo += "\n\n" + cupom
    #     with open(ARQUIVO_WHATSAPP, 'w', encoding='utf-8') as f:
    #         f.write(conteudo)
    #     print("DEBUG WHATSAPP | Arquivo salvo!")
    # except Exception as e:
    #     print(f"Erro ao salvar arquivo WhatsApp: {e}")

    print(f"Oferta enviada! Total acumulado: {len(ofertas_enviadas)}")

# ============================================================
# INICIA O BOT
# ============================================================
print("Bot iniciado! Monitorando ofertas...")
client.start()
client.run_until_disconnected()
        

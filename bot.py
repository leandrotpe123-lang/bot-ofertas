# ============================================================
# BOT DE OFERTAS - TELEGRAM
# Plataformas: Amazon | Shopee
# ============================================================

from telethon import TelegramClient, events
import re, requests, json, time, hashlib
from bs4 import BeautifulSoup

api_id = 33768893
api_hash = '7959ea0392ff7f91b4f7e207e75a1813'
client = TelegramClient('session_leo', api_id, api_hash)

grupos_origem = ['https://t.me/botofera']
grupo_destino = '@ofertap'

AMAZON_TAG    = "leo21073-20"
SHOPEE_APP_ID = "18348480261"
SHOPEE_SECRET = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"

ofertas_enviadas = set()

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
            f'originUrl: "{url_real}"'
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
def extrair_links(texto):
    return re.findall(r'https?://[^\s\)\]\>\"]+', texto)

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

def gerar_hash_oferta(texto):
    texto_sem_links = re.sub(r'https?://\S+', '', texto)
    preco = re.findall(r'R\$[\s]?\d+[,\.]?\d*', texto)
    preco = preco[0] if preco else ""
    texto_limpo = re.sub(r'[^\w\s.,:!R$-]', '', texto_sem_links)
    return hashlib.md5(
        f"{texto_limpo.strip().lower()}_{preco}".encode()
    ).hexdigest()

def extrair_cupom(texto):
    # Formato monoespaco `CUPOM`
    mono = re.findall(r'`([A-Za-z0-9]{4,20})`', texto)
    if mono:
        print("DEBUG CUPOM | Monoespaco:", mono[0])
        return mono[0]

    # Linha com emoji ou palavras de desconto - pega depois do ultimo ":"
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

    texto = atualizar_texto(texto)

    hash_atual = gerar_hash_oferta(texto)
    if hash_atual in ofertas_enviadas:
        print("DEBUG | Oferta duplicada, ignorando:", hash_atual)
        return
    ofertas_enviadas.add(hash_atual)

    cupom = extrair_cupom(texto)

    if event.message.photo:
        print("DEBUG IMAGEM | Usando foto da mensagem original.")
        await client.send_file(grupo_destino, event.message.photo, caption=texto)
    else:
        imagem = buscar_melhor_imagem(texto)
        if imagem:
            await client.send_file(grupo_destino, imagem, caption=texto)
        else:
            await client.send_message(grupo_destino, texto)

    if cupom:
        await client.send_message(grupo_destino, cupom)
        print(f"DEBUG CUPOM | Cupom enviado: {cupom}")

    print(f"Oferta enviada! Total acumulado: {len(ofertas_enviadas)}")

# ============================================================
# INICIA O BOT
# ============================================================
print("Bot iniciado! Monitorando ofertas...")
client.start()
client.run_until_disconnected()

# ============================================================
# BOT DE OFERTAS - TELEGRAM (VERSÃO 15.0 - BITLY SÓ MAGALU)
# ============================================================

from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaWebPage
import re, requests, json, time, hashlib
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

# ============================================================
# 🔹 CONFIGURAÇÕES GERAIS
# ============================================================
api_id = 33768893
api_hash = '7959ea0392ff7f91b4f7e207e75a1813'
client = TelegramClient('session_leo', api_id, api_hash)

grupos_origem = ['https://t.me/botofera']
grupo_destino = '@ofertap'

# AFILIADOS
AMAZON_TAG      = "leo21073-20"
MAGALU_PARTNER  = "3440"
MAGALU_PROMOTER = "5479317"
ML_SOURCE       = "silvaleo20230518163534"
SHOPEE_APP_ID   = "18348480261"
SHOPEE_SECRET   = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"

# BITLY TOKEN (Exclusivo para Magalu)
BITLY_TOKEN = "69cdfdea70096c9cf42a5eac20cb55b17668ede9"

# SEU SOCIAL ML
MEU_LINK_SOCIAL_ML = "https://mercadolivre.com/sec/23NpLSc"

historico_ofertas = set()

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
# 🔹 FUNÇÕES DE APOIO
# ============================================================

def encurtar_bitly(url_longa):
    """Encurta o link usando o token do Bitly (USADO APENAS NO MAGALU)."""
    try:
        headers = {
            "Authorization": f"Bearer {BITLY_TOKEN}",
            "Content-Type": "application/json"
        }
        payload = {"long_url": url_longa}
        res = requests.post("https://api-ssl.bitly.com/v4/shorten", json=payload, headers=headers, timeout=10)
        if res.status_code in [200, 201]:
            return res.json().get("link")
        return url_longa
    except:
        return url_longa

def gerar_dna_oferta(texto):
    """DNA baseado em Texto + Valor, ignorando emojis e links."""
    t = re.sub(r'https?://\S+', '', texto)
    precos = re.findall(r'R\$\s?\d+[.,\d]*', t)
    valor = precos[0].replace(" ", "") if precos else "0"
    t_limpo = re.sub(r'[^\w\s]', '', t)
    return f"{' '.join(t_limpo.split()).lower()}_{valor}"

def modificar_params(url, novos_params):
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        for k, v in novos_params.items():
            params[k] = [v]
        nova_query = urlencode(params, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, nova_query, parsed.fragment))
    except: return url

# ============================================================
# 🔹 CONVERSORES POR PLATAFORMA
# ============================================================

def converter_ml(url):
    try:
        url_l = url.lower()
        if "/sec/" in url_l: return MEU_LINK_SOCIAL_ML
        
        url_final = url
        if "meli.la" in url_l:
            try:
                res = requests.get(url, allow_redirects=True, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                url_final = res.url
            except: pass
            
        tags = {"matt_tool": "afiliados", "matt_word": "oferta", "matt_source": ML_SOURCE, "matt_campaign": "ofertap"}
        return modificar_params(url_final, tags)
    except: return url

def converter_amazon(url):
    try:
        if "amzn.to" in url.lower():
            res = requests.get(url, allow_redirects=True, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            url = res.url
        return modificar_params(url, {"tag": AMAZON_TAG})
    except: return url

def converter_magalu(url):
    try:
        # 1. Gera o link longo com suas tags
        long_url = modificar_params(url, {
            "utm_source": "divulgador", "utm_medium": "magalu",
            "partner_id": MAGALU_PARTNER, "promoter_id": MAGALU_PROMOTER,
            "utm_campaign": MAGALU_PROMOTER
        })
        # 2. ENCURTA COM BITLY (Exclusivo para Magalu)
        return encurtar_bitly(long_url)
    except: return url

def converter_shopee(url):
    try:
        query = f'mutation {{ generateShortLink(input: {{ originUrl: "{url}" }}) {{ shortLink }} }}'
        payload = json.dumps({"query": query}, separators=(",", ":"))
        ts = str(int(time.time())); sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
        headers = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
        res = requests.post("https://open-api.affiliate.shopee.com.br/graphql", headers=headers, data=payload, timeout=10)
        return res.json()["data"]["generateShortLink"]["shortLink"]
    except: return url

# ============================================================
# 🔹 MOTOR DE PROCESSAMENTO
# ============================================================

def processar_texto(texto):
    links = re.findall(r'(https?://[^\s,]+)', texto)
    links = sorted(list(set(links)), key=len, reverse=True)

    for link in links:
        link_limpo = link.rstrip('.,)!?')
        l_link = link_limpo.lower()
        novo = link_limpo

        if "amazon" in l_link or "amzn" in l_link:
            novo = converter_amazon(link_limpo)
        elif "magalu" in l_link or "magazineluiza" in l_link:
            novo = converter_magalu(link_limpo)
        elif "mercadolivre" in l_link or "meli.la" in l_link:
            novo = converter_ml(link_limpo)
        elif "shopee" in l_link:
            novo = converter_shopee(link_limpo)

        if novo and novo != link_limpo:
            texto = texto.replace(link_limpo, novo)
    return texto

# ============================================================
# 🔹 HANDLER TELEGRAM
# ============================================================

@client.on(events.NewMessage(chats=grupos_origem))
async def handler(event):
    raw_text = event.message.text or ""
    if not raw_text.strip(): return
    if any(p.lower() in raw_text.lower() for p in filtro): return

    # DNA (Anti-duplicidade Texto + Valor)
    dna = gerar_dna_oferta(raw_text)
    if dna in historico_ofertas:
        print(f"🚫 DNA Repetido: {dna}")
        return

    # Processa texto
    texto_final = processar_texto(raw_text)
    historico_ofertas.add(dna)

    try:
        tem_midia = event.message.media and not isinstance(event.message.media, MessageMediaWebPage)
        
        if tem_midia:
            if len(texto_final) > 1024:
                await client.send_file(grupo_destino, event.message.media)
                await client.send_message(grupo_destino, texto_final)
            else:
                await client.send_file(grupo_destino, event.message.media, caption=texto_final)
        else:
            await client.send_message(grupo_destino, texto_final)
        
        print(f"✅ Enviado! (Bitly apenas no Magalu) DNA: {dna}")

    except Exception as e:
        print(f"❌ Erro envio: {e}")

# ============================================================
# 🔹 START
# ============================================================
print("🚀 Bot Versão 15.0 Online! Bitly ativo EXCLUSIVAMENTE para Magalu.")
client.start()
client.run_until_disconnected()

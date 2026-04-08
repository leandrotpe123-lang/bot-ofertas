import re, asyncio, hashlib, json, time, os, aiohttp, random, unicodedata, requests
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage
from telethon.errors import MessageNotModifiedError
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from difflib import SequenceMatcher
from threading import Lock

# ============================================================
# 🔹 CONFIGURAÇÕES GERAIS
# ============================================================
API_ID = 33768893
API_HASH = '7959ea0392ff7f91b4f7e207e75a1813'
SESSION_STRING = os.environ.get("TELEGRAM_SESSION")

# Grupos monitorados
GRUPOS_ORIGEM = ['fumotom', 'promotom', 'botofera']
GRUPO_DESTINO = '@ofertap'

# TAGS AFILIADOS
AMAZON_TAG      = "leo21073-20"
MAGALU_PARTNER  = "3440"
MAGALU_PROMOTER = "5479317"
ML_SOURCE       = "silvaleo20230518163534"
SHOPEE_APP_ID   = "18348480261"
SHOPEE_SECRET   = "SGC7FQQQ4R5QCFULPXIBCANATLP272B3"
BITLY_TOKEN     = "69cdfdea70096c9cf42a5eac20cb55b17668ede9"

# IMAGENS FIXAS
IMG_ML_FIXA = "mercado_livre_c1a918503a.jpg"
IMG_SHOPEE_FIXA = "shopee_3764834888.jpg"
IMG_AMAZON_FIXA = "amazon_46545785.jpg"

ARQUIVO_CACHE = "cache_dedup_profissional.json"
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

envio_lock = asyncio.Semaphore(2)
LOCK = Lock()

# FILTRO BLINDADO
FILTRO = [
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

PALAVRAS_RUIDO = {"promo", "promocao", "promoção", "oferta", "desconto", "cupom", "corre", "aproveita", "urgente", "gratis", "grátis", "frete", "hoje", "agora", "relampago", "relâmpago"}

# ============================================================
# 🔹 SISTEMA DE DEDUPLICAÇÃO PROFISSIONAL (SUA LÓGICA)
# ============================================================

def remover_acentos(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def normalizar_texto(texto):
    if not texto: return ""
    texto = remover_acentos(texto.lower())
    texto = re.sub(r"http\S+|www\S+", " ", texto)
    texto = re.sub(r"[^\w\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    tokens = sorted([t for t in texto.split() if t not in PALAVRAS_RUIDO])
    return " ".join(tokens)

def deve_enviar_oferta(plataforma, produto_id, preco, cupom="", texto=""):
    with LOCK:
        if not os.path.exists(ARQUIVO_CACHE): cache = {}
        else:
            try: cache = json.load(open(ARQUIVO_CACHE, "r"))
            except: cache = {}
        
        agora = time.time()
        cache = {k: v for k, v in cache.items() if agora - v["timestamp"] < 7200} # 120 min TTL
        
        texto_norm = normalizar_texto(texto)
        hash_exato = hashlib.sha256(f"{plataforma}|{produto_id}|{preco}|{cupom.lower()}|{texto_norm}".encode()).hexdigest()

        if hash_exato in cache: return False

        for _, o in cache.items():
            if str(o["produto_id"]) == str(produto_id) and str(o["preco"]) == str(preco) and o["cupom"] == str(cupom).lower():
                if agora - o["timestamp"] < 900 and SequenceMatcher(None, texto_norm, o["texto"]).ratio() >= 0.90:
                    return False

        cache[hash_exato] = {"produto_id": str(produto_id), "preco": str(preco), "cupom": str(cupom).lower(), "texto": texto_norm, "timestamp": agora}
        json.dump(cache, open(ARQUIVO_CACHE, "w"), indent=2)
        return True

# ============================================================
# 🔹 DESENCURTADOR PROFISSIONAL (ATÉ O OSSO)
# ============================================================

async def elite_expand(url):
    """Expande qualquer link recursivamente até o destino final."""
    print(f"DEBUG | Desencurtando: {url}")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        async with aiohttp.ClientSession(headers=headers) as s:
            async with s.get(url, allow_redirects=True, timeout=12) as r:
                dest = str(r.url)
                print(f"DEBUG | Destino Final: {dest}")
                return dest
    except Exception as e:
        print(f"DEBUG | Erro Expansão: {e}")
        return url

async def encurtar_bitly(url):
    async with aiohttp.ClientSession() as s:
        h = {"Authorization": f"Bearer {BITLY_TOKEN}"}
        try:
            async with s.post("https://api-ssl.bitly.com/v4/shorten", json={"long_url": url}, headers=h) as r:
                d = await r.json()
                return d.get("link", url)
        except: return url

# ============================================================
# 🔹 CONVERSORES POR PLATAFORMA
# ============================================================

async def converter_link(url):
    url_l = url.lower()
    
    # MERCADO LIVRE
    if "mercadolivre" in url_l or "meli.la" in url_l:
        if "/sec/" in url_l:
            url = url.replace("mercadolivre.com/sec/2U6U32Q", "mercadolivre.com/sec/23NpLSc")
            return url, "ml", True
        
        url_ex = await elite_expand(url)
        q = parse_qs(urlparse(url_ex).query)
        q.update({"matt_tool": ["afiliados"], "matt_source": [ML_SOURCE], "matt_campaign": ["ofertap"]})
        return urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True))), "ml", False

    # AMAZON (Lógica Solicitada)
    elif "amazon" in url_l or "amzn" in url_l:
        url_ex = await elite_expand(url)
        base = url_ex.split("?")[0].split("#")[0]
        node = re.search(r'node=([^&]+)', url_ex)
        url_final = f"{base}?node={node.group(1)}&tag={AMAZON_TAG}" if node else f"{base}?tag={AMAZON_TAG}"
        return url_final, "amazon", False

    # MAGALU
    elif "magazineluiza" in url_l or "magalu" in url_l:
        url_ex = await elite_expand(url)
        q = parse_qs(urlparse(url_ex).query)
        q.update({"partner_id": [MAGALU_PARTNER], "promoter_id": [MAGALU_PROMOTER]})
        l_url = urlunparse(urlparse(url_ex)._replace(query=urlencode(q, doseq=True)))
        return await encurtar_bitly(l_url), "magalu", False

    # SHOPEE
    elif "shopee" in url_l:
        ts = str(int(time.time()))
        query = f'mutation {{ generateShortLink(input: {{ originUrl: "{url}" }}) {{ shortLink }} }}'
        payload = json.dumps({"query": query}, separators=(",", ":"))
        sig = hashlib.sha256(f"{SHOPEE_APP_ID}{ts}{payload}{SHOPEE_SECRET}".encode()).hexdigest()
        h = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={ts},Signature={sig}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession() as s:
            try:
                async with s.post("https://open-api.affiliate.shopee.com.br/graphql", data=payload, headers=h) as r:
                    res = await r.json()
                    return res["data"]["generateShortLink"]["shortLink"], "shopee", False
            except: return url, "shopee", False

    # LINKS DE CADASTRO
    if any(x in url_l for x in ["cadastro", "ganhe", "promo", "formulario", "inscricao"]): return url, "info", False
    
    return None, None, False # Descarta qualquer outro link

# ============================================================
# 🔹 FORMATAÇÃO
# ============================================================
async def processar_evento(event, is_edit=False):
    texto_bruto = event.message.text or ""
    if not texto_bruto.strip(): return

    # 1. Filtro de palavras (Blacklist)
    if any(p.lower() in texto_bruto.lower() for p in FILTRO):
        print(f"🔎 DEBUG | Bloqueado pelo filtro de palavras.")
        return

    chat = await event.get_chat()
    username = (chat.username or "").lower()
    links_detectados = re.findall(r'https?://\S+', texto_bruto)
    
    # Bloqueia se não for grupo de cupons e não tiver link
    if not links_detectados and username != "fadadoscupons": return

    # 2. Converte os links (Até 50) e identifica plataforma/ID para o Cache
    links_conv = []
    forcar_img_ml = False
    plataforma_p, prod_id = "outro", "0"
    texto_final = texto_bruto # Começa com o texto original fiel

    for link in links_detectados[:50]:
        # O robô usa o seu Desencurtador Elite (Amazon, ML, Magalu, Shopee)
        novo, plat, force_img = await converter_link(link) 
        if novo:
            links_conv.append(novo)
            # Troca o link antigo pelo novo diretamente no texto original
            texto_final = texto_final.replace(link, novo)
            if plat != "info": 
                plataforma_p = plat
                # Extrai o ID para a sua lógica de deduplicação profissional
                id_m = re.search(r'/(?:dp|MLB|product|i\.)/([A-Z0-9.\-_]+)', novo)
                prod_id = id_m.group(1) if id_m else novo[-15:]
            if force_img: forcar_img_ml = True

    # 🛑 BLOQUEIO: Se a oferta original tinha links, mas nenhum foi convertido, ABORTA.
    if links_detectados and not links_conv:
        print("🔎 DEBUG | Bloqueado: Link desconhecido/estranho detectado.")
        return

    # 3. Extração de dados para a Deduplicação Profissional (Sua Lógica JSON)
    preco_m = re.search(r'R\$\s?\d+[.,\d]*', texto_bruto)
    preco = preco_m.group(0) if preco_m else "0"
    cupom_m = re.search(r'\b([A-Z0-9]{5,25})\b', texto_bruto)
    cupom = cupom_m.group(1) if cupom_m else ""

    # 4. APLICA SUA LÓGICA DE DEDUPLICAÇÃO (Similaridade 90% / Janela 15min)
    if not is_edit:
        if not deve_enviar_oferta(plataforma_p, prod_id, preco, cupom, texto_bruto):
            print("🔎 DEBUG | Bloqueado por similaridade/deduplicação.")
            return

    # 5. LÓGICA DE IMAGEM (Suas Regras Rígidas)
    imagem = None
    tem_media_original = event.message.media and not isinstance(event.message.media, MessageMediaWebPage)
    is_ml_cupom = any(x in texto_bruto.upper() for x in ["CUPOM", "CUPONS"]) and "MERCADO LIVRE" in texto_bruto.upper()

    if forcar_img_ml or is_ml_cupom:
        # REGRA ML: Lista, Social ou Cupom ML -> SEMPRE FIXA (Apaga a original)
        imagem = IMG_ML_FIXA
    elif username == "fadadoscupons":
        # REGRA CUPONS: Se tem foto original, usa. Se não, usa a fixa da loja.
        if tem_media_original:
            imagem = event.message.media
        else:
            txt_l = texto_final.lower()
            if "shopee" in txt_l: imagem = IMG_SHOPEE_FIXA
            elif "amazon" in txt_l: imagem = IMG_AMAZON_FIXA
            else: imagem = IMG_ML_FIXA
    elif tem_media_original:
        # REGRA GERAL: Usa a foto que veio na oferta
        imagem = event.message.media
    elif links_conv:
        # REGRA SEM FOTO: Tenta buscar imagem no site 3x (Scrape)
        imagem = await buscar_imagem_3x(links_conv[0])

    # 6. ENVIO / EDIÇÃO ATIVA (Bypass de Flood)
    async with envio_lock:
        try:
            # Carrega mapeamento para edições (Edição Ativa)
            mapping = json.load(open(ARQUIVO_MAPEAMENTO, "r")) if os.path.exists(ARQUIVO_MAPEAMENTO) else {}
            
            if is_edit and str(event.message.id) in mapping:
                msg_id_dest = mapping[str(event.message.id)]
                try: 
                    await client.edit_message(GRUPO_DESTINO, msg_id_dest, texto_final)
                    print(f"🔎 DEBUG | Mensagem editada com sucesso.")
                except MessageNotModifiedError: 
                    pass
            else:
                sent = None
                # Tratamento de legenda longa (>1024 caracteres)
                if imagem:
                    if len(texto_final) > 1024:
                        photo_msg = await client.send_file(GRUPO_DESTINO, imagem)
                        sent = await client.send_message(GRUPO_DESTINO, texto_final, reply_to=photo_msg.id)
                    else:
                        sent = await client.send_file(GRUPO_DESTINO, imagem, caption=texto_final)
                else:
                    # Sem foto e sem scrape: Envia texto puro (ativa preview automático)
                    sent = await client.send_message(GRUPO_DESTINO, texto_final)
                
                if sent:
                    mapping[str(event.message.id)] = sent.id
                    json.dump(mapping, open(ARQUIVO_MAPEAMENTO, "w"))
                    print("✅ DEBUG | Oferta enviada escorregando!")
        except Exception as e: 
            print(f"❌ DEBUG | Erro no envio: {e}")


# ============================================================
# 🔹 MOTOR DE PROCESSAMENTO
# ============================================================

async def processar_evento(event, is_edit=False):
    texto_bruto = event.message.text or ""
    if not texto_bruto.strip() or any(p.lower() in texto_bruto.lower() for p in FILTRO): return

    links_detectados = re.findall(r'https?://\S+', texto_bruto)
    chat = await event.get_chat()
    username = (chat.username or "").lower()

    if not links_detectados and username != "fadadoscupons": return

    print(f"DEBUG | Iniciando conversão de {len(links_detectados)} links.")
    links_conv, force_img_ml, plat_p, prod_id = [], False, "outro", "0"

    for link in links_detectados[:50]:
        novo, plat, img_f = await converter_link(link)
        if novo:
            links_conv.append(novo)
            if plat != "info": 
                plat_p = plat
                prod_id = re.search(r'/(?:dp|MLB|product|i\.)/([A-Z0-9.\-_]+)', novo).group(1) if re.search(r'/(?:dp|MLB|product|i\.)/([A-Z0-9.\-_]+)', novo) else novo[-15:]
            if img_f: force_img_ml = True

    # BLOQUEIO: Se tinha link mas nada sobrou das 4 lojas, cancela.
    if links_detectados and not links_conv:
        print("DEBUG | Oferta descartada (Links não permitidos).")
        return

    preco = re.search(r'R\$\s?\d+[.,\d]*', texto_bruto).group(0) if re.search(r'R\$\s?\d+[.,\d]*', texto_bruto) else "0"
    cupom = re.search(r'\b([A-Z0-9]{5,25})\b', texto_bruto).group(1) if re.search(r'\b([A-Z0-9]{5,25})\b', texto_bruto) else ""

    if not is_edit and not deve_enviar_oferta(plat_p, prod_id, preco, cupom, texto_bruto):
        print("DEBUG | Bloqueado por Deduplicação.")
        return

    final_msg = formatar_texto(texto_bruto, links_conv)

    # 🔹 LÓGICA DE IMAGEM
    imagem = None
    tem_media = event.message.media and not isinstance(event.message.media, MessageMediaWebPage)
    is_ml_cupom = "CUPOM" in texto_bruto.upper() and "MERCADO LIVRE" in texto_bruto.upper()

    if force_img_ml or is_ml_cupom:
        imagem = IMG_ML_FIXA
    elif username == "fadadoscupons":
        if tem_media: imagem = event.message.media
        else:
            if "shopee" in str(links_conv).lower(): imagem = IMG_SHOPEE_FIXA if os.path.exists(IMG_SHOPEE_FIXA) else IMG_ML_FIXA
            else: imagem = IMG_AMAZON_FIXA if os.path.exists(IMG_AMAZON_FIXA) else IMG_ML_FIXA
    elif tem_media:
        imagem = event.message.media
    elif links_conv:
      # ============================================================
# 🔹 FORMATAÇÃO
# ============================================================

def formatar_texto_fiel(texto, mapa_links):
    # 1. Remove os nomes Produto, Preço, Cupom, Valor (Mantém o texto da frente)
    texto = re.sub(r'(?i)(produto|preço|cupom|valor):\s*', '', texto)
    
    # 2. Troca os links originais pelos convertidos dentro do texto
    for link_velho, link_novo in mapa_links.items():
        texto = texto.replace(link_velho, link_novo)
    
    # 3. Ajuste de Emojis: Só coloca se a linha não começar com um emoji/símbolo
    linhas = texto.split('\n')
    novas_linhas = []
    for i, linha in enumerate(linhas):
        linha = linha.strip()
        if not linha: continue
        
        # Se a linha não começar com emoji ou caractere especial
        if not re.match(r'[^\w\s]', linha):
            if i == 0: linha = "✅ " + linha
            elif "R$" in linha: linha = "🔥 " + linha
            elif len(linha) < 30 and any(c.isdigit() for c in linha): linha = "🎟 " + linha
        
        novas_linhas.append(linha)
    
    return "\n".join(novas_linhas).strip()

# ============================================================
# 🔹 MOTOR DE PROCESSAMENTO
# ============================================================

async def processar_evento(event, is_edit=False):
    texto_bruto = event.message.text or ""
    if not texto_bruto.strip(): return

    # 1. Filtro de palavras (Blacklist)
    if any(p.lower() in texto_bruto.lower() for p in FILTRO):
        print(f"🔎 DEBUG | Bloqueado pelo filtro.")
        return

    chat = await event.get_chat()
    username = (chat.username or "").lower()
    links_detectados = re.findall(r'https?://\S+', texto_bruto)
    
    # Bloqueia se não for grupo de cupons e não tiver link
    if not links_detectados and username != "fadadoscupons": return

    # 2. Converte os links (Até 50) e identifica plataforma/ID
    mapa_links_convertidos = {}
    forcar_img_ml = False
    plataforma_p, prod_id = "outro", "0"

    for link in links_detectados[:50]:
        # Desencurtador Elite (Amazon, ML, Magalu, Shopee)
        novo, plat, force_img = await converter_link(link) 
        if novo:
            mapa_links_convertidos[link] = novo
            if plat != "info": 
                plataforma_p = plat
                id_m = re.search(r'/(?:dp|MLB|product|i\.)/([A-Z0-9.\-_]+)', novo)
                prod_id = id_m.group(1) if id_m else novo[-15:]
            if force_img: forcar_img_ml = True

    # 🛑 BLOQUEIO CRÍTICO: Se tinha link mas nada foi convertido para as 4 lojas, ABORTA.
    if links_detectados and not mapa_links_convertidos:
        print("🔎 DEBUG | Bloqueado: Link estranho/desconhecido detectado.")
        return

    # 3. Extração para Deduplicação Profissional (Sua Lógica JSON)
    preco_m = re.search(r'R\$\s?\d+[.,\d]*', texto_bruto)
    preco = preco_m.group(0) if preco_m else "0"
    cupom_m = re.search(r'\b([A-Z0-9]{5,25})\b', texto_bruto)
    cupom = cupom_m.group(1) if cupom_m else ""

    # 4. APLICA SUA LÓGICA DE DEDUPLICAÇÃO
    if not is_edit:
        if not deve_enviar_oferta(plataforma_p, prod_id, preco, cupom, texto_bruto):
            print("🔎 DEBUG | Bloqueado por similaridade/deduplicação.")
            return

    # 5. MONTAGEM DA MENSAGEM (TEXTO FIEL + LINKS TROCADOS)
    final_msg = formatar_texto_fiel(texto_bruto, mapa_links_convertidos)

    # 6. LÓGICA DE IMAGEM (Suas Regras Rígidas)
    imagem = None
    tem_media_original = event.message.media and not isinstance(event.message.media, MessageMediaWebPage)
    txt_upper = texto_bruto.upper()
    is_ml_cupom = ("CUPOM" in txt_upper or "CUPONS" in txt_upper) and "MERCADO LIVRE" in txt_upper
    is_shopee_cupom = "CUPOM" in txt_upper and "SHOPEE" in txt_upper
    is_amazon_cupom = "CUPOM" in txt_upper and "AMAZON" in txt_upper

    if forcar_img_ml or is_ml_cupom:
        # REGRA ML: Lista, Social ou Cupom ML -> SEMPRE FIXA
        imagem = IMG_ML_FIXA
    elif username == "fadadoscupons" or is_shopee_cupom or is_amazon_cupom:
        # REGRA CUPONS: Se tem foto original, usa. Se não, usa a fixa da respectiva loja.
        if tem_media_original:
            imagem = event.message.media
        else:
            if "shopee" in final_msg.lower(): imagem = IMG_SHOPEE_FIXA
            elif "amazon" in final_msg.lower(): imagem = IMG_AMAZON_FIXA
            else: imagem = IMG_ML_FIXA
    elif tem_media_original:
        # REGRA GERAL: Usa a foto que veio na oferta
        imagem = event.message.media
    elif mapa_links_convertidos:
        # REGRA SEM FOTO: Tenta buscar imagem no site 3x
        imagem = await buscar_imagem_3x(list(mapa_links_convertidos.values())[0])

    # 7. ENVIO / EDIÇÃO ATIVA
    async with envio_lock:
        try:
            if not os.path.exists(ARQUIVO_MAPEAMENTO): mapping = {}
            else:
                with open(ARQUIVO_MAPEAMENTO, "r") as f: mapping = json.load(f)
            
            if is_edit and str(event.message.id) in mapping:
                msg_id_dest = mapping[str(event.message.id)]
                try: 
                    await client.edit_message(GRUPO_DESTINO, msg_id_dest, final_msg)
                    print(f"🔎 DEBUG | Mensagem editada com sucesso.")
                except MessageNotModifiedError: pass
            else:
                sent = None
                if imagem:
                    if len(final_msg) > 1024:
                        # Envia imagem e depois texto na própria imagem (legenda longa)
                        photo_msg = await client.send_file(GRUPO_DESTINO, imagem)
                        sent = await client.send_message(GRUPO_DESTINO, final_msg, reply_to=photo_msg.id)
                    else:
                        sent = await client.send_file(GRUPO_DESTINO, imagem, caption=final_msg)
                else:
                    sent = await client.send_message(GRUPO_DESTINO, final_msg)
                
                if sent:
                    mapping[str(event.message.id)] = sent.id
                    with open(ARQUIVO_MAPEAMENTO, "w") as f: json.dump(mapping, f)
                    print("✅ DEBUG | Oferta enviada escorregando!")
        except Exception as e: 
            print(f"❌ DEBUG | Erro no envio: {e}")
# ============================================================
# 🔹 START
# ============================================================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

async def main():
    await client.connect()
    if not await client.is_user_authorized(): return
    print("🚀 BOT MASTER v43.0 ONLINE!")
    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def n_h(e): await processar_evento(e)
    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def e_h(e): await processar_evento(e, is_edit=True)
    await client.run_until_disconnected()

if __name__ == '__main__': asyncio.run(main())

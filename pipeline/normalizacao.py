"""Camada 3 — Normalização: filtro, limpeza, cupom, estado, desencurtamento."""
from __future__ import annotations
import asyncio
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import aiohttp
from bs4 import BeautifulSoup

import config
from database import db_get_dedupe, db_get_link, db_set_link
from globals import _get_session, _get_raw, _get_final, _set_raw, _log_cache_stats
from logger import log_cls, log_nrm
from pipeline.classificacao import (
    LinkClassificado, _AMZ_DOMINIOS, _ENCURTADORES, _FORCA_GET,
    _MGL_DOMINIOS, _SHP_DOMINIOS, _P_AMZ_ASIN, _P_MGL,
    _AMZ_PATHS_SEM_TAG, _classificar_cached, classificar_links,
    _eh_magalu_url,
)
from pipeline.ingestao import MensagemBruta
from utils.hashes import _fp_c3
from utils.urls import _netloc, _sanitizar_url

# ── Janelas de deduplicação por plataforma ────────────────────────
_JANELA_C3: Dict[str, float]    = {"shopee":60.0, "amazon":300.0, "magalu":300.0, "default":120.0}
_TTL_RESTOCK_C3: Dict[str, float] = {"shopee":3600.0, "amazon":7200.0, "magalu":14400.0, "default":3600.0}

# ── 3a. Filtro de texto ───────────────────────────────────────────
_FILTRO_TEXTO = [
    "Monitor Samsung","Fonte Mancer","Placa de video","Monitor LG",
    "PC home Essential","Suporte articulado","VHAGAR","Superframe","AM5","AM4","GTX",
    "Placa de Vídeo","DDR5","DDR4","Dram","Monitor Safe","Monitor Redragon","CL18","CL16",
    "CL32","MT/s","MHz","RX 580","Ryzen","Placa Mãe","Gabinete Gamer",
    "Water Cooler","Monitor Dell","Monitor Gamer","Air Cooler",
]
_RE_MERCADO_LIVRE = re.compile(r'\b(?:mercado\s*livre|mercadolivre|mercado\s*pago)\b', re.I)
_RE_MULTI_OFERTA  = re.compile(
    r'\b(?:ofertas?|promoções?)\s+(?:na\s+|no\s+|da\s+)?'
    r'(?:shopee|amazon|magalu|magazine\s*luiza)\b', re.I)
_RE_PRECO_LINHA   = re.compile(r'R\$\s?[\d.,]+')
_RE_URL_COUNT     = re.compile(r'https?://')
_RE_PRECO_FORTE   = re.compile(
    r'(?:r\$\s*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\b|\d+\s*%\s*off|r\$\s*\d+\s*off)', re.I)
_RE_CUPOM_FORTE   = re.compile(
    r'\b(?:cupom|coupon|c[oó]digo)\b.*\b[A-Z][A-Z0-9_-]{3,19}\b', re.I)


def _tem_sinal_preco_forte(texto: str) -> bool:
    return bool(_RE_PRECO_FORTE.search(texto)) and bool(_RE_CUPOM_FORTE.search(texto))


def _eh_multi_produto(texto: str) -> bool:
    if _RE_MULTI_OFERTA.search(texto): return True
    linhas_preco = sum(1 for l in texto.splitlines() if _RE_PRECO_LINHA.search(l))
    return linhas_preco >= 2 or len(_RE_URL_COUNT.findall(texto)) >= 3


def _tem_link_plataforma(links: List[str]) -> bool:
    from utils.urls import _netloc as netloc
    for u in links:
        nl = netloc(u)
        for d in (*_AMZ_DOMINIOS, *_SHP_DOMINIOS, *_MGL_DOMINIOS,
                  *_ENCURTADORES, *_FORCA_GET):
            if nl == d or nl.endswith("." + d):
                return True
    return False


def texto_bloqueado(texto: str) -> Tuple[bool, bool]:
    """Retorna (bloqueado, is_override)."""
    if _RE_MERCADO_LIVRE.search(texto):
        log_cls.debug("🚫 Mercado Livre"); return True, False
    if _eh_multi_produto(texto): return False, False
    tl = texto.lower()
    for p in _FILTRO_TEXTO:
        if p.lower() in tl:
            if _tem_sinal_preco_forte(texto):
                log_cls.debug(f"⚡ Override filtro '{p}' — cupom+preço forte detectado")
                return False, True
            log_cls.debug(f"🚫 Filtro: '{p}'"); return True, False
    return False, False


# ─────────────────────────────────────────────────────────────────
# FILTROS DE POST INDESEJADO (v80.3) — auditoria sênior
# ─────────────────────────────────────────────────────────────────
# Bloqueia posts que NÃO devem ser republicados por motivos
# qualitativos (não relacionados ao filtro de produto).
#
# Categorias detectadas:
#   A) "Exclusivo do canal/grupo/@nome" — não é pra todos
#   B) Posts que pedem pro usuário "enviar link" (serviço genérico)
#   C) URLs Shopee de vídeo (/v/, /vt/, /live/)
#   D) Posts com APENAS links de sites concorrentes
#   E) Posts redirecionando pra outro canal/grupo Telegram
# ─────────────────────────────────────────────────────────────────

# A) Exclusividade declarada
# Detecta "exclusivo do canal", "exclusivo do grupo", "exclusivo @nome",
# "só do canal", "somente do canal", etc.
_RE_EXCLUSIVO_CANAL = re.compile(
    r'\b(?:exclusivo|exclusiva|s[oó]|somente|apenas)\s+'
    r'(?:do|da|de|para|pra|p/|pro|pros)\s+'
    r'(?:canal|grupo|membros?|seguidores?|@\w+)',
    re.I,
)

# B) Posts de "serviço de cupom" — pedem usuário enviar link
# Detecta "envie o link", "mande seu link", "manda o link", "envia aí o link"
_RE_PEDE_LINK = re.compile(
    r'\b(?:envie?|mande|manda|envia|coloque|cole|cola|passe|passa)\s+'
    r'(?:o\s+|seu\s+|os\s+|aí\s+(?:o\s+)?)?link',
    re.I,
)

# Combinado com "no chat" / "pelo site" / "pelo nosso site"
_RE_VIA_CHAT_SITE = re.compile(
    r'\b(?:no\s+chat|pelo\s+(?:nosso\s+)?site|aqui\s+no\s+grupo|'
    r'aqui\s+no\s+canal|aqui\s+embaixo|abaixo|aba\s+de)\b',
    re.I,
)

# C) URLs de vídeo Shopee (não devem ser republicadas)
_RE_SHOPEE_VIDEO = re.compile(
    r'https?://(?:[a-z0-9-]+\.)?shopee\.com\.br/'
    r'(?:v|vt|live|video)/',
    re.I,
)

# D) Domínios de sites de cupom CONCORRENTES (não são afiliados seus)
# Atenção: lista pode crescer conforme aparece no monitoramento.
_DOMINIOS_CONCORRENTES = frozenset({
    "fadadoscupons.com", "fadadoscupons.com.br",
    "pelando.com.br", "pelando.com",
    "promobit.com.br", "promobit.com",
    "savvii.com.br",
    "tecmundo.com.br/promobit",
    "buscape.com.br/cupom",
    "meliuz.com.br",
    "cuponomia.com.br",
    "cuponeria.com.br",
    "picodi.com",
    "cupomvalido.com.br",
})

# E) Links que redirecionam pra outros canais/grupos Telegram
_RE_TELEGRAM_LINK = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org)/',
    re.I,
)


def _tem_link_concorrente(texto: str) -> bool:
    """Verifica se o texto tem link pra site de cupom concorrente."""
    from utils.urls import _netloc as netloc
    urls = re.findall(r'https?://[^\s\)>\]\}",;]+', texto)
    for url in urls:
        nl = netloc(url)
        if nl in _DOMINIOS_CONCORRENTES:
            return True
        for d in _DOMINIOS_CONCORRENTES:
            if nl.endswith("." + d):
                return True
    return False


def _tem_link_plataforma_real(texto: str) -> bool:
    """
    Verifica se o texto tem PELO MENOS UM link de plataforma real
    (Amazon/Shopee/Magalu) ou encurtador conhecido.
    """
    from utils.urls import _netloc as netloc
    urls = re.findall(r'https?://[^\s\)>\]\}",;]+', texto)
    if not urls:
        return False
    for url in urls:
        nl = netloc(url)
        for d in (*_AMZ_DOMINIOS, *_SHP_DOMINIOS, *_MGL_DOMINIOS,
                  *_ENCURTADORES, *_FORCA_GET):
            if nl == d or nl.endswith("." + d):
                return True
    return False


def _eh_post_indesejado(texto: str) -> Tuple[bool, str]:
    """
    Detecta posts que não devem ser republicados (filtros qualitativos).

    Returns:
        (bloqueado, motivo). Se bloqueado=False, motivo é "".
    """
    # ── A) Exclusividade declarada ────────────────────────────
    if _RE_EXCLUSIVO_CANAL.search(texto):
        return True, "exclusivo_canal"

    # ── C) URL Shopee de vídeo ────────────────────────────────
    if _RE_SHOPEE_VIDEO.search(texto):
        return True, "shopee_video"

    # ── B) Pede pra enviar link + via chat/site ───────────────
    # Combinação obrigatória: pede link + indica onde enviar
    # (evita falso positivo de "envie esse link pra alguém")
    if _RE_PEDE_LINK.search(texto) and _RE_VIA_CHAT_SITE.search(texto):
        return True, "pede_link_servico"

    # ── D) APENAS link de site concorrente (sem link plataforma) ──
    # Se tem fadadoscupons.com mas NÃO tem amazon/shopee/magalu,
    # é serviço de outro canal — bloqueia.
    if _tem_link_concorrente(texto) and not _tem_link_plataforma_real(texto):
        return True, "link_concorrente_sem_plataforma"

    # ── E) Apenas link Telegram (encaminhamento pra outro canal) ──
    if (_RE_TELEGRAM_LINK.search(texto)
            and not _tem_link_plataforma_real(texto)):
        return True, "encaminha_telegram"

    return False, ""


# ── 3b. Limpeza de ruído textual ─────────────────────────────────
_RE_INVISIVEIS  = re.compile(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]')
_RE_GRUPO_EXT   = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)[^\s]*', re.I)
_RE_LIXO_STRUCT = re.compile(
    r'^\s*(?:-?\s*An[uú]ncio|Publicidade|:::+|---+|===+|'
    r'[-–—]\s*(?:ML|MG|AMZ)|(?:ML|MG|AMZ)\s*:)\s*$', re.I)
_RE_CTA = re.compile(
    r'^\s*(?:link\s+(?:do\s+)?produto|link\s+da\s+oferta|resgate\s+aqui|'
    r'clique\s+aqui|acesse\s+aqui|compre\s+aqui|grupo\s+vip|'
    r'entrar\s+no\s+grupo|acessar\s+grupo)\s*:?\s*$', re.I)
_RE_REDES = re.compile(
    r'^\s*(?:redes\s+\w+|[-–]\s*grupo\s*(?:cupons?|promoções?|vip)?\s*:?\s*$|'
    r'[-–]\s*(?:chat|twitter|whatsapp|instagram|tiktok|youtube)\s*:?\s*$|'
    r'acesse\s+nossas\s+redes)', re.I)
_RE_ROTULO    = re.compile(r'^\s*[-–•]\s*\w[\w\s]{0,30}:\s*$')
_RE_EMOJI_CHK = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF\u2B50\u2B55]")
_KW_EVENTO    = re.compile(
    r'\b(?:quiz|roleta|miss[aã]o|arena|girar|gire|roda|jogar|jogue|desafio)\b', re.I)


def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI_CHK.search(s))


def _eh_header_canal(linha: str) -> bool:
    l = linha.strip()
    if not l or _tem_emoji(l[0]): return False
    if re.match(r'^[A-ZÀ-Ú][\w\s]{2,30}\s*/\s*[\w\s]{2,30}', l): return True
    if re.match(r'^[A-ZÀÁÂÃÉÊÍÓÔÕÚ\s]{4,30}[\s🔥💥⚡🚀]+$', l, re.UNICODE): return True
    return False


def limpar_texto(texto: str) -> str:
    texto = _RE_INVISIVEIS.sub(" ", texto).replace("\r\n", "\n").replace("\r", "\n")
    linhas = texto.split("\n"); saida: List[str] = []
    vazio = False; em_redes = False; primeira = True
    for linha in linhas:
        l = linha.strip()
        if not l:
            if not vazio: saida.append("")
            vazio = True; em_redes = False; continue
        vazio = False
        if primeira:
            primeira = False
            if _eh_header_canal(l): continue
        if _RE_REDES.match(l): em_redes = True; continue
        if em_redes:
            if _RE_ROTULO.match(l) or not l: continue
            if not re.match(r'https?://', l): em_redes = False
            else: continue
        if _RE_CTA.match(l) or _RE_LIXO_STRUCT.match(l): continue
        if _RE_GRUPO_EXT.search(l):
            l = _RE_GRUPO_EXT.sub("", l).strip()
            if not l: continue
        saida.append(l)
    return "\n".join(saida).strip()


# ── 3c. Extração cupom / SKU ─────────────────────────────────────
# Sistema em 3 NÍVEIS DE CONFIANÇA:
#   ALTA  : código entre crases (`CODIGO`) — 100% certeza, vem do Telegram
#           formatado como CODE entity. Divulgadores profissionais usam isso.
#   MÉDIA : código próximo à palavra-chave (Cupom: CODIGO, Código: CODIGO)
#   BAIXA : código solto em maiúsculas no texto (heurística, pode falhar)
#
# Quando há crases, SEMPRE prefere a confiança ALTA (mesmo que outra
# regex pegue diferente). Esse é o sinal mais forte que existe.
# ─────────────────────────────────────────────────────────────────
# EXTRAÇÃO DE CUPOM — Sistema em 5 níveis de confiança (v80.3)
# ─────────────────────────────────────────────────────────────────
# Filosofia: errar pra MENOS é melhor que errar pra MAIS.
# Em caso de dúvida, NÃO extrair (deixa cupom vazio).
# Cupom vazio = oferta tratada como "produto" (identidade pelo ASIN/SKU).
# Cupom errado = duplicações ou conflitos de identidade — RUIM.
#
# NÍVEL 1 (100% certeza): código entre crases (CODE entity do Telegram).
#                          Divulgador profissional SEMPRE formata assim.
# NÍVEL 2 (95% certeza):   formato "R$ X OFF em R$ Y: CODIGO"
#                          (lista de cupons formato pro)
# NÍVEL 3 (90% certeza):   "OFF: CODIGO" / "Cupom: CODIGO" / "Código: CODIGO"
#                          (formato chave-valor no texto)
# NÍVEL 4 (60% certeza):   código alfanumérico próximo à palavra "cupom"
#                          (até 3 linhas de distância)
# NÍVEL 5 (descartado):    se nenhum nível bateu, retorna ""
#                          (melhor do que arriscar pegar palavra falsa)
# ─────────────────────────────────────────────────────────────────

# Palavras-chave que indicam menção a cupom no texto
_KW_CUPOM = re.compile(
    r'\b(?:cupom|cupons|c[oó]digo|c[oó]digos|coupon|coupons|voucher|vouchers)\b',
    re.I,
)

# Padrão de código: aceita começar com NÚMERO (cupom "555H0PPR3C0")
# OU letra (cupom "MASTER15OFF"). Mínimo 4 chars, máximo 20.
_KW_COD = re.compile(r'\b([A-Z0-9][A-Z0-9_-]{3,19})\b')

# Validação interna: código puro (usado em _eh_cupom_valido)
_RE_COD_PURO = re.compile(r'^[A-Z0-9][A-Z0-9_-]{3,19}$')

# Detecta presença de letra E número (cupom misto típico)
_RE_TEM_LETRA = re.compile(r'[A-Z]')
_RE_TEM_DIGITO = re.compile(r'[0-9]')

# Lista BLINDADA de FALSO_CUPOM — palavras que NUNCA são cupom mesmo
# que apareçam em maiúsculas. Organizada por categoria.
_FALSO_CUPOM = frozenset({
    # ── Palavras óbvias (a #1 que causou o bug do 555H0PPR3C0) ──
    "CUPOM", "CUPONS", "CODIGO", "CÓDIGO", "COUPON", "COUPONS",
    "VOUCHER", "VOUCHERS", "DESCONTO", "DESCONTOS", "PROMO",
    "PROMOÇÃO", "PROMOCAO", "OFERTA", "OFERTAS", "EXCLUSIVO",
    "EXCLUSIVA", "OFICIAL", "RESGATE", "RESGATES", "GRÁTIS",
    "GRATIS", "FRETE", "FRETES", "COMPRA", "COMPRE", "PAGUE",
    "ATIVO", "ATIVAR", "USAR", "CLIQUE", "ACESSE", "CONFIRA",
    "COPIE", "AGORA", "HOJE", "NOVO", "NOVA", "MEGA", "ULTRA",
    "SUPER", "TOP", "PREMIUM", "BLACK", "FRIDAY", "CYBER",
    "SOMENTE", "APENAS", "BRASIL", "BRASILEIRO", "MUNDO",
    "TESTE", "DEMO", "BETA", "AJUDA", "HELP", "MAIS", "MENOS",
    "VOLTA", "VOLTOU", "RENOVADO", "REATIVADO", "NORMALIZOU",
    "RECUPERADO", "DISPONIVEL", "DISPONÍVEL", "VALIDA", "VÁLIDA",
    "VALIDO", "VÁLIDO", "EXPIRADO", "EXPIROU",

    # ── Marcas (nunca são cupom) ──────────────────────────────
    "AMAZON", "AMZN", "SHOPEE", "MAGALU", "MAGAZINE", "LUIZA",
    "ALIEXPRESS", "ALIBABA", "MERCADO", "LIVRE", "AMERICANAS",
    "CASAS", "BAHIA", "EXTRA", "CARREFOUR", "PAODE", "ACUCAR",
    "SUBMARINO", "DAFITI", "NETSHOES", "CENTAURO", "RIACHUELO",
    "RENNER", "SAMSUNG", "APPLE", "PHILIPS", "XIAOMI", "MOTOROLA",
    "NOKIA", "MICROSOFT", "INTEL", "AMD", "NVIDIA", "ASUS", "ACER",
    "DELL", "LENOVO", "LG", "SONY", "PANASONIC", "BOSCH", "BRASTEMP",
    "CONSUL", "ELETROLUX", "ELECTROLUX", "CADENCE", "MONDIAL", "ARNO",
    "OSTER", "BRAUN", "FAGOR", "ITATIAIA", "TRAMONTINA", "POLISHOP",
    "PHILCO", "LATINA", "BLACK", "DECKER", "MAKITA", "BRITANIA",
    "BRITÂNIA", "ALPINO", "PAMPERS", "NESTLE", "NESTLÉ", "POSITIVO",
    "INTELBRAS", "MALIBU", "LOGITECH", "RAZER", "REDRAGON", "HYPER",
    "CORSAIR", "KINGSTON", "WESTERN", "DIGITAL", "SEAGATE", "SANDISK",
    "TOSHIBA", "HUAWEI", "VIVO", "CLARO", "OI", "TIM",

    # ── Plataformas / serviços ─────────────────────────────────
    "NETFLIX", "DISNEY", "DISNEYPLUS", "GLOBOPLAY", "PRIME", "VIDEO",
    "PARAMOUNT", "HBOMAX", "STAR", "APPLETV", "YOUTUBE", "SPOTIFY",
    "DEEZER", "TWITCH", "STEAM", "EPIC", "GOG", "PLAYSTATION", "NINTENDO",
    "XBOX", "PSN", "PSPLUS",

    # ── Categorias e termos técnicos ───────────────────────────
    "GAMING", "GAMER", "OFFICE", "HOMEOFFICE", "WORK", "PRO", "MAX",
    "PLUS", "LITE", "MINI", "AIR", "PROMAX", "STANDARD", "BASIC",
    "EXTRA", "OUTLET", "DUTYFREE", "FREESHIP", "FREE", "PAID",

    # ── Unidades e siglas técnicas ─────────────────────────────
    "USD", "BRL", "EUR", "USB", "HDMI", "BLE", "WIFI", "GPS", "NFC",
    "IPS", "OLED", "LCD", "LED", "ATX", "ITX", "ITV", "IPTV", "OTT",
    "P2P", "B2B", "B2C", "KPI", "KYC", "CPU", "GPU", "RAM", "ROM",
    "SSD", "HDD", "TB", "GB", "MB", "KB", "FPS", "HZ", "GHZ", "MHZ",
    "RGB", "HDR", "SDR", "PWM", "PCIE", "SATA", "DDR", "DDR4", "DDR5",
    "M2", "NVME", "USB", "RJ45", "VGA", "DVI", "TYPE", "TYPEC",
    "PS3", "PS4", "PS5", "WII", "DEX", "API", "SDK", "URL", "URI",
    "JSON", "XML", "HTTP", "HTTPS", "TCP", "UDP", "DNS", "IP", "SQL",
    "ETL", "OCR", "VPN", "SSO", "MFA", "OTP", "ASTRO", "DIGITAL",
    "SLIM", "GRAN", "TURISMO", "PACOTE",

    # ── Termos de pagamento/fiscal ─────────────────────────────
    "PIX", "BOLETO", "CARTAO", "CARTÃO", "MASTER", "MASTERCARD",
    "VISA", "AMEX", "ELO", "HIPERCARD", "CREDITO", "CRÉDITO",
    "CREDIT", "DEBITO", "DÉBITO", "DEBIT", "MOEDA", "MOEDAS",
    "JURO", "JUROS", "PARCELA", "PARCELAS", "REAL", "REAIS",
    "DOLAR", "DÓLAR", "DOLLAR", "EURO", "VALOR", "VALORES",
    "PRECO", "PREÇO", "PRECOS", "PREÇOS", "BARATO", "BARATA",

    # ── Cashback / pontos (não são cupom-code) ────────────────
    "CASHBACK", "CASH", "BACK", "PONTOS", "PONTO", "MILHAS", "MILHA",

    # ── Termos de roleta / evento ─────────────────────────────
    "ROLETA", "GIRO", "GIROS", "ARENA", "QUIZ", "MISSAO", "MISSÃO",
    "DESAFIO", "SORTEIO", "PREMIO", "PRÊMIO", "PREMIOS", "PRÊMIOS",
    "EVENTO", "EVENTOS", "CAMPANHA", "CAMPANHAS",

    # ── Direção / status ──────────────────────────────────────
    "OFF", "ON", "OK", "GO", "STOP", "PAUSE", "PLAY", "STAR",
    "STARS", "FIRE", "HOT", "COLD", "WARM",
})

# Padrão "lista de cupons profissional" — formato típico:
# "R$ 100 OFF em R$ 1000: QUERO100"
# "Cupons ainda ativos:"
_RE_LISTA_CUPONS = re.compile(
    r'(?:r\$\s*\d+\s+off\s+em\s+r\$\s*\d+\s*:\s*[A-Z0-9]{4,}|'
    r'cupons?\s+(?:ainda\s+)?ativos?\s*:|ainda\s+ativos?\s*:)',
    re.I,
)

# Padrão chave-valor estrito: "PALAVRA: CODIGO"
# Ex: "OFF: BOMDIA10", "Cupom: 555H0PPR3C0"
_RE_KV_CUPOM = re.compile(
    r'(?:OFF|cupom|cupons|c[oó]digo|c[oó]digos|coupon|voucher)\s*[:=]\s*'
    r'([A-Z0-9][A-Z0-9_-]{3,19})\b',
    re.I,
)


def _eh_cupom_valido(c: str) -> bool:
    """
    Validação STRICT de candidato a cupom.

    Regras (todas precisam passar):
      1. Não vazio
      2. 4-20 chars
      3. NÃO está em _FALSO_CUPOM (palavras óbvias bloqueadas)
      4. Formato puro [A-Z0-9][A-Z0-9_-]{3,19}
      5. Tem PELO MENOS 1 letra (não pode ser só números: "1200" não é cupom)
      6. Se tem só letras, precisa ter >= 5 chars (rejeita "ABCD")
    """
    if not c:
        return False
    if len(c) < 4 or len(c) > 20:
        return False
    c_upper = c.upper()
    if c_upper in _FALSO_CUPOM:
        return False
    if not _RE_COD_PURO.match(c_upper):
        return False
    # Tem que ter pelo menos 1 letra
    tem_letra = bool(_RE_TEM_LETRA.search(c_upper))
    tem_digito = bool(_RE_TEM_DIGITO.search(c_upper))
    if not tem_letra:
        return False  # só números: "1200", "555" não são cupons
    # Se for só letras, exige >= 5 chars
    if not tem_digito and len(c_upper) < 5:
        return False
    return True


def _filtrar_codes_validos(code_entities: list) -> List[str]:
    """
    Pega entidades CODE do Telegram e retorna só as que parecem cupom válido.
    Pode ter mais de um se divulgador formatou múltiplos códigos.
    """
    if not code_entities:
        return []
    validos: List[str] = []
    for trecho in code_entities:
        candidato = trecho.strip()
        if _eh_cupom_valido(candidato):
            validos.append(candidato.upper())
            continue
        # Trecho pode ter palavras múltiplas — testa cada
        for palavra in re.findall(r'[A-Z0-9][A-Z0-9_-]{3,19}', candidato):
            if _eh_cupom_valido(palavra):
                validos.append(palavra.upper())
    return validos


def extrair_cupom_de_codes(code_entities: list) -> str:
    """NÍVEL 1 (100% certeza) — primeiro código válido entre crases."""
    validos = _filtrar_codes_validos(code_entities)
    return validos[0] if validos else ""


def extrair_cupom(texto: str, code_entities: list = None) -> str:
    """
    Extrai o código de cupom do texto com 5 níveis de confiança.

    Em caso de dúvida, retorna "" (vazio). É melhor não extrair do que
    extrair errado e causar conflito de identidade na deduplicação.

    Args:
        texto: texto cru da mensagem
        code_entities: lista de trechos formatados como CODE no Telegram
                       (capturado pelo ingestao.py)

    Returns:
        Código de cupom em maiúsculas, ou string vazia se não confiável.
    """
    # ── NÍVEL 1: CRASES (Telegram CODE) — confiança 100% ──────────
    if code_entities:
        c = extrair_cupom_de_codes(code_entities)
        if c:
            return c

    # ── NÍVEL 2: Lista profissional "R$ X OFF em R$ Y: CODIGO" ────
    # Formato usado por divulgadores top — MUITO confiável
    if _RE_LISTA_CUPONS.search(texto):
        for linha in texto.splitlines():
            # Pega tudo após o último ":" que seguido de espaço
            m = re.search(
                r':\s*([A-Z0-9][A-Z0-9_-]{3,19})\b',
                linha,
            )
            if m:
                c = m.group(1).upper()
                if _eh_cupom_valido(c):
                    return c

    # ── NÍVEL 3: Padrão chave-valor "OFF: X" / "Cupom: X" ─────────
    # Regex já trata case-insensitive na palavra-chave
    for m in _RE_KV_CUPOM.finditer(texto):
        c = m.group(1).upper()
        if _eh_cupom_valido(c):
            return c

    # ── NÍVEL 4: Código próximo à palavra "cupom" (até 3 linhas) ──
    # Confiança média — usa quando texto MENCIONA cupom mas não tem
    # formato estrito chave-valor
    if _KW_CUPOM.search(texto):
        linhas = texto.splitlines()
        for i, linha in enumerate(linhas):
            if not _KW_CUPOM.search(linha):
                continue
            # Procura código na mesma linha + 3 seguintes
            for j in range(i, min(i + 4, len(linhas))):
                for m in _KW_COD.finditer(linhas[j]):
                    c = m.group(1).upper()
                    if _eh_cupom_valido(c):
                        return c

    # ── NÍVEL 5: NÃO TEM cupom claro — retorna vazio ──────────────
    # Política conservadora: melhor identificar oferta como "produto"
    # do que arriscar pegar palavra falsa como cupom.
    return ""


def extrair_todos_cupons(texto: str, code_entities: list = None) -> List[str]:
    """
    Extrai TODOS os códigos válidos do texto (não para no primeiro).
    Usado pra logs/análise — retorna em ordem de confiança.
    """
    encontrados: List[str] = []
    visto = set()

    def add(c: str):
        cu = c.upper()
        if cu not in visto and _eh_cupom_valido(cu):
            visto.add(cu)
            encontrados.append(cu)

    # NÍVEL 1: crases
    for c in _filtrar_codes_validos(code_entities or []):
        add(c)

    # NÍVEL 2+3: chave-valor
    for m in _RE_KV_CUPOM.finditer(texto):
        add(m.group(1))

    # NÍVEL 2 (lista): ":CODIGO"
    if _RE_LISTA_CUPONS.search(texto):
        for linha in texto.splitlines():
            m = re.search(r':\s*([A-Z0-9][A-Z0-9_-]{3,19})\b', linha)
            if m:
                add(m.group(1))

    # NÍVEL 4: linhas com palavra-chave
    if _KW_CUPOM.search(texto):
        for linha in texto.splitlines():
            if _KW_CUPOM.search(linha):
                for m in _KW_COD.finditer(linha):
                    add(m.group(1))

    return encontrados


def _extrair_asin_texto(texto: str, mapa: dict) -> str:
    for u in list(mapa.values()) + [texto]:
        for pat in _P_AMZ_ASIN:
            m = pat.search(u)
            if m: return m.group(1).upper()
    return ""


def _extrair_id_magalu(texto: str, mapa: dict) -> str:
    for u in list(mapa.values()) + [texto]:
        m = _P_MGL.search(u)
        if m: return m.group(1)
    return ""


def tem_contexto(texto: str) -> bool:
    linhas = [l.strip() for l in texto.splitlines()
              if l.strip() and not re.match(r'https?://', l.strip())]
    if not linhas: return False
    total = " ".join(linhas)
    indicadores = [
        r'off', r'%', r'r\$', r'cupom', r'desconto', r'promoção', r'oferta',
        r'grátis', r'evento', r'live', r'relâmpago', r'flash', r'volta',
        r'normalizou', r'a\s+partir', r'ativo', r'disponivel', r'pix',
        r'voltando', r'reativado', r'jogos?\s+gr[aá]tis',
    ]
    for ind in indicadores:
        if re.search(ind, total, re.I): return True
    return len(total) > 20


# ── 3d. Estado do evento ─────────────────────────────────────────
class EstadoEvento(Enum):
    NEW       = "new"
    SEEN      = "seen"
    EXPIRED   = "expired"
    RESTOCKED = "restocked"


_RE_RESTOCK_C3 = re.compile(
    r'voltou|restock|reativado|dispon[ií]vel\s+novamente|voltou\s+ao\s+estoque|'
    r'de\s+volta|ativo\s+novamente|normalizou|voltando|voltou\s+cupom|relançamento', re.I)


def detectar_estado_evento(texto: str, id_global: str, plat: str) -> EstadoEvento:
    eh_restock = bool(_RE_RESTOCK_C3.search(texto))
    entrada    = db_get_dedupe(_fp_c3(id_global, plat))
    if not entrada: return EstadoEvento.NEW
    ts_ant = entrada.get("ts", 0)
    delta  = __import__('time').time() - ts_ant
    janela = _JANELA_C3.get(plat, _JANELA_C3["default"])
    ttl    = _TTL_RESTOCK_C3.get(plat, _TTL_RESTOCK_C3["default"])
    if delta < janela:  return EstadoEvento.SEEN
    if eh_restock:      return EstadoEvento.RESTOCKED
    if delta > ttl:     return EstadoEvento.EXPIRED
    return EstadoEvento.SEEN


# ── 3e. Desencurtador ────────────────────────────────────────────
async def desencurtar(url: str, sessao: aiohttp.ClientSession, depth: int = 0) -> str:
    import random
    from config import USER_AGENTS
    from pipeline.classificacao import _FORCA_GET
    if depth > 15: return url
    url = _sanitizar_url(url)
    if not url.startswith(("http://", "https://")): return url
    nl = _netloc(url)
    if depth > 0 and nl == "cutt.ly": return url
    cached = _get_raw(url)
    if cached: return cached
    hdrs = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }
    try:
        usar_head = nl not in _FORCA_GET and not any(nl.endswith("." + d) for d in _FORCA_GET)
        if usar_head:
            try:
                async with sessao.head(url, headers=hdrs, allow_redirects=True,
                                       timeout=aiohttp.ClientTimeout(total=10),
                                       max_redirects=20) as r:
                    final = str(r.url)
                    if final != url:
                        _set_raw(url, final)
                        return await desencurtar(final, sessao, depth + 1)
            except Exception:
                pass
        async with sessao.get(url, headers=hdrs, allow_redirects=True,
                              timeout=aiohttp.ClientTimeout(total=20),
                              max_redirects=20) as r:
            pos = str(r.url)
            if pos != url:
                _set_raw(url, pos)
                return await desencurtar(pos, sessao, depth + 1)
            html = await r.text(errors="ignore")
            if len(html) > 500_000:
                _set_raw(url, pos); return pos
            soup = BeautifulSoup(html, "html.parser")
            ref = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
            if ref and ref.get("content"):
                m = re.search(r"url[=\s]*([^\s;\"']+)", ref["content"], re.I)
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    if novo.startswith("http"):
                        return await desencurtar(novo, sessao, depth + 1)
            for pat in [
                r'window\.location(?:\.href)?\s*=\s*["\']([^"\']{15,})["\']',
                r'location\.replace\s*\(\s*["\']([^"\']{15,})["\']\s*\)',
                r'location\.href\s*=\s*["\']([^"\']{15,})["\']',
            ]:
                mj = re.search(pat, html)
                if mj and mj.group(1).startswith("http"):
                    return await desencurtar(mj.group(1), sessao, depth + 1)
            og = soup.find("meta", attrs={"property": "og:url"})
            if og and og.get("content", "").startswith("http") and og["content"] != url:
                return await desencurtar(og["content"], sessao, depth + 1)
            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href", "").startswith("http") and canon["href"] != url:
                return await desencurtar(canon["href"], sessao, depth + 1)
            _set_raw(url, pos); return pos
    except asyncio.TimeoutError:
        log_nrm.warning(f"⏱ Timeout desencurtar d={depth}: {url[:60]}"); return url
    except Exception as e:
        log_nrm.error(f"❌ desencurtar d={depth}: {e}"); return url


# ── 3f. Dataclasses ──────────────────────────────────────────────
@dataclass
class MensagemNormalizada:
    msg_id:        int
    chat:          str
    texto_limpo:   str
    mapa:          Dict[str, str]
    preservar:     List[str]
    plat:          str
    cupom:         str
    sku:           str
    tem_midia:     bool
    media_obj:     object
    estado_evento: EstadoEvento = EstadoEvento.NEW
    ids_globais:   List[str]    = field(default_factory=list)
    is_reply:      bool         = False
    reply_to:      int          = 0
    is_override:   bool         = False


# ── 3g. normalizar() ─────────────────────────────────────────────
async def _normalizar_um(lc: LinkClassificado, sessao: aiohttp.ClientSession,
                         msg_id: int = 0) -> Tuple[str, Optional[str], str]:
    """
    BUG FIX (v80.1): preserva a URL ORIGINAL que estava no texto da mensagem.
    Antes, quando 'lc' era reatribuído após desencurtar (linha do 'lc =
    _classificar_cached(url)'), a url_original retornada virava a URL
    DESENCURTADA — e o mapa de substituição em montagem.py não conseguia
    achar a URL original (cutt.ly/maga.lu/etc) no texto.
    Resultado: o link do grupo monitorado vazava pro grupo destino.
    """
    from plataformas.affiliate_router import rotear_afiliacao
    plat = lc.plat
    url_original_real = lc.url_original          # ← SALVA ANTES DE QUALQUER MUDANÇA
    if plat == "mundial":   return url_original_real, url_original_real, "mundial"
    if plat == "preservar": return url_original_real, url_original_real, "preservar"
    if plat is None or lc.tipo in ("invalido", "bloqueado", "grupo_externo", "desconhecido"):
        return url_original_real, None, plat or "none"
    if plat == "amazon" and lc.tipo == "claims":
        return url_original_real, url_original_real, "amazon"
    cached = _get_final(url_original_real) or db_get_link(url_original_real)
    if cached: return url_original_real, cached, plat
    url = url_original_real
    if plat == "expandir":
        try: url = await desencurtar(url, sessao)
        except Exception: return url_original_real, None, "none"
        lc = _classificar_cached(url); plat = lc.plat
        if plat is None:      return url_original_real, None, "none"
        if plat == "mundial": return url_original_real, url, "mundial"
        if plat == "amazon" and lc.tipo == "claims":
            return url_original_real, url, "amazon"
        cached = _get_final(url) or db_get_link(url)
        if cached: return url_original_real, cached, plat
    convertido = await rotear_afiliacao(plat, url, sessao, msg_id)
    return url_original_real, convertido, plat


async def normalizar(bruta: MensagemBruta,
                     is_override: bool = False) -> Optional[MensagemNormalizada]:
    import time
    if not bruta.texto.strip(): return None
    if not is_override:
        bloqueado, override_flag = texto_bloqueado(bruta.texto)
        if bloqueado: return None
        is_override = override_flag

    # ── FILTROS DE POST INDESEJADO (v80.3) ────────────────────────
    # Bloqueia: exclusivo do canal, vídeo Shopee, post-serviço,
    # link concorrente sem plataforma, encaminhamento Telegram.
    # is_override NÃO ignora esses filtros (são qualitativos absolutos).
    indesejado, motivo = _eh_post_indesejado(bruta.texto)
    if indesejado:
        log_nrm.info(f"🚫 Post bloqueado [{motivo}] | @{bruta.chat}")
        return None

    texto_limpo   = limpar_texto(bruta.texto)
    if not tem_contexto(texto_limpo): return None
    classificados = classificar_links(bruta.links)
    converter     = [lc for lc in classificados if lc.plat not in ("preservar", None)]
    preservar_lst = [lc.url_original for lc in classificados if lc.plat == "preservar"]
    if not converter and not preservar_lst:
        if "fadadoscupons" not in bruta.chat: return None
    sessao    = await _get_session()
    resultados = await asyncio.gather(
        *[_normalizar_um(lc, sessao, bruta.msg_id) for lc in converter[:50]],
        return_exceptions=True)
    mapa: Dict[str, str] = []; plats: List[str] = []
    mapa = {}
    for res in resultados:
        if isinstance(res, Exception): log_nrm.error(f"❌ normalizar link: {res}"); continue
        orig, conv, plat = res
        if conv and plat not in ("none", None):
            mapa[orig] = conv
            if plat not in ("mundial", "preservar"): plats.append(plat)
    if converter and not mapa and not preservar_lst:
        log_nrm.warning(f"🚫 Zero links convertidos | @{bruta.chat}"); return None
    plat_dom    = max(set(plats), key=plats.count) if plats else "amazon"
    cupom       = extrair_cupom(texto_limpo, getattr(bruta, "code_entities", None))
    sku         = (
        next((f"{lc.plat[:3]}_{lc.sku}" for lc in classificados if lc.sku), "")
        or _extrair_asin_texto(texto_limpo, mapa)
        or _extrair_id_magalu(texto_limpo, mapa)
    )
    ids_globais: List[str] = []
    for orig in mapa:
        lc = _classificar_cached(orig)
        if lc.sku and lc.sku not in ids_globais: ids_globais.append(lc.sku)
    if sku and sku not in ids_globais: ids_globais.append(sku)
    estado = EstadoEvento.NEW
    if ids_globais:
        estado = detectar_estado_evento(texto_limpo, ids_globais[0], plat_dom)
    elif cupom:
        fp_cup  = _fp_c3(f"cup_{cupom}", plat_dom)
        entrada = db_get_dedupe(fp_cup)
        if entrada:
            delta  = time.time() - entrada.get("ts", 0)
            janela = _JANELA_C3.get(plat_dom, 120.0)
            estado = EstadoEvento.SEEN if delta < janela else EstadoEvento.EXPIRED
    log_nrm.info(
        f"✅ {len(mapa)}/{len(converter)} | plat={plat_dom} "
        f"cupom='{cupom}' sku={sku} estado={estado.value} override={is_override}"
    )
    _log_cache_stats()
    return MensagemNormalizada(
        msg_id=bruta.msg_id, chat=bruta.chat, texto_limpo=texto_limpo,
        mapa=mapa, preservar=preservar_lst, plat=plat_dom, cupom=cupom,
        sku=sku, tem_midia=bruta.tem_midia, media_obj=bruta.media_obj,
        estado_evento=estado, ids_globais=ids_globais,
        is_reply=bruta.is_reply, reply_to=bruta.reply_to,
        is_override=is_override,
)

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
    LinkClassificado,
    _AMZ_DOMINIOS,
    _ENCURTADORES,
    _FORCA_GET,
    _MGL_DOMINIOS,
    _SHP_DOMINIOS,
    _P_AMZ_ASIN,
    _P_MGL,
    _P_SHP,
    _AMZ_PATHS_SEM_TAG,
    _classificar_cached,
    classificar_links,
    _eh_magalu_url,
)
from pipeline.ingestao import MensagemBruta
from utils.hashes import _fp_c3
from utils.urls import _netloc, _sanitizar_url


# ─────────────────────────────────────────────────────────────────
# Janelas de deduplicação por plataforma (usadas em detectar_estado_evento)
# ─────────────────────────────────────────────────────────────────
_JANELA_C3 = {
    "shopee":  60.0,
    "amazon":  300.0,
    "magalu":  300.0,
    "default": 120.0,
}

_TTL_RESTOCK_C3 = {
    "shopee":  3600.0,
    "amazon":  7200.0,
    "magalu":  14400.0,
    "default": 3600.0,
}


# ─────────────────────────────────────────────────────────────────
# Filtro de texto (produtos/categorias indesejáveis)
# ─────────────────────────────────────────────────────────────────
_FILTRO_TEXTO = [
    "Monitor Samsung","Fonte Mancer","Placa de video","Monitor LG",
    "PC home Essential","Suporte articulado","VHAGAR","Superframe","AM5","AM4","GTX",
    "Placa de Vídeo","DDR5","DDR4","Dram","Monitor Safe","Monitor Redragon","CL18","CL16",
    "CL32","MT/s","MHz","RX 580","Ryzen","Placa Mãe","Gabinete Gamer",
    "Water Cooler","Monitor Dell","Monitor Gamer","Air Cooler",
]
_RE_MULTI_OFERTA  = re.compile(
    r'\b(?:ofertas?|promoções?)\s+(?:na\s+|no\s+|da\s+)?'
    r'(?:shopee|amazon|magalu|magazine\s*luiza)\b', re.I)
_RE_PRECO_LINHA   = re.compile(r'R\$\s?[\d.,]+')
_RE_URL_COUNT     = re.compile(r'https?://')

# Sinal SOCIAL forte — reação humana/comunitária que justifica
# override do filtro de blacklist. Preço/cupom NÃO entram aqui:
# o filtro foi feito justamente pra barrar produtos comerciais
# normais; só passa por reação real ("CORRE", "ESGOTA", "INSANO").
_RE_SINAL_FORTE = re.compile(
    r'\b(?:'
    r'esgota\s+r[aá]pido|'
    r'corre|'
    r'voa|'
    r'pega\s+logo|'
    r'quem\s+pegou\s+pegou|'
    r'acaba\s+r[aá]pido|'
    r'poucas?\s+unidades?|'
    r'bug|'
    r'erro\s+de\s+pre[cç]o|'
    r'j[aá]\s+era|'
    r'insano'
    r')\b',
    re.I,
)


def _tem_sinal_social_forte(texto: str) -> bool:
    """Detecta reação social/comunitária forte ('CORRE', 'ESGOTA', etc)."""
    if not texto:
        return False
    return bool(_RE_SINAL_FORTE.search(texto))


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


def texto_bloqueado(
    texto: str,
    contexto_extra: str = "",
) -> Tuple[bool, bool]:
    """
    Retorna (bloqueado, is_override).

    Override SÓ ativa por sinal social forte presente em `contexto_extra`
    (ex: 'CORRE', 'VAI ESGOTAR', 'INSANO'). Preço/cupom NÃO é override —
    blacklist tem prioridade pra produtos comerciais comuns.

    `contexto_extra` pode ser o próprio texto da mensagem (Caso A:
    sinal social na própria oferta) ou texto de comentários/replies
    relacionados (Caso B: liberação por reação posterior).
    """
    if _eh_multi_produto(texto):
        return False, False

    tl = texto.casefold()

    for p in _FILTRO_TEXTO:
        if p.casefold() in tl:
            if _tem_sinal_social_forte(contexto_extra):
                log_cls.debug(f"⚡ Override social '{p}'")
                return False, True

            log_cls.debug(f"🚫 Filtro: '{p}'")
            return True, False

    return False, False


# ─────────────────────────────────────────────────────────────────
# Filtros de post indesejado (qualitativos)
# ─────────────────────────────────────────────────────────────────
_RE_EXCLUSIVO_CANAL = re.compile(
    r'\b(?:exclusivo|exclusiva|s[oó]|somente|apenas)\s+'
    r'(?:do|da|de|para|pra|p/|pro|pros)\s+'
    r'(?:canal|grupo|membros?|seguidores?|@\w+)',
    re.I,
)
_RE_PEDE_LINK = re.compile(
    r'\b(?:envie?|mande|manda|envia|coloque|cole|cola|passe|passa)\s+'
    r'(?:o\s+|seu\s+|os\s+|aí\s+(?:o\s+)?)?link',
    re.I,
)
_RE_VIA_CHAT_SITE = re.compile(
    r'\b(?:no\s+chat|pelo\s+(?:nosso\s+)?site|aqui\s+no\s+grupo|'
    r'aqui\s+no\s+canal|aqui\s+embaixo|abaixo|aba\s+de)\b',
    re.I,
)
_RE_SHOPEE_VIDEO = re.compile(
    r'https?://(?:[a-z0-9-]+\.)?shopee\.com\.br/'
    r'(?:v|vt|live|video)/',
    re.I,
)
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
_RE_TELEGRAM_LINK = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org)/',
    re.I,
)


def _tem_link_concorrente(texto: str) -> bool:
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
    """Detecta posts que não devem ser republicados."""
    if _RE_EXCLUSIVO_CANAL.search(texto):
        return True, "exclusivo_canal"
    if _RE_SHOPEE_VIDEO.search(texto):
        return True, "shopee_video"
    if _RE_PEDE_LINK.search(texto) and _RE_VIA_CHAT_SITE.search(texto):
        return True, "pede_link_servico"
    if _tem_link_concorrente(texto) and not _tem_link_plataforma_real(texto):
        return True, "link_concorrente_sem_plataforma"
    if (_RE_TELEGRAM_LINK.search(texto)
            and not _tem_link_plataforma_real(texto)):
        return True, "encaminha_telegram"
    return False, ""


# ─────────────────────────────────────────────────────────────────
# Limpeza de ruído textual
# ─────────────────────────────────────────────────────────────────
_RE_INVISIVEIS  = re.compile(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]')
_RE_GRUPO_EXT   = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)[^\s]*', re.I)
_RE_LIXO_STRUCT = re.compile(
    r'^\s*(?:-?\s*An[uú]ncio|Publicidade|:::+|---+|===+|'
    r'[-–—]\s*(?:MG|AMZ)|(?:MG|AMZ)\s*:)\s*$', re.I)
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


# ─────────────────────────────────────────────────────────────────
# Extração de cupom — sistema em 5 níveis
# ─────────────────────────────────────────────────────────────────
_KW_CUPOM = re.compile(
    r'\b(?:cupom|cupons|c[oó]digo|c[oó]digos|coupon|coupons|voucher|vouchers)\b',
    re.I,
)
_KW_COD = re.compile(r'\b([A-Z0-9][A-Z0-9_-]{3,19})\b')
_RE_COD_PURO = re.compile(r'^[A-Z0-9][A-Z0-9_-]{3,19}$')
_RE_TEM_LETRA = re.compile(r'[A-Z]')
_RE_TEM_DIGITO = re.compile(r'[0-9]')

_FALSO_CUPOM = frozenset({
    # ── Palavras óbvias ──
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
    # ── Termos de marketing / condições ──
    "VALIDADE", "VENCIMENTO", "DURACAO", "DURAÇÃO", "QUANTIDADE",
    "LIMITE", "LIMITES", "LIMITADO", "LIMITADA", "ILIMITADO",
    "IMPERDIVEL", "IMPERDÍVEL", "INCRIVEL", "INCRÍVEL",
    "MAXIMO", "MÁXIMO", "MINIMO", "MÍNIMO", "ENTREGA",
    "RAPIDA", "RÁPIDA", "RAPIDO", "RÁPIDO",
    "PARCIAL", "INTEGRAL", "PARCELADO", "AVISTA", "AVÍSTA",
    "VARIOS", "VÁRIOS", "VARIAS", "VÁRIAS", "TODOS", "TODAS",
    # ── Marcas ──
    "AMAZON", "AMZN", "SHOPEE", "MAGALU", "MAGAZINE", "LUIZA",
    "ALIEXPRESS", "ALIBABA", "MERCADO", "LIVRE", "AMERICANAS",
    "CASAS", "BAHIA", "EXTRA", "CARREFOUR", "PAODE", "ACUCAR",
    "SUBMARINO", "DAFITI", "NETSHOES", "CENTAURO", "RIACHUELO",
    "RENNER", "SAMSUNG", "APPLE", "PHILIPS", "XIAOMI", "MOTOROLA",
    "NOKIA", "MICROSOFT", "INTEL", "AMD", "NVIDIA", "ASUS", "ACER",
    "DELL", "LENOVO", "LG", "SONY", "PANASONIC", "BOSCH", "BRASTEMP",
    "CONSUL", "ELETROLUX", "ELECTROLUX", "CADENCE", "MONDIAL", "ARNO",
    "OSTER", "BRAUN", "FAGOR", "ITATIAIA", "TRAMONTINA", "POLISHOP",
    "PHILCO", "LATINA", "DECKER", "MAKITA", "BRITANIA",
    "BRITÂNIA", "ALPINO", "PAMPERS", "NESTLE", "NESTLÉ", "POSITIVO",
    "INTELBRAS", "MALIBU", "LOGITECH", "RAZER", "REDRAGON", "HYPER",
    "CORSAIR", "KINGSTON", "WESTERN", "DIGITAL", "SEAGATE", "SANDISK",
    "TOSHIBA", "HUAWEI", "VIVO", "CLARO", "TIM",
    # ── Plataformas / serviços ──
    "NETFLIX", "DISNEY", "DISNEYPLUS", "GLOBOPLAY", "PRIME", "VIDEO",
    "PARAMOUNT", "HBOMAX", "STAR", "APPLETV", "YOUTUBE", "SPOTIFY",
    "DEEZER", "TWITCH", "STEAM", "EPIC", "GOG", "PLAYSTATION", "NINTENDO",
    "XBOX", "PSN", "PSPLUS",
    # ── Categorias ──
    "GAMING", "GAMER", "OFFICE", "HOMEOFFICE", "WORK", "PRO", "MAX",
    "PLUS", "LITE", "MINI", "AIR", "PROMAX", "STANDARD", "BASIC",
    "OUTLET", "DUTYFREE", "FREESHIP", "FREE", "PAID",
    # ── Unidades técnicas ──
    "USD", "BRL", "EUR", "USB", "HDMI", "BLE", "WIFI", "GPS", "NFC",
    "IPS", "OLED", "LCD", "LED", "ATX", "ITX", "ITV", "IPTV", "OTT",
    "P2P", "B2B", "B2C", "KPI", "KYC", "CPU", "GPU", "RAM", "ROM",
    "SSD", "HDD", "FPS", "HZ", "GHZ", "MHZ",
    "RGB", "HDR", "SDR", "PWM", "PCIE", "SATA", "DDR", "DDR4", "DDR5",
    "NVME", "RJ45", "VGA", "DVI", "TYPE", "TYPEC",
    "PS3", "PS4", "PS5", "WII", "DEX", "API", "SDK", "URL", "URI",
    "JSON", "XML", "HTTP", "HTTPS", "TCP", "UDP", "DNS", "SQL",
    "ETL", "OCR", "VPN", "SSO", "MFA", "OTP", "ASTRO",
    "SLIM", "GRAN", "TURISMO", "PACOTE",
    # ── Pagamento ──
    "PIX", "BOLETO", "CARTAO", "CARTÃO", "MASTER", "MASTERCARD",
    "VISA", "AMEX", "ELO", "HIPERCARD", "CREDITO", "CRÉDITO",
    "CREDIT", "DEBITO", "DÉBITO", "DEBIT", "MOEDA", "MOEDAS",
    "JURO", "JUROS", "PARCELA", "PARCELAS", "REAL", "REAIS",
    "DOLAR", "DÓLAR", "DOLLAR", "EURO", "VALOR", "VALORES",
    "PRECO", "PREÇO", "PRECOS", "PREÇOS", "BARATO", "BARATA",
    # ── Cashback / pontos ──
    "CASHBACK", "CASH", "BACK", "PONTOS", "PONTO", "MILHAS", "MILHA",
    # ── Roleta / evento ──
    "ROLETA", "GIRO", "GIROS", "ARENA", "QUIZ", "MISSAO", "MISSÃO",
    "DESAFIO", "SORTEIO", "PREMIO", "PRÊMIO", "PREMIOS", "PRÊMIOS",
    "EVENTO", "EVENTOS", "CAMPANHA", "CAMPANHAS",
    # ── Direção / status ──
    "OFF", "ON", "OK", "GO", "STOP", "PAUSE", "PLAY",
    "STARS", "FIRE", "HOT", "COLD", "WARM",
})

_RE_LISTA_CUPONS = re.compile(
    r'(?:r\$\s*\d+\s+off\s+em\s+r\$\s*\d+\s*:\s*[A-Z0-9]{4,}|'
    r'cupons?\s+(?:ainda\s+)?ativos?\s*:|ainda\s+ativos?\s*:)',
    re.I,
)
_RE_KV_CUPOM = re.compile(
    r'(?:OFF|cupom|cupons|c[oó]digo|c[oó]digos|coupon|voucher)\s*[:=]\s*'
    r'([A-Z0-9][A-Z0-9_-]{3,19})\b',
    re.I,
)
_RE_LINHA_CUPOM_LISTA = re.compile(
    r'(?:r\$\s*\d+|\d+\s*%)\s+off(?:\s+em\s+r\$\s*\d+)?\s*:\s*[A-Z0-9][A-Z0-9_-]{3,19}',
    re.I,
)


def _eh_cupom_valido(c: str) -> bool:
    """Validação STRICT de candidato a cupom."""
    if not c:
        return False
    if len(c) < 4 or len(c) > 20:
        return False
    c_upper = c.upper()
    if c_upper in _FALSO_CUPOM:
        return False
    if not _RE_COD_PURO.match(c_upper):
        return False
    tem_letra = bool(_RE_TEM_LETRA.search(c_upper))
    tem_digito = bool(_RE_TEM_DIGITO.search(c_upper))
    if not tem_letra:
        return False
    if not tem_digito and len(c_upper) < 5:
        return False
    return True


def _filtrar_codes_validos(code_entities: list) -> List[str]:
    if not code_entities:
        return []
    validos: List[str] = []
    for trecho in code_entities:
        candidato = trecho.strip()
        if _eh_cupom_valido(candidato):
            validos.append(candidato.upper())
            continue
        for palavra in re.findall(r'[A-Z0-9][A-Z0-9_-]{3,19}', candidato):
            if _eh_cupom_valido(palavra):
                validos.append(palavra.upper())
    return validos


def extrair_cupom_de_codes(code_entities: list) -> str:
    """NÍVEL 1 — primeiro código válido entre crases."""
    validos = _filtrar_codes_validos(code_entities)
    return validos[0] if validos else ""


def extrair_cupom(texto: str, code_entities: list = None) -> str:
    """Extrai o código de cupom do texto com 5 níveis de confiança."""
    # NÍVEL 1: CRASES (Telegram CODE) — confiança 100%
    if code_entities:
        c = extrair_cupom_de_codes(code_entities)
        if c:
            return c

    # NÍVEL 2: Lista profissional "R$ X OFF em R$ Y: CODIGO"
    if _RE_LISTA_CUPONS.search(texto):
        for linha in texto.splitlines():
            m = re.search(
                r':\s*([A-Z0-9][A-Z0-9_-]{3,19})\b',
                linha,
            )
            if m:
                c = m.group(1).upper()
                if _eh_cupom_valido(c):
                    return c

    # NÍVEL 3: Padrão chave-valor "OFF: X" / "Cupom: X"
    for m in _RE_KV_CUPOM.finditer(texto):
        c = m.group(1).upper()
        if _eh_cupom_valido(c):
            return c

    # NÍVEL 4: Código próximo à palavra "cupom" (até 3 linhas)
    if _KW_CUPOM.search(texto):
        linhas = texto.splitlines()
        for i, linha in enumerate(linhas):
            if not _KW_CUPOM.search(linha):
                continue
            for j in range(i, min(i + 4, len(linhas))):
                for m in _KW_COD.finditer(linhas[j]):
                    c = m.group(1).upper()
                    if _eh_cupom_valido(c):
                        return c

    # NÍVEL 5: NÃO TEM cupom claro — retorna vazio
    return ""


def extrair_todos_cupons(texto: str, code_entities: list = None) -> List[str]:
    """Extrai TODOS os códigos válidos do texto."""
    encontrados: List[str] = []
    visto = set()

    def add(c: str):
        cu = c.upper()
        if cu not in visto and _eh_cupom_valido(cu):
            visto.add(cu)
            encontrados.append(cu)

    for c in _filtrar_codes_validos(code_entities or []):
        add(c)

    for m in _RE_KV_CUPOM.finditer(texto):
        add(m.group(1))

    if _RE_LISTA_CUPONS.search(texto):
        for linha in texto.splitlines():
            m = re.search(r':\s*([A-Z0-9][A-Z0-9_-]{3,19})\b', linha)
            if m:
                add(m.group(1))

    linhas_lista = [
        l for l in texto.splitlines()
        if _RE_LINHA_CUPOM_LISTA.search(l)
    ]
    if len(linhas_lista) >= 2:
        for linha in linhas_lista:
            for m in re.finditer(r'\b([A-Z0-9][A-Z0-9_-]{3,19})\b', linha):
                add(m.group(1))

    if _KW_CUPOM.search(texto):
        for linha in texto.splitlines():
            if _KW_CUPOM.search(linha):
                for m in _KW_COD.finditer(linha):
                    add(m.group(1))

    return encontrados


# ─────────────────────────────────────────────────────────────────
# Extratores de ID por plataforma
# ─────────────────────────────────────────────────────────────────
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


def _extrair_id_shopee_texto(texto: str, mapa: dict) -> str:
    """
    Extrai SHOPID.ITEMID de URLs Shopee (URLs convertidas/expandidas).
    Resolve o caso típico: encurtador `s.shopee.com.br/XXXX` não tem
    SKU no path, mas a URL convertida tem `/i.SHOPID.ITEMID`.
    """
    for u in list(mapa.values()) + [texto]:
        for pat in _P_SHP:
            m = pat.search(u)
            if m:
                return f"{m.group(1)}.{m.group(2)}"
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


# ─────────────────────────────────────────────────────────────────
# Estado do evento
# ─────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────
# Desencurtador (legado, com cache compartilhado)
# ─────────────────────────────────────────────────────────────────
async def desencurtar(url: str, sessao: aiohttp.ClientSession, depth: int = 0) -> str:
    """
    Desencurtador legado — mantido pra compat. Código novo deveria
    usar pipeline.expansao.expandir_url() (motor com retry + loop detect).
    """
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


# ─────────────────────────────────────────────────────────────────
# Dataclasses
# ─────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────
# Expansão antecipada de encurtadores
#
# Hosts que precisam ser expandidos antes da afiliação. Sem isso, o
# SKU não é extraível da URL original e o sistema cai em fallback de
# identidade, gerando IDs diferentes pro mesmo produto quando o link
# de "resgate de cupons" varia.
# ─────────────────────────────────────────────────────────────────
_ENCURTADORES_SHOPEE = frozenset({
    "s.shopee.com.br", "shope.ee", "s.shopee.com",
})
_ENCURTADORES_MAGALU_GENERICOS = frozenset({
    "maga.lu", "divulgador.magalu.com",
    "cutt.ly", "bit.ly", "tinyurl.com", "rb.gy", "is.gd", "tiny.cc",
})


async def _normalizar_um(lc: LinkClassificado, sessao: aiohttp.ClientSession,
                         msg_id: int = 0) -> Tuple[str, Optional[str], str]:
    """
    Processa um link classificado:
      - expande encurtadores que escondem o ID do produto antes da afiliação
      - preserva a URL original (não é substituída pela expandida no retorno)
      - retorna (url_original, url_convertida, plat)
    """
    from plataformas.affiliate_router import rotear_afiliacao

    plat = lc.plat
    url_original_real = lc.url_original

    if plat == "mundial":   return url_original_real, url_original_real, "mundial"
    if plat == "preservar": return url_original_real, url_original_real, "preservar"
    if plat is None or lc.tipo in ("invalido", "bloqueado", "grupo_externo", "desconhecido"):
        return url_original_real, None, plat or "none"
    if plat == "amazon" and lc.tipo == "claims":
        return url_original_real, url_original_real, "amazon"

    cached = _get_final(url_original_real) or db_get_link(url_original_real)
    if cached: return url_original_real, cached, plat

    url = url_original_real
    nl = _netloc(url)

    # ── Expansão antecipada ──────────────────────────────────────
    eh_encurtador_shopee = nl in _ENCURTADORES_SHOPEE
    eh_encurtador_magalu = nl in _ENCURTADORES_MAGALU_GENERICOS
    precisa_expandir = (
        plat == "expandir"
        or (plat == "shopee" and eh_encurtador_shopee)
        or (plat == "magalu" and eh_encurtador_magalu)
    )
    if precisa_expandir:
        try:
            url_expandida = await desencurtar(url, sessao)
            if url_expandida and url_expandida != url:
                url = url_expandida
                lc = _classificar_cached(url)
                if lc.plat:
                    plat = lc.plat
                # Validação anti-loop: se virou nada útil, descarta
                if plat is None or plat in ("expandir", "none"):
                    log_nrm.warning(
                        f"🚫 expansão não revelou plataforma final: {url[:80]}"
                    )
                    return url_original_real, None, "none"
                if plat == "mundial":
                    return url_original_real, url, "mundial"
                if plat == "amazon" and lc.tipo == "claims":
                    return url_original_real, url, "amazon"
                cached = _get_final(url) or db_get_link(url)
                if cached:
                    return url_original_real, cached, plat
        except Exception as e:
            log_nrm.warning(f"  ⚠️ expansão antecipada falhou: {e}")
            if plat == "expandir":
                return url_original_real, None, "none"

    # Fluxo legado pra plat="expandir" que ainda não foi expandido
    # (caso _eh_encurtador_X não bateu mas plat era "expandir")
    if plat == "expandir":
        try:
            url = await desencurtar(url, sessao)
        except Exception:
            return url_original_real, None, "none"
        lc = _classificar_cached(url)
        plat = lc.plat
        if plat is None or plat in ("expandir", "none"):
            return url_original_real, None, "none"
        if plat == "mundial":
            return url_original_real, url, "mundial"
        if plat == "amazon" and lc.tipo == "claims":
            return url_original_real, url, "amazon"
        cached = _get_final(url) or db_get_link(url)
        if cached:
            return url_original_real, cached, plat

    convertido = await rotear_afiliacao(plat, url, sessao, msg_id)
    return url_original_real, convertido, plat


async def _registrar_pending_safe(bruta: MensagemBruta) -> None:
    """
    Registra mensagem bloqueada no pending. Comentário posterior do tipo
    'CORRE/precinho' via shadow_reply pode liberar o post dentro do TTL.

    Lazy import + try/except: se handlers.pending sumir ou falhar, a
    normalização continua funcionando — só o Caso B fica desligado.
    """
    try:
        from handlers.pending import _registrar_pending
        await _registrar_pending(bruta)
    except Exception as e:
        log_nrm.warning(f"⚠️ registrar pending: {e}")


async def normalizar(bruta: MensagemBruta,
                     is_override: bool = False) -> Optional[MensagemNormalizada]:
    """Normaliza a mensagem bruta — limpeza + expansão + afiliação + extração."""
    import time
    if not bruta.texto.strip(): return None
    if not is_override:
        # Override SOMENTE por sinal social forte. Caso A (sinal no próprio
        # texto da oferta) é coberto passando bruta.texto como contexto.
        # Caso B (sinal em comentário posterior) fica armado via pending:
        # quando bloquear, registra; shadow_reply libera se houver reação.
        bloqueado, override_flag = texto_bloqueado(
            bruta.texto,
            contexto_extra=bruta.texto,
        )
        if bloqueado:
            await _registrar_pending_safe(bruta)
            return None
        is_override = override_flag

    indesejado, motivo = _eh_post_indesejado(bruta.texto)
    if indesejado:
        log_nrm.info(f"🚫 Post bloqueado [{motivo}] | @{bruta.chat}")
        return None

    texto_limpo = limpar_texto(bruta.texto)
    if not tem_contexto(texto_limpo): return None

    classificados = classificar_links(bruta.links)
    converter     = [lc for lc in classificados if lc.plat not in ("preservar", None)]
    preservar_lst = [lc.url_original for lc in classificados if lc.plat == "preservar"]
    if not converter and not preservar_lst:
        return None

    sessao = await _get_session()
    resultados = await asyncio.gather(
        *[_normalizar_um(lc, sessao, bruta.msg_id) for lc in converter[:50]],
        return_exceptions=True,
    )

    mapa: Dict[str, str] = {}
    plats: List[str] = []
    for res in resultados:
        if isinstance(res, Exception):
            log_nrm.error(f"❌ normalizar link: {res}")
            continue
        orig, conv, plat = res
        if conv and plat not in ("none", None):
            mapa[orig] = conv
            if plat not in ("mundial", "preservar"):
                plats.append(plat)

    if converter and not mapa and not preservar_lst:
        log_nrm.warning(f"🚫 Zero links convertidos | @{bruta.chat}")
        return None

    plat_dom = max(set(plats), key=plats.count) if plats else "amazon"
    cupom    = extrair_cupom(texto_limpo, getattr(bruta, "code_entities", None))

    # Cadeia de extração de SKU — usa classificação, depois extratores
    # por texto/mapa, re-classifica URL convertida (cobre encurtador → produto)
    sku = (
        next((f"{lc.plat[:3]}_{lc.sku}" for lc in classificados if lc.sku), "")
        or _extrair_asin_texto(texto_limpo, mapa)
        or _extrair_id_magalu(texto_limpo, mapa)
        or _extrair_id_shopee_texto(texto_limpo, mapa)
    )

    ids_globais: List[str] = []
    for orig, conv in mapa.items():
        lc_orig = _classificar_cached(orig)
        if lc_orig.sku and lc_orig.sku not in ids_globais:
            ids_globais.append(lc_orig.sku)
        if conv:
            lc_conv = _classificar_cached(conv)
            if lc_conv.sku and lc_conv.sku not in ids_globais:
                ids_globais.append(lc_conv.sku)
    if sku and sku not in ids_globais:
        ids_globais.append(sku)

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
        f"cupom='{cupom}' sku={sku} ids={ids_globais} "
        f"estado={estado.value} override={is_override}"
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

"""
Utilitário — Detecção e validação de cupons.

Responsabilidade única: identificar e validar códigos de cupom em
texto livre. Dado um texto e, opcionalmente, trechos formatados como
código (capturados pela ingestão), determina quais sequências são
códigos de cupom legítimos.

Consumido por: normalização, deduplicação e montagem.

NÃO faz:
  - normalização de mensagem (responsabilidade da normalização)
  - classificação de plataforma (responsabilidade da classificação)
  - deduplicação (responsabilidade da deduplicação)
  - limpeza de texto (responsabilidade da normalização)
"""
from __future__ import annotations

import re
from typing import List, Optional


# ── Palavra-chave de domínio ──────────────────────────────────────
# Detecta a presença de termos que indicam contexto de cupom.
# Esta é a definição canônica de _KW_CUPOM no sistema.
_KW_CUPOM = re.compile(
    r'\b(?:cupom|cupons|c[oó]digo|c[oó]digos|coupon|coupons|'
    r'voucher|vouchers)\b',
    re.I,
)


# ── Padrões de extração ───────────────────────────────────────────
_KW_COD       = re.compile(r'\b([A-Z0-9][A-Z0-9_-]{3,19})\b')
_RE_COD_PURO  = re.compile(r'^[A-Z0-9][A-Z0-9_-]{3,19}$')
_RE_TEM_LETRA = re.compile(r'[A-Z]')
_RE_TEM_DIGITO = re.compile(r'[0-9]')

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
    r'(?:r\$\s*\d+|\d+\s*%)\s+off(?:\s+em\s+r\$\s*\d+)?\s*:\s*'
    r'[A-Z0-9][A-Z0-9_-]{3,19}',
    re.I,
)


# ── Blacklist de falsos cupons ────────────────────────────────────
# Palavras que satisfazem o formato de um código mas não são cupons:
# marcas, termos técnicos, vocabulário de marketing, siglas.
_FALSO_CUPOM = frozenset({
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
    "VALIDADE", "VENCIMENTO", "DURACAO", "DURAÇÃO", "QUANTIDADE",
    "LIMITE", "LIMITES", "LIMITADO", "LIMITADA", "ILIMITADO",
    "IMPERDIVEL", "IMPERDÍVEL", "INCRIVEL", "INCRÍVEL",
    "MAXIMO", "MÁXIMO", "MINIMO", "MÍNIMO", "ENTREGA",
    "RAPIDA", "RÁPIDA", "RAPIDO", "RÁPIDO",
    "PARCIAL", "INTEGRAL", "PARCELADO", "AVISTA", "AVÍSTA",
    "VARIOS", "VÁRIOS", "VARIAS", "VÁRIAS", "TODOS", "TODAS",
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
    "NETFLIX", "DISNEY", "DISNEYPLUS", "GLOBOPLAY", "PRIME", "VIDEO",
    "PARAMOUNT", "HBOMAX", "STAR", "APPLETV", "YOUTUBE", "SPOTIFY",
    "DEEZER", "TWITCH", "STEAM", "EPIC", "GOG", "PLAYSTATION", "NINTENDO",
    "XBOX", "PSN", "PSPLUS",
    "GAMING", "GAMER", "OFFICE", "HOMEOFFICE", "WORK", "PRO", "MAX",
    "PLUS", "LITE", "MINI", "AIR", "PROMAX", "STANDARD", "BASIC",
    "OUTLET", "DUTYFREE", "FREESHIP", "FREE", "PAID",
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
    "PIX", "BOLETO", "CARTAO", "CARTÃO", "MASTER", "MASTERCARD",
    "VISA", "AMEX", "ELO", "HIPERCARD", "CREDITO", "CRÉDITO",
    "CREDIT", "DEBITO", "DÉBITO", "DEBIT", "MOEDA", "MOEDAS",
    "JURO", "JUROS", "PARCELA", "PARCELAS", "REAL", "REAIS",
    "DOLAR", "DÓLAR", "DOLLAR", "EURO", "VALOR", "VALORES",
    "PRECO", "PREÇO", "PRECOS", "PREÇOS", "BARATO", "BARATA",
    "CASHBACK", "CASH", "BACK", "PONTOS", "PONTO", "MILHAS", "MILHA",
    "ROLETA", "GIRO", "GIROS", "ARENA", "QUIZ", "MISSAO", "MISSÃO",
    "DESAFIO", "SORTEIO", "PREMIO", "PRÊMIO", "PREMIOS", "PRÊMIOS",
    "EVENTO", "EVENTOS", "CAMPANHA", "CAMPANHAS",
    "OFF", "ON", "OK", "GO", "STOP", "PAUSE", "PLAY",
    "STARS", "FIRE", "HOT", "COLD", "WARM",
})


# ── Validação ─────────────────────────────────────────────────────
def _eh_cupom_valido(c: str) -> bool:
    """
    Determina se uma sequência é um código de cupom legítimo.

    Critérios: comprimento entre 4 e 20 caracteres, formato puro de
    código, ausência da blacklist, presença de ao menos uma letra,
    e presença de dígito quando o código é curto.
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
    tem_letra  = bool(_RE_TEM_LETRA.search(c_upper))
    tem_digito = bool(_RE_TEM_DIGITO.search(c_upper))
    if not tem_letra:
        return False
    if not tem_digito and len(c_upper) < 5:
        return False
    return True


def _filtrar_codes_validos(code_entities: list) -> List[str]:
    """
    Filtra os trechos formatados como código (capturados pela
    ingestão) retornando apenas os que são cupons válidos.
    """
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


# ── Extração ──────────────────────────────────────────────────────
def extrair_cupom_de_codes(code_entities: list) -> str:
    """Retorna o primeiro cupom válido entre os trechos de código."""
    validos = _filtrar_codes_validos(code_entities)
    return validos[0] if validos else ""


def extrair_cupom(texto: str, code_entities: list = None) -> str:
    """
    Extrai o cupom principal de um texto, aplicando uma hierarquia
    de estratégias em ordem de confiança decrescente:

      1. trechos formatados como código (alta confiança)
      2. linhas em formato de lista de cupons
      3. pares chave-valor ("cupom: CODIGO")
      4. proximidade de palavra-chave de cupom
    """
    if code_entities:
        c = extrair_cupom_de_codes(code_entities)
        if c:
            return c

    if _RE_LISTA_CUPONS.search(texto):
        for linha in texto.splitlines():
            m = re.search(r':\s*([A-Z0-9][A-Z0-9_-]{3,19})\b', linha)
            if m:
                c = m.group(1).upper()
                if _eh_cupom_valido(c):
                    return c

    for m in _RE_KV_CUPOM.finditer(texto):
        c = m.group(1).upper()
        if _eh_cupom_valido(c):
            return c

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
    return ""


def extrair_todos_cupons(texto: str, code_entities: list = None) -> List[str]:
    """
    Extrai todos os cupons distintos presentes em um texto.
    Aplica as mesmas estratégias de `extrair_cupom`, acumulando
    resultados únicos em ordem de descoberta.

    GARANTIA DE LITERALIDADE (contratual):
      Quando code_entities NÃO é fornecido, todo código devolvido é
      a forma MAIÚSCULA de um recorte literal do texto de entrada:
      as quatro estratégias textuais extraem exclusivamente por
      correspondência sobre o próprio texto. Consumidores que
      LOCALIZAM o código no texto para aplicar apresentação (ver
      montagem._crases) dependem desta propriedade. Qualquer
      evolução que normalize ou transforme o código devolvido DEVE
      preservá-la ou revisar esses consumidores explicitamente.
    """
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
        l for l in texto.splitlines() if _RE_LINHA_CUPOM_LISTA.search(l)
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
